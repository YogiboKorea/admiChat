// ================================================================
// 「추석 AI 사진관」 이벤트 모듈  (2026.09)
//
//  종류 4가지 (chuseokPrompts.js 참고)
//    card   한가위 축전        인사말 입력 → 축전 배경은 AI, 글자는 서버가 새긴다 (AI 는 한글을 틀린다)
//    studio 한가위 사진관      사진 2장 → 한 장의 한복 사진
//    pet    댕냥이 한복        반려동물 사진 + 성별
//    moon   달나라 떡방아 알바생 얼굴 사진 → 스노우 느낌 달토끼
//
//  ── 고정 규칙 (2026-09-15 확정 — 환경변수로 바꾸지 않는다) ──
//   · 회원만 참여한다. 아이디가 없으면 접수하지 않는다.
//   · 회원당 5회. 실패(과금 전)는 세지 않는다. 관리자 아이디(testid·yogibo)는 제한 없음.
//   · 적립금은 계정당 1회 — 「나의 쉼 순간」과 같은 방식(예약 → 호출 → 확정, unique index).
//
//  ── 반드시 지키는 것 (「나의 쉼 순간」과 동일) ──
//   ① 원본 사진은 메모리에서만 다루고 생성에 넘긴 직후 파기한다. 디스크·Mongo·로그·FTP 어디에도 남기지 않는다.
//   ② 공개 URL 에 회원 아이디를 넣지 않는다 (파일명 YYYYMMDD_<난수>).
//   ③ 14세 미만이 보이는 사진은 생성 모델에 보내지 않는다 — 설명(연령대·남녀 정도)만으로 임의 캐릭터를 그린다.
//   ④ 사진 속 인물이 실제처럼 보이는 결과라 작게 "AI 생성 이미지" 표기를 넣는다 (AI 기본법 투명성 — 기획안 공통 원칙).
//
//  라우트는 server.js 에서 mount(app, deps) 로 붙인다. 경로는 /api/chuseok/*
// ================================================================
'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const multer = require('multer');
const sharp = require('sharp');
const opentype = require('opentype.js');                 // 축전 글씨를 폰트에 기대지 않고 path 로 그린다
const ftp = require('basic-ftp');
const { Readable } = require('stream');

const CP = require('./chuseokPrompts');
const RM = require('./restMoment');                      // 금칙어 · 아이 일반화 · 분석 정리 · 아이디 정규화 재사용
const { sanitizeAnalysis, normId } = RM.__internals;

// ── 컬렉션 · 고정 규칙 ───────────────────────────────────────────
const ENTRY_COLLECTION = 'chuseokEntry';
const REWARD_COLLECTION = 'chuseokReward';
const TRASH_COLLECTION = 'chuseokTrash';                  // 관리자가 지운 응모의 URL 기록 — 파일 삭제 실패 시 재시도 근거
const REJECT_COLLECTION = 'chuseokRejects';               // 사진 확인에서 돌려보낸 기록(이유·인원 수만, 사진 없음) — 관리 페이지에서 본다

const MAX_PER_MEMBER = 5;                                 // 고정 (결정 사항)
// 갤러리 공개 보너스 — 공개한 사진 한 장당 1장씩 더, 최대 PUBLIC_BONUS 장까지.
// 5장을 다 공개하면 5장이 더 열려 최대 10장 (결정 사항 2026-09-16: +1 → 한 번에 +5 → 공개한 장수만큼 최대 5)
const PUBLIC_BONUS = 5;
const MEMBERS_ONLY = true;                                // 고정 (결정 사항)
const POINT_AMOUNT = Number(process.env.CHUSEOK_POINT || 3000);
const EVENT_START = process.env.CHUSEOK_START || '';      // 비우면 바로 열림 (YYYY-MM-DD, KST)
const EVENT_END = process.env.CHUSEOK_END || '2026-09-30';
const MAX_GEN_TOTAL = process.env.CHUSEOK_MAX_GEN === undefined ? 1000 : Math.max(0, Number(process.env.CHUSEOK_MAX_GEN) || 0);
const MAX_GEN_DAILY = Math.max(0, Number(process.env.CHUSEOK_DAILY_GEN || 0) || 0);
const MASTER_IDS = (process.env.CHUSEOK_MASTER_IDS || process.env.REST_MOMENT_MASTER_IDS || 'testid,yogibo')
  .split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
// 이미지 위에 글자로 찍던 'AI 생성 이미지' 표시 — 기본은 끈다 (결정 사항 2026-09-15: 이미지 자체엔 안 보이게).
// 대신 모든 결과 파일 안에 AI 생성 정보를 넣는다(EXIF + XMP IPTC DigitalSourceType). 다시 글자로 찍으려면 CHUSEOK_WATERMARK=1
const WATERMARK = /^(1|true|yes|on)$/i.test(String(process.env.CHUSEOK_WATERMARK || '0'));
const CONCURRENCY = Math.max(1, Number(process.env.CHUSEOK_CONCURRENCY || 2));

const MAX_PHOTO_BYTES = 12 * 1024 * 1024;
const GREETING_MAX = 24;                                  // 축전 글자 — 두 줄에 크게 들어가는 한계
const MAX_PEOPLE_TOTAL = 6;                               // 사진관 — 두 사진 합쳐 이 이상이면 얼굴이 뭉개진다

const OPENAI_MODEL = process.env.OPENAI_IMAGE_MODEL || 'gpt-image-2';
const OPENAI_QUALITY = process.env.OPENAI_IMAGE_QUALITY || 'high';      // 건당 ≈ $0.19
const OPENAI_VISION = process.env.OPENAI_VISION_MODEL || 'gpt-4.1-mini';

const FTP_DIR = process.env.FTP_CHUSEOK_DIR || '/web/img/md/09/chuseok';
const FTP_DIR_REL = FTP_DIR.split('/').filter(Boolean).join('/');
const FTP_PUBLIC = (process.env.FTP_CHUSEOK_PUBLIC_BASE || 'https://yogibo.openhost.cafe24.com/web/img/md/09/chuseok').replace(/\/$/, '');

// 메이트 참조 — 종류별 실제 제품 사진. 파일이 없으면 그 종류는 안 나온다(참조 없이 그리게 하지 않는다)
const MATE_REF_PATHS = {
  fox: path.join(__dirname, 'public', 'rest-moment', 'ref-mate-fox.png'),
  trex: path.join(__dirname, 'public', 'rest-moment', 'ref-mate-trex.png'),
};
// 축전 참고 카드 — public/chuseok/ref-card-*.jpg (요기보 한가위 카드 10종). 한 건마다 한 장을 골라 같이 보낸다.
// 사용 비율(%) — 기본 100. 0 이면 참고 카드 없이 예전처럼 그린다
const CARD_REF_DIR = path.join(__dirname, 'public', 'chuseok');
// 축전 글씨용 한글 폰트 (모두 OFL — 상업 사용 가능). 글자는 opentype 으로 path 를 떠서 그리므로
// 서버(컨테이너)에 한글 폰트가 깔려 있지 않아도 절대 깨지지 않는다
const FONT_DIR = path.join(__dirname, 'public', 'fonts');
const FONT_FILES = {
  jua: 'Jua-Regular.ttf',                       // 둥글고 도톰한 손글씨 — 카톡 축전 느낌
  brush: 'NanumBrushScript-Regular.ttf',        // 붓글씨 — 밤하늘·한지 카드
  black: 'BlackHanSans-Regular.ttf',            // 굵은 포스터 — 강한 색 카드
};
// 글꼴마다 글자 크기·테두리 두께를 따로 잡는다 — 붓글씨는 획이 얇아 크게, 테두리는 가늘게
const FONT_TUNE = {
  jua:   { scale: 1.00, edge: 0.24, inner: 0.12 },
  brush: { scale: 1.34, edge: 0.13, inner: 0.05 },
  black: { scale: 0.96, edge: 0.22, inner: 0.10 },
};
const CARD_REF_RATE = process.env.CHUSEOK_CARD_REF_RATE === undefined ? 100 : Math.max(0, Math.min(100, Number(process.env.CHUSEOK_CARD_REF_RATE) || 0));
// 메이트 등장 확률(%) — 기본 60. 0 이면 안 나오고 100 이면 늘 나온다 (결정 사항: 100% 노출은 아니게)
const MATE_RATE = process.env.CHUSEOK_MATE_RATE === undefined ? 60 : Math.max(0, Math.min(100, Number(process.env.CHUSEOK_MATE_RATE) || 0));

const OUT_W = 1080, OUT_H = 1350;                         // 4:5 — 카톡 전송·프사에 무난한 세로 카드

// ── 종류 정의 ────────────────────────────────────────────────────
const TYPES = {
  card:   { key: 'card',   label: '한가위 축전',        photos: 0 },
  studio: { key: 'studio', label: '한가위 사진관',      photos: 2 },
  pet:    { key: 'pet',    label: '댕냥이 한복',        photos: 1 },
  moon:   { key: 'moon',   label: '달나라 떡방아 알바생', photos: 1 },
};

// ── 오리진 ───────────────────────────────────────────────────────
const ALLOWED_ORIGINS = (process.env.REST_ALLOWED_ORIGINS || [
  'https://yogibo.kr', 'https://www.yogibo.kr', 'https://yogibo.co.kr', 'https://www.yogibo.co.kr',
  'https://yogibo.cafe24.com', 'https://*.yogibo.cafe24.com', 'http://yogibo.cafe24.com', 'http://*.yogibo.cafe24.com',
].join(',')).split(',').map(s => s.trim()).filter(Boolean);
function originAllowed(origin) {
  if (!origin) return true;
  return ALLOWED_ORIGINS.some(rule => {
    if (rule === origin) return true;
    if (!rule.includes('*')) return false;
    const re = new RegExp('^' + rule.split('*').map(s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('[^.]+') + '$');
    return re.test(origin);
  });
}

// ── 유틸 ─────────────────────────────────────────────────────────
const pad = n => String(n).padStart(2, '0');
function nowKST() { return new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' })); }
function ymd(d) { return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`; }
function publicName() {
  const d = nowKST();
  return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}_${crypto.randomBytes(5).toString('hex')}.jpg`;
}
function isMasterId(id) { return !!id && MASTER_IDS.includes(String(id)); }

// 갤러리에 보여줄 아이디 — 앞 두 글자만. 공개 동의 문구("아이디는 앞 두 글자만 보여요")와 같아야 한다.
// 「나의 쉼 순간」 maskId 는 끝 글자를 붙이지만 여기선 붙이지 않는다 (리뷰 2026-09-15)
function maskId(id) {
  let t = String(id || '');
  if (!t) return null;
  const at = t.indexOf('@');
  if (at > 0) t = t.slice(0, at);
  if (t.length <= 2) return t.charAt(0) + '***';
  return t.slice(0, 2) + '***';
}
// 관리자 아이디로 만든 건은 여러 사람이 참여한 것처럼 — 저장할 때 한 번 뽑아 고정
const FAKE_HEADS = ['su', 'mi', 'yo', 'ji', 'ha', 'se', 'ju', 'da', 'so', 'na', 'eu', 'hy', 'ky', 'bo', 'ye', 'ch', 'ka', 'ri', 'wo', 'ta', 'je', 'in', 'do', 'ga'];
function fakeDisplayId() {
  const head = FAKE_HEADS[crypto.randomInt(FAKE_HEADS.length)];
  return head + '***';                                     // 실제 회원 표기와 같은 모양 — 끝 숫자가 붙으면 관리자 건이 티 난다
}

// 관리 키 — 영문·숫자·기호(ASCII)만. 비교는 시간 차이가 안 나게
let ADMIN_KEY = String(process.env.CHUSEOK_ADMIN_KEY || process.env.REST_MOMENT_ADMIN_KEY || '').trim();
if (ADMIN_KEY && !/^[\x21-\x7E]+$/.test(ADMIN_KEY)) { console.error('★ [추석] 관리 키는 ASCII 만 됩니다 — 관리 API 를 닫습니다'); ADMIN_KEY = ''; }
function keyEq(given, expected) {
  if (!expected || !given) return false;
  const a = Buffer.from(String(given)), b = Buffer.from(String(expected));
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}
function withinEventPeriod(d) {
  const day = ymd(d);
  if (EVENT_START && day < EVENT_START) return false;
  return !EVENT_END || day <= EVENT_END;
}
const CTRL_RE = /[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g;
function cleanText(v) { return String(v == null ? '' : v).replace(CTRL_RE, '').replace(/\s+/g, ' ').trim(); }
function escXml(s) {
  return String(s).replace(CTRL_RE, '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&apos;');
}
const dataUrl = (buf, mime = 'image/jpeg') => `data:${mime};base64,${buf.toString('base64')}`;
/** 글자 수 — 한글·이모지 한 개를 한 글자로 센다 */
const charLen = s => Array.from(String(s || '')).length;

// ── 사진 보관소 (메모리 전용) ─────────────────────────────────────
// 접수 → 워커가 집기까지 잠깐 둔다. 꺼내는 즉시 비우고, 30분이 지나면 저절로 버린다.
const PHOTO_TTL_MS = 30 * 60 * 1000;
const photoVault = new Map();
function stashPhotos(id, photos) { photoVault.set(String(id), { photos, at: Date.now() }); }
function takePhotos(id) { const k = String(id); const v = photoVault.get(k); photoVault.delete(k); return v ? v.photos : null; }
setInterval(() => {
  const cut = Date.now() - PHOTO_TTL_MS;
  for (const [k, v] of photoVault) if (v.at < cut) { photoVault.delete(k); console.warn(`[추석] ${k} 사진 대기 만료 → 파기`); }
}, 60 * 1000).unref();

// ── 외부 호출 (테스트에서 바꿔 끼울 수 있게 한곳에 모은다) ─────────
const impl = {
  async visionJson(parts, maxTokens = 400) {
    const key = process.env.OPENAI_API_KEY;
    if (!key) throw new Error('OPENAI_API_KEY 없음');
    const res = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: OPENAI_VISION, messages: [{ role: 'user', content: parts }], response_format: { type: 'json_object' }, max_tokens: maxTokens, temperature: 0 }),
      signal: AbortSignal.timeout(30000),
    });
    if (!res.ok) throw new Error(`vision ${res.status}: ${(await res.text()).slice(0, 200)}`);
    const json = await res.json();
    const choice = json.choices && json.choices[0];
    // 답이 한도에서 잘리면 JSON 이 깨진다 — 이유를 분명히 남기고 다시 시도하게 한다 (여러 명 사진에서 400 토큰이 모자랐다)
    if (choice && choice.finish_reason === 'length') throw new Error(`vision 응답 잘림(max_tokens ${maxTokens})`);
    const text = (choice && choice.message && choice.message.content) || '{}';
    return JSON.parse(text);
  },

  /** gpt-image 호출. refs = [{buf, mime, name}] — 순서가 프롬프트의 "reference photo N" 과 맞아야 한다.
   *  refs 가 비면(참조 이미지 0장) 편집 API 는 받지 않으므로 생성 API(/images/generations)로 보낸다. 크기·품질은 같다 */
  async imageEdit(prompt, refs) {
    const key = process.env.OPENAI_API_KEY;
    if (!key) throw Object.assign(new Error('OPENAI_API_KEY 없음'), { retryable: false });
    const headers = { Authorization: `Bearer ${key}` };
    let url, body;
    if (refs && refs.length) {
      const fd = new FormData();
      fd.append('model', OPENAI_MODEL);
      fd.append('prompt', prompt);
      fd.append('size', '1024x1536');
      fd.append('quality', OPENAI_QUALITY);
      fd.append('output_format', 'png');
      fd.append('n', '1');
      refs.forEach(r => fd.append('image[]', new Blob([r.buf], { type: r.mime }), r.name));
      url = 'https://api.openai.com/v1/images/edits'; body = fd;
    } else {
      url = 'https://api.openai.com/v1/images/generations';
      headers['Content-Type'] = 'application/json';
      body = JSON.stringify({ model: OPENAI_MODEL, prompt, size: '1024x1536', quality: OPENAI_QUALITY, output_format: 'png', n: 1 });
    }
    // 실패를 둘로 나눈다 (리뷰 2026-09-15):
    //  · 요청이 OpenAI 에 닿기 전(주소·연결 단계) → 과금 없음이 확실 → 다시 시도
    //  · 보낸 뒤 기다리다 끊김(시간 초과·연결 끊김·응답 깨짐) → OpenAI 는 이미 그려 과금했을 수 있다
    //    → 다시 보내지 않는다(두 번 결제 방지). chargeUnknown 으로 알려 예산에는 세고 회원 횟수에서는 빼지 않는다.
    const PRE_SEND = ['ENOTFOUND', 'EAI_AGAIN', 'ECONNREFUSED', 'UND_ERR_CONNECT_TIMEOUT', 'ENETUNREACH', 'EHOSTUNREACH', 'ENETDOWN', 'EADDRNOTAVAIL'];
    // IPv6→IPv4 순서로 붙어 보는(autoSelectFamily) 경우 AggregateError 로 온다 — 안의 오류가 전부 연결 단계일 때만 "보내기 전"
    const isPreSend = c => !!(c && ((Array.isArray(c.errors) && c.errors.length)
      ? c.errors.every(x => x && PRE_SEND.indexOf(x.code) >= 0)
      : PRE_SEND.indexOf(c.code) >= 0));
    let res;
    try {
      // 생성은 평균 2분 안팎, 느릴 땐 4분 가까이 걸린다(avgGenMs 상한 240초) — 그보다 넉넉히
      res = await fetch(url, { method: 'POST', headers, body, signal: AbortSignal.timeout(300000) });
    } catch (e) {
      const cause = e && e.cause;
      if (isPreSend(cause)) {
        throw Object.assign(new Error('GPT 연결 실패(보내기 전): ' + (cause.code || 'connect')), { retryable: true });
      }
      throw Object.assign(new Error('GPT 응답 대기 중 끊김: ' + ((e && (e.name + ' ' + e.message)) || e)), { retryable: false, chargeUnknown: true });
    }
    if (!res.ok) {
      const t = await res.text().catch(() => '');
      throw Object.assign(new Error(`GPT ${res.status}: ${t.slice(0, 240)}`), { status: res.status, retryable: res.status === 429 || res.status >= 500 });
    }
    let json;
    try { json = await res.json(); }
    catch (e) { throw Object.assign(new Error('GPT 응답을 읽지 못함: ' + (e && e.message)), { retryable: false, chargeUnknown: true }); }
    const b64 = json && json.data && json.data[0] && json.data[0].b64_json;
    if (!b64) throw Object.assign(new Error('GPT 응답에 이미지가 없습니다'), { retryable: false, chargeUnknown: true });
    return Buffer.from(b64, 'base64');
  },

  async upload(buffer, filename) {
    let last;
    for (let i = 0; i < 3; i++) {
      const client = new ftp.Client(30000);
      client.ftp.verbose = false;
      try {
        await client.access({
          host: process.env.FTP_HOST || 'yogibo.ftp.cafe24.com', port: Number(process.env.FTP_PORT || 21),
          user: process.env.FTP_USER, password: process.env.FTP_PASS, secure: false,
        });
        await client.ensureDir(FTP_DIR_REL);
        await client.uploadFrom(Readable.from(buffer), filename);
        return `${FTP_PUBLIC}/${filename}`;
      } catch (e) { last = e; await new Promise(r => setTimeout(r, 1500 * (i + 1))); }
      finally { client.close(); }
    }
    throw last;
  },

  /** 공개 URL 하나를 FTP 에서 지운다 — 이 이벤트가 올린 파일(추석 폴더 · YYYYMMDD_난수10.jpg)만. 그 밖의 것은 절대 건드리지 않는다 */
  async remove(url) {
    if (!url) return { ok: true };
    const u = String(url);
    if (u.indexOf(FTP_PUBLIC + '/') !== 0) return { ok: false, error: '추석 폴더 파일이 아님: ' + u };
    const name = u.slice(FTP_PUBLIC.length + 1);
    if (!PUBLIC_NAME_RE.test(name)) return { ok: false, error: '지울 수 없는 파일명: ' + name };
    if (!process.env.FTP_USER) return { ok: false, error: 'FTP 계정 미설정' };
    const client = new ftp.Client(30000);
    client.ftp.verbose = false;
    try {
      await client.access({
        host: process.env.FTP_HOST || 'yogibo.ftp.cafe24.com', port: Number(process.env.FTP_PORT || 21),
        user: process.env.FTP_USER, password: process.env.FTP_PASS, secure: false,
      });
      await client.remove(`${FTP_DIR_REL}/${name}`);
      return { ok: true };
    } catch (e) {
      return { ok: false, error: e.message };
    } finally { client.close(); }
  },
};
const PUBLIC_NAME_RE = /^\d{8}_[0-9a-f]{10}\.jpg$/;     // publicName() 이 만드는 모양

// ── 사진 확인 (접수 시, 과금 전) ─────────────────────────────────
async function smallJpeg(buf, side = 768) {
  return sharp(buf).rotate().resize({ width: side, height: side, fit: 'inside' }).jpeg({ quality: 82 }).toBuffer();
}
/** 사람 사진 — 인원·연령대·14세 미만 여부. 두 번까지 시도하고 안 되면 null (호출부가 막는다 — 아이 사진이 모델로 새지 않게 닫힌 쪽으로) */
// 분석 정확도 (2026-09-15 "자동 분석이 잘 안 된다"): 저해상도(detail low, 512px)에선 작은 얼굴의 나이·인원을 자주 틀렸다.
// 1024px + detail high 로 보고, 여러 명(인물마다 설명이 붙는다)도 잘리지 않게 답 한도를 넉넉히 준다. 비용은 사진 한 장당 1원 안팎.
async function analyzePeople(buf) {
  const small = await smallJpeg(buf, 1024);
  for (let i = 0; i < 2; i++) {
    try { return sanitizeAnalysis(await impl.visionJson([{ type: 'text', text: CP.PEOPLE_ANALYSIS_PROMPT }, { type: 'image_url', image_url: { url: dataUrl(small), detail: 'high' } }], 1400)); }
    catch (e) { console.warn(`[추석] 사람 사진 확인 실패(${i + 1}/2):`, e.message); }
  }
  return null;
}
async function analyzePet(buf) {
  const small = await smallJpeg(buf, 1024);
  for (let i = 0; i < 2; i++) {
    try {
      const a = await impl.visionJson([{ type: 'text', text: CP.PET_ANALYSIS_PROMPT }, { type: 'image_url', image_url: { url: dataUrl(small), detail: 'high' } }], 300);
      const animal = ['dog', 'cat', 'other', 'none'].includes(a && a.animal) ? a.animal : 'none';
      // 사람·아이 여부 — 답이 없거나 이상하면 "사람 있음·아이 있음" 으로 본다 (닫힌 쪽으로. 리뷰 2026-09-15)
      const people = Number(a && a.people);
      const minorPresent = a && typeof a.minorPresent === 'boolean' ? a.minorPresent : true;
      return { animal, count: Number(a && a.count) || 0, people: Number.isFinite(people) ? people : 1, minorPresent };
    } catch (e) { console.warn(`[추석] 반려동물 사진 확인 실패(${i + 1}/2):`, e.message); }
  }
  return null;
}

// ── 축전 글자 ────────────────────────────────────────────────────
/** 인사말을 한두 줄로 나눈다 — 짧으면 한 줄, 길면 가운데에 가장 가까운 띄어쓰기·문장부호에서 끊는다 */
function splitGreeting(text) {
  const s = cleanText(text);
  const chars = Array.from(s);
  if (chars.length <= 10) return [s];
  const mid = chars.length / 2;
  let best = -1, bestDist = Infinity;
  chars.forEach((ch, i) => {
    if (i === 0 || i >= chars.length - 1) return;
    const breakAfter = /[\s~!,.?♡♥]/.test(ch);
    if (!breakAfter) return;
    const d = Math.abs(i + 1 - mid);
    if (d < bestDist) { bestDist = d; best = i + 1; }
  });
  if (best < 0 || bestDist > chars.length * 0.3) best = Math.ceil(mid);   // 적당한 끊을 곳이 없으면 가운데서
  return [chars.slice(0, best).join('').trim(), chars.slice(best).join('').trim()].filter(Boolean);
}

/** 축전 글자 SVG — 금박 그라데이션 + 흰 안쪽 테 + 진한 바깥 테 + 그림자 + 반짝이. 어떤 배경 위에서도 읽힌다 */
/** 축전 글씨 — 고른 참고 카드(styleKey)의 분위기에 맞춘 글꼴·색으로 새긴다.
 *  글자는 opentype 으로 path 를 떠서 그린다(시스템 한글 글꼴이 없어도 안 깨진다).
 *  폰트를 못 읽으면 예전처럼 <text> 로 그린다. */
function greetingSvg(text, styleKey, W = OUT_W, H = OUT_H) {
  const lines = splitGreeting(text);
  const longest = Math.max(...lines.map(charLen));
  const size = Math.max(70, Math.min(150, Math.floor((W * 0.84) / Math.max(longest, 1))));
  const lineH = Math.round(size * 1.18);
  const centerY = Math.round(H * 0.25);                   // 생성 이미지를 4:5 로 자른 뒤 비워 둔 띠(원본 14~44%)의 가운데
  const firstBase = Math.round(centerY - ((lines.length - 1) * lineH) / 2 + size * 0.36);
  const st = CARD_TEXT_STYLES[styleKey] || DEFAULT_TEXT_STYLE;
  const font = loadFont(st.font);
  const tune = FONT_TUNE[st.font] || FONT_TUNE.jua;
  const fsize = Math.round(size * (font ? tune.scale : 1));

  // 글자 그리기 — 폰트가 있으면 path, 없으면 <text>
  const draw = (extra) => lines.map((ln, i) => {
    const y = firstBase + i * lineH;
    if (font) {
      const w = font.getAdvanceWidth(ln, fsize);
      return `<path d="${font.getPath(ln, (W - w) / 2, y, fsize).toPathData(2)}" ${extra || ''}/>`;
    }
    return `<text x="${W / 2}" y="${y}" ${extra || ''}>${escXml(ln)}</text>`;
  }).join('');

  const sw1 = Math.max(2, Math.round(fsize * (font ? tune.edge : 0.24)));
  const sw2 = Math.max(1, Math.round(fsize * (font ? tune.inner : 0.12)));
  const top = firstBase - size, bottom = firstBase + (lines.length - 1) * lineH + size * 0.3;
  const spark = (x, y, r, c) => `<path d="M${x} ${y - r} L${x + r * 0.22} ${y - r * 0.22} L${x + r} ${y} L${x + r * 0.22} ${y + r * 0.22} L${x} ${y + r} L${x - r * 0.22} ${y + r * 0.22} L${x - r} ${y} L${x - r * 0.22} ${y - r * 0.22} Z" fill="${c}"/>`;
  const sysFont = "Pretendard, 'Pretendard Variable', 'Apple SD Gothic Neo', 'Malgun Gothic', sans-serif";
  const [c0, c1, c2, c3] = st.fill;
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">
  <defs>
    <linearGradient id="gold" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="${c0}"/><stop offset="0.38" stop-color="${c1}"/>
      <stop offset="0.55" stop-color="${c2}"/><stop offset="1" stop-color="${c3}"/>
    </linearGradient>
    <filter id="blur" x="-10%" y="-30%" width="120%" height="160%"><feGaussianBlur stdDeviation="${Math.round(size * 0.08)}"/></filter>
  </defs>
  <style>text{font-family:${sysFont};font-size:${size}px;font-weight:900;text-anchor:middle;letter-spacing:-0.02em;}</style>
  <g opacity="0.55" filter="url(#blur)" transform="translate(0 ${Math.round(size * 0.06)})">${draw(`fill="${st.glow}" stroke="${st.glow}" stroke-width="${sw1}"`)}</g>
  ${draw(`fill="none" stroke="${st.edge}" stroke-width="${sw1}" stroke-linejoin="round" stroke-linecap="round"`)}
  ${draw(`fill="none" stroke="${st.inner}" stroke-width="${sw2}" stroke-linejoin="round" stroke-linecap="round"`)}
  ${draw('fill="url(#gold)"')}
  ${spark(W * 0.1, top + 10, size * 0.28, '#FFF6C2')}${spark(W * 0.9, top + 30, size * 0.22, '#FFFFFF')}
  ${spark(W * 0.14, bottom - 10, size * 0.18, '#FFE27A')}${spark(W * 0.87, bottom + 6, size * 0.3, '#FFF6C2')}
</svg>`;
}

function watermarkSvg(W = OUT_W, H = OUT_H) {
  const font = "Pretendard, 'Pretendard Variable', 'Malgun Gothic', sans-serif";
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}">
  <text x="${W - 26}" y="${H - 24}" text-anchor="end" font-family="${font}" font-size="22" font-weight="600" fill="#000" fill-opacity="0.35" transform="translate(1 1)">AI 생성 이미지</text>
  <text x="${W - 26}" y="${H - 24}" text-anchor="end" font-family="${font}" font-size="22" font-weight="600" fill="#FFF" fill-opacity="0.85">AI 생성 이미지</text>
</svg>`;
}

// ── 보이지 않는 AI 생성 표시 ── 사람 눈엔 안 보이고, 파일 정보(EXIF·XMP)를 읽는 프로그램이 알아본다.
// XMP 의 IPTC DigitalSourceType trainedAlgorithmicMedia 는 "생성형 AI 로 만든 이미지" 를 뜻하는 국제 표준 값이다.
// (카카오톡 등이 다시 압축하면 파일 정보가 떨어질 수 있다 — 그래서 이벤트 페이지 화면에도 AI 생성이라고 적는다)
const AI_EXIF = { IFD0: { ImageDescription: 'AI-generated image (Yogibo Chuseok AI photo event)', Software: 'Generative AI' } };
const AI_XMP = '<?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?>'
  + '<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
  + '<rdf:Description rdf:about="" xmlns:Iptc4xmpExt="http://iptc.org/std/Iptc4xmpExt/2008-02-29/" xmlns:dc="http://purl.org/dc/elements/1.1/"'
  + ' Iptc4xmpExt:DigitalSourceType="http://cv.iptc.org/newscodes/digitalsourcetype/trainedAlgorithmicMedia">'
  + '<dc:description><rdf:Alt><rdf:li xml:lang="x-default">AI 생성 이미지 · 요기보 추석 AI 사진관</rdf:li></rdf:Alt></dc:description>'
  + '</rdf:Description></rdf:RDF></x:xmpmeta><?xpacket end="r"?>';

/** JPEG 의 앞쪽 APPn 묶음 바로 뒤에 XMP(APP1) 조각을 끼운다. JPEG 이 아니면 그대로 돌려준다 */
function addXmp(jpeg, xmp = AI_XMP) {
  if (!Buffer.isBuffer(jpeg) || jpeg.length < 4 || jpeg[0] !== 0xFF || jpeg[1] !== 0xD8) return jpeg;
  const payload = Buffer.concat([Buffer.from('http://ns.adobe.com/xap/1.0/\0', 'latin1'), Buffer.from(xmp, 'utf8')]);
  if (payload.length + 2 > 0xFFFF) return jpeg;
  let at = 2;
  while (at + 4 <= jpeg.length && jpeg[at] === 0xFF && jpeg[at + 1] >= 0xE0 && jpeg[at + 1] <= 0xEF) at += 2 + jpeg.readUInt16BE(at + 2);
  if (at > jpeg.length) return jpeg;
  const head = Buffer.alloc(4);
  head[0] = 0xFF; head[1] = 0xE1; head.writeUInt16BE(payload.length + 2, 2);
  return Buffer.concat([jpeg.subarray(0, at), head, payload, jpeg.subarray(at)]);
}

/** 생성 원본(2:3) → 4:5 카드 → (축전이면 글자) → (켜 둔 경우만 글자 워터마크) → JPEG + 보이지 않는 AI 생성 정보 */
async function renderFinal(genBuf, doc) {
  let img = sharp(genBuf).resize(OUT_W, OUT_H, { fit: 'cover', position: 'centre' });
  const layers = [];
  // 글씨 연출은 그 응모가 고른 참고 카드에 맞춘다 (카드마다 글꼴·색이 다르다)
  if (doc.type === 'card' && doc.greeting) {
    layers.push({ input: Buffer.from(greetingSvg(doc.greeting, cardStyleKeyOf(String(doc._id || '')))), top: 0, left: 0 });
  }
  if (WATERMARK) layers.push({ input: Buffer.from(watermarkSvg()), top: 0, left: 0 });
  if (layers.length) img = sharp(await img.png().toBuffer()).composite(layers);
  const jpg = await img.withMetadata({ exif: AI_EXIF }).jpeg({ quality: 90, mozjpeg: true }).toBuffer();
  return addXmp(jpg);
}

// ── 프롬프트 + 참조 조립 ─────────────────────────────────────────
const mateRefCache = {};
/* 참고 카드마다 글씨 느낌을 맞춘다 — 카드 그림의 글씨처럼 보이게 (결정 사항 2026-09-16).
   fill = 글자 채움(위→아래 그라데이션), edge = 바깥 테두리, inner = 안쪽 가는 선, glow = 뒤 번짐 */
const CARD_TEXT_STYLES = {
  '01': { font: 'jua',   fill: ['#FFFBE0', '#FFD84D', '#F0A21A', '#FFE7A3'], edge: '#7A0F1E', inner: '#FFFFFF', glow: '#000000' },  // 밤 한옥 — 금박
  '02': { font: 'jua',   fill: ['#FFFFFF', '#FFE9F2', '#FFC7DE', '#FFFFFF'], edge: '#B3245C', inner: '#FFFFFF', glow: '#7A1240' },  // 분홍 — 흰 글씨
  '03': { font: 'black', fill: ['#3A2408', '#20140A', '#120B05', '#2E1C0A'], edge: '#F6D77A', inner: '#FFF6D8', glow: '#8A6A20' },  // 크림·금화 — 진한 먹
  '04': { font: 'jua',   fill: ['#FFFBE0', '#FFD84D', '#E9971A', '#FFE7A3'], edge: '#3B1B6B', inner: '#FFFFFF', glow: '#170A33' },  // 보라 밤 — 금박
  '05': { font: 'jua',   fill: ['#FFFFFF', '#F3FAFF', '#D9EEFF', '#FFFFFF'], edge: '#124C86', inner: '#FFFFFF', glow: '#0B2F55' },  // 파란 하늘 — 흰 글씨
  '06': { font: 'jua',   fill: ['#FFFBA8', '#FFE94D', '#FFC01A', '#FFF3A8'], edge: '#C2118A', inner: '#FFFFFF', glow: '#000000' },  // 네온 — 노랑+자홍
  '07': { font: 'brush', fill: ['#FFF3C4', '#FFE08A', '#F5C451', '#FFEFB8'], edge: '#2A1A52', inner: '#FFF9DF', glow: '#120A2E' },  // 연등 밤하늘 — 따뜻한 손글씨
  '08': { font: 'brush', fill: ['#4A2E18', '#2E1B0C', '#1C1006', '#3A2210'], edge: '#F3E3C4', inner: '#FFFFFF', glow: '#8A6A3A' },  // 한지 족자 — 먹 글씨
  '09': { font: 'black', fill: ['#FFF3D8', '#FFD46B', '#E8892A', '#FFE6AE'], edge: '#8C2A12', inner: '#FFFFFF', glow: '#3A1206' },  // 단풍 — 주황 금박
  '10': { font: 'brush', fill: ['#FFFBE0', '#FFE7A3', '#F3C862', '#FFF3C8'], edge: '#12224A', inner: '#FFF9E4', glow: '#07122B' },  // 보름달 밤 — 달빛 손글씨
};
const DEFAULT_TEXT_STYLE = CARD_TEXT_STYLES['01'];

const fontCache = {};
/** 폰트 한 벌 — 없으면 null (그때는 예전처럼 시스템 글꼴로 <text> 를 쓴다) */
function loadFont(kind) {
  const file = FONT_FILES[kind] || FONT_FILES.jua;
  if (fontCache[file] !== undefined) return fontCache[file];
  try {
    const buf = fs.readFileSync(path.join(FONT_DIR, file));
    fontCache[file] = opentype.parse(buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength));
  } catch (e) {
    console.warn(`[추석] 축전 글꼴을 못 읽었습니다 (${file}) — 시스템 글꼴로 그립니다:`, e.message);
    fontCache[file] = null;
  }
  return fontCache[file];
}
/** 응모 id 로 고른 참고 카드 번호(01~10) — 글씨 연출을 그 카드에 맞추려고 쓴다 */
function cardStyleKeyOf(seed) {
  const ref = cardRefNameOf(seed);
  const m = ref && ref.match(/(\d{2})\.[a-z]+$/i);
  return m ? m[1] : null;
}

/** 응모 id + 꼬리표 → 늘 같은 숫자 (같은 건이면 늘 같은 카드가 골라지게) */
function seedInt(seed, tag) {
  return crypto.createHash('sha1').update(String(seed) + ':' + tag).digest().readUInt32BE(0);
}

/** 축전 참고 카드 한 장 — 응모 id 로 고르니 같은 건은 늘 같은 카드, 사람마다는 골고루 */
const cardRefCache = {};
let cardRefNames = null;
function cardRefNameOf(seed) {
  if (CARD_REF_RATE <= 0) return null;
  if (cardRefNames === null) {
    try { cardRefNames = fs.readdirSync(CARD_REF_DIR).filter(f => /^ref-card-.*\.(jpe?g|png)$/i.test(f)).sort(); }
    catch { cardRefNames = []; }
    if (!cardRefNames.length) console.warn('[추석] 축전 참고 카드가 없습니다 —', CARD_REF_DIR);
  }
  if (!cardRefNames.length) return null;
  // 쓸지 말지도 같은 씨앗으로 — 비율을 낮추면 일부만 참고 카드를 쓴다
  if (CARD_REF_RATE < 100 && (seedInt(seed, 'cardref') % 100) >= CARD_REF_RATE) return null;
  return cardRefNames[seedInt(seed, 'cardpick') % cardRefNames.length];
}
async function cardRef(seed) {
  const name = cardRefNameOf(seed);
  if (!name) return null;
  if (!cardRefCache[name]) {
    cardRefCache[name] = await sharp(fs.readFileSync(path.join(CARD_REF_DIR, name)))
      .resize({ width: 768, height: 768, fit: 'inside', withoutEnlargement: true }).jpeg({ quality: 88 }).toBuffer();
  }
  return { buf: cardRefCache[name], mime: 'image/jpeg', name: 'refcard.jpg', file: name };
}

async function mateRef(kind) {
  if (!MATE_REF_PATHS[kind]) return null;
  if (mateRefCache[kind]) return mateRefCache[kind];
  if (!fs.existsSync(MATE_REF_PATHS[kind])) return null;
  mateRefCache[kind] = await sharp(fs.readFileSync(MATE_REF_PATHS[kind])).resize({ width: 768, height: 768, fit: 'inside', withoutEnlargement: true }).png().toBuffer();
  return mateRefCache[kind];
}

/**
 * 응모 한 건의 프롬프트와 참조 이미지 목록.
 * photos: 보관소에서 꺼낸 사진들 — 접수 때 아이가 보여 버린 사진은 애초에 들어 있지 않다(설명만 문서에 있다).
 * 메이트: 응모 id 로 등장 여부·종류를 정한다(재시도해도 같다). 참조 파일이 없으면 "안 나옴" 으로 바꿔 프롬프트도 금지 문장으로.
 * 돌려주는 refs 가 비어 있으면(축전·아이만 있는 사진에 메이트도 없음) 편집이 아니라 생성 호출로 간다.
 */
async function buildJob(doc, photos) {
  const seed = String(doc._id);
  const refs = [];
  const toRef = async (p, i) => ({ buf: await sharp(p.buffer).rotate().resize({ width: 1024, height: 1024, fit: 'inside' }).jpeg({ quality: 88 }).toBuffer(), mime: 'image/jpeg', name: `photo${i + 1}.jpg` });

  let mate = CP.pickMate(seed, MATE_RATE);
  const mateBuf = mate ? await mateRef(mate) : null;
  if (mate && !mateBuf) { console.warn(`[추석] 메이트 참조(${mate}) 없음 → 이번 건은 메이트 없이`); mate = null; }

  let prompt;
  if (doc.type === 'card') {
    // 참고 카드를 쓰면 캐릭터가 그 카드에서 오므로 메이트 인형은 따로 넣지 않는다
    const ref = await cardRef(seed);
    if (ref) { refs.push({ buf: ref.buf, mime: ref.mime, name: ref.name }); mate = null; }
    prompt = CP.cardPrompt(seed, mate, !!ref);
  } else if (doc.type === 'studio') {
    // groups 순서 = 사진 1·2 순서. 사진이 있는 그룹만 참조로 보낸다
    const groups = (doc.groups || []).map((g, i) => {
      const ph = (photos || []).find(p => p.slot === i);
      // count: 접수 때 비전으로 센 인원 — 프롬프트가 "정확히 N명" 을 말해 사람이 빠지거나 늘지 않게
      return ph ? { source: 'photo', ph, count: g.count } : { source: 'brief', people: (g.analysis && g.analysis.people) || [] };
    });
    for (const g of groups) if (g.source === 'photo') refs.push(await toRef(g.ph, refs.length));
    prompt = CP.studioPrompt(seed, groups, mate);
  } else if (doc.type === 'pet') {
    const ph = (photos || [])[0];
    if (!ph) throw Object.assign(new Error('반려동물 사진이 메모리에 없습니다(재시작·대기 만료)'), { retryable: false, photoLost: true });
    refs.push(await toRef(ph, 0));
    prompt = CP.petPrompt(seed, doc.petGender, doc.species, mate);
  } else if (doc.type === 'moon') {
    const ph = (photos || [])[0];
    const person = ph ? { source: 'photo' } : { source: 'brief', people: (doc.analysis && doc.analysis.people) || [] };
    if (ph) refs.push(await toRef(ph, 0));
    prompt = CP.moonPrompt(seed, person, mate, doc.personGender);
  } else {
    throw Object.assign(new Error('알 수 없는 종류: ' + doc.type), { retryable: false });
  }
  if (mate) refs.push({ buf: mateBuf, mime: 'image/png', name: 'mate.png' });   // 메이트는 늘 마지막 참조
  return { prompt, refs, mate: mate || 'none' };
}

// ── 예산 · 횟수 · 예상 시간 ───────────────────────────────────────
async function genBudget(col) {
  const k = nowKST();
  const dayStart = new Date(k.getFullYear(), k.getMonth(), k.getDate());
  const [total, today, processing] = await Promise.all([
    // 예산은 "과금 여부 불명(genUnknownAt)" 도 센다 — 돈이 나갔을 수 있으므로. 회원 횟수(usedFilter)는 genAt 만 센다
    col.countDocuments({ $or: [{ genAt: { $exists: true } }, { genUnknownAt: { $exists: true } }] }),
    col.countDocuments({ $or: [{ genAt: { $gte: dayStart } }, { genUnknownAt: { $gte: dayStart } }] }),
    col.countDocuments({ status: 'processing' }),
  ]);
  const inflightOthers = Math.max(0, processing - 1);
  let reason = null;
  if (MAX_GEN_TOTAL > 0 && total + inflightOthers >= MAX_GEN_TOTAL) reason = 'total';
  else if (MAX_GEN_DAILY > 0 && today + inflightOthers >= MAX_GEN_DAILY) reason = 'daily';
  return { total, today, allowed: !reason, reason };
}

/** 쓴 횟수 — 과금 전 실패는 안 센다. GPT 를 이미 부른(genAt) 건은 실패했어도 센다 */
function usedFilter(mid) { return { memberId: mid, $or: [{ status: { $ne: 'failed' } }, { genAt: { $exists: true } }] }; }

/** 공개 보너스 기준 — 지금 공개 중이고(관리자가 숨기지 않았고) 횟수에 잡히는 응모가 하나라도 있으면 +PUBLIC_BONUS장.
 *  공개를 거두면 보너스도 빠진다(남은 횟수가 줄 뿐, 이미 만든 사진을 뺏지는 않는다). */
function publicFilter(mid) { return Object.assign(usedFilter(mid), { public: true, hidden: { $ne: true } }); }

async function quotaFor(db, mid) {
  // bonusMax — 공개로 열리는 장수(페이지 문구용). bonusLeft — 지금 공개하면 실제로 더 만들 수 있는 장수
  const base = { loggedIn: false, master: false, unlimited: false, base: MAX_PER_MEMBER, bonus: 0, bonusMax: PUBLIC_BONUS, bonusAvailable: false, publishMore: false, bonusLeft: 0, max: MAX_PER_MEMBER, used: 0, left: MAX_PER_MEMBER, done: 0, rewarded: false, membersOnly: MEMBERS_ONLY };
  if (!mid) return base;
  const col = db.collection(ENTRY_COLLECTION);
  const [used, done, reward, publicCount] = await Promise.all([
    col.countDocuments(usedFilter(mid)),
    col.countDocuments({ memberId: mid, status: 'done' }),
    db.collection(REWARD_COLLECTION).findOne({ memberId: mid }, { projection: { settled: 1 } }),
    col.countDocuments(publicFilter(mid)),
  ]);
  const master = isMasterId(mid);
  const rewarded = !!reward && (reward.settled === true || reward.settled === undefined);
  // 공개한 장수만큼 — 1장 공개 = 1장 더, 5장 공개 = 5장 더(거기서 멈춘다)
  const bonus = master ? 0 : Math.min(publicCount, PUBLIC_BONUS);
  const max = MAX_PER_MEMBER + bonus;
  // 이번 한 장을 "공개" 로 만들면 그 한 장이 실제로 들어갈 자리가 생기는가.
  // (공개를 거둬 이미 한도를 넘겨 쓴 계정에 이걸 권하면, 눌러도 서버가 거절하는 막다른 길이 된다)
  const bonusAvailable = !master && bonus < PUBLIC_BONUS && used < MAX_PER_MEMBER + Math.min(publicCount + 1, PUBLIC_BONUS);
  // 새로 만들 자리는 없지만, 이미 만든 사진을 보관함에서 공개하면 자리가 늘어나는 경우
  const publishMore = !master && !bonusAvailable && bonus < PUBLIC_BONUS && used < MAX_PER_MEMBER + PUBLIC_BONUS;
  return Object.assign(base, {
    loggedIn: true, master, unlimited: master, used, done, rewarded,
    bonus, bonusAvailable, publishMore,
    // 앞으로 더 열 수 있는 장수 (남은 보너스와 남은 자리 중 적은 쪽)
    bonusLeft: (bonusAvailable || publishMore) ? Math.min(PUBLIC_BONUS - bonus, MAX_PER_MEMBER + PUBLIC_BONUS - Math.max(used, max)) : 0,
    max, left: master ? null : Math.max(0, max - used),
  });
}

const recentGenMs = [];
const DEFAULT_GEN_MS = 110 * 1000;
function noteGenMs(ms) { if (ms > 5000 && Number.isFinite(ms)) { recentGenMs.push(ms); if (recentGenMs.length > 20) recentGenMs.shift(); } }
function avgGenMs() {
  if (!recentGenMs.length) return DEFAULT_GEN_MS;
  return Math.min(240000, Math.max(40000, recentGenMs.reduce((a, b) => a + b, 0) / recentGenMs.length));
}
async function estimateEta(col, doc) {
  const avg = avgGenMs();
  if (!doc || doc.status === 'done' || doc.status === 'failed') return { etaSec: 0, ahead: 0 };
  if (doc.status === 'processing') {
    const started = doc.pickedAt ? new Date(doc.pickedAt).getTime() : Date.now();
    return { etaSec: Math.round(Math.max(8000, avg - (Date.now() - started)) / 1000), ahead: 0 };
  }
  let ahead = 0;
  try {
    const [p, q] = await Promise.all([
      col.countDocuments({ status: 'pending', createdAt: { $lt: doc.createdAt } }),
      col.countDocuments({ status: 'processing' }),
    ]);
    ahead = p + q;
  } catch { /* 기본 추정 */ }
  const waves = Math.floor(ahead / CONCURRENCY);
  return { etaSec: Math.round((avg * (waves + 1)) / 1000) + 3, ahead };
}

// ── 워커 ─────────────────────────────────────────────────────────
let inflight = 0;

async function processOne(db, doc) {
  const col = db.collection(ENTRY_COLLECTION);
  const claim = await col.updateOne({ _id: doc._id, status: 'pending' }, { $set: { status: 'processing', pickedAt: new Date() } });
  if (!claim.matchedCount) return;
  const t0 = Date.now();
  let photos = doc.photoCount ? takePhotos(doc._id) : null;
  const wipe = () => { if (photos) photos.forEach(p => { p.buffer = null; }); photos = null; };
  let charged = false;
  try {
    const budget = await genBudget(col);
    if (!budget.allowed) {
      // 상한 도달 — 과금 없이 실패로 끝낸다. 횟수에 안 잡힌다(genAt 없음).
      wipe();
      await col.updateOne({ _id: doc._id }, { $set: { status: 'failed', lastError: 'budget:' + budget.reason, failedAt: new Date() }, $unset: { analysis: '', groups: '' } });
      console.warn(`[추석] 생성 상한(${budget.reason}) → ${doc._id} 과금 없이 종료`);
      return;
    }

    // 사진이 필요한데 메모리에 없다(재배포·대기 만료) — 닮게 그릴 수 없으니 과금 전에 멈춘다. 횟수에 안 잡혀 다시 올리면 된다.
    if (doc.photoCount && !photos) {
      await col.updateOne({ _id: doc._id }, { $set: { status: 'failed', lastError: 'photo_lost', failedAt: new Date() }, $unset: { analysis: '', groups: '' } });
      console.warn(`[추석] ${doc._id} 사진이 메모리에 없음 → 과금 없이 종료`);
      return;
    }

    const job = await buildJob(doc, photos || []);
    // ★ 보내기 직전 표시 — 이 뒤에 서버가 죽으면(재배포) 요청이 OpenAI 에 닿았는지 모른다. 복구 때 다시 보내지 않는 근거 (리뷰 2026-09-15)
    await col.updateOne({ _id: doc._id }, { $set: { genSentAt: nowKST() } });
    const g0 = Date.now();
    const gen = await impl.imageEdit(job.prompt, job.refs);
    charged = true;
    // ★ 원본 사진 파기 — 생성이 끝나면 더는 필요 없다 (일시 오류 재시도를 위해 그 전까지만 메모리에 둔다)
    wipe();
    job.refs.length = 0;
    const genMs = Date.now() - g0;
    noteGenMs(genMs);
    // ★ 과금 표시 — 이후 발행이 실패해도 예산·횟수에 잡힌다
    await col.updateOne({ _id: doc._id }, { $set: { genAt: nowKST(), genMs } });

    const out = await renderFinal(gen, doc);
    const imageUrl = await impl.upload(out, publicName());
    await col.updateOne({ _id: doc._id }, {
      $set: { status: 'done', imageUrl, doneAt: nowKST(), totalMs: Date.now() - t0, mate: job.mate },
      $unset: { lastError: '', analysis: '', groups: '' },
    });
    console.log(`[추석] 완성 ${doc._id} (${doc.type} · 메이트 ${job.mate})`);
  } catch (err) {
    const tries = (doc.tries || 0) + 1;
    const lastError = String(err && err.message || err).slice(0, 300);
    // 과금 전 일시 오류(429·5xx·연결)만 다시 한다 — 사진은 보관소에 되돌려 둔다(메모리, 30분 만료 그대로)
    if (!charged && err && err.retryable && tries < 3) {
      if (photos) { stashPhotos(doc._id, photos); photos = null; }
      // 재시도 대상은 보내기 전 연결 오류·429·5xx — 과금이 없으니 "보냄" 표시를 지운다
      await col.updateOne({ _id: doc._id }, { $set: { status: 'pending', tries, nextAt: new Date(Date.now() + 20000 * tries), lastError }, $unset: { genSentAt: '' } });
      console.error(`[추석] 생성 일시 오류 ${doc._id} (${tries}회, 재시도):`, lastError);
      return;
    }
    wipe();
    const failSet = { status: 'failed', tries, lastError, failedAt: new Date() };
    if (err && err.chargeUnknown && !charged) failSet.genUnknownAt = nowKST();   // 과금됐을 수 있음 — 다시 보내지 않고 예산에만 센다
    await col.updateOne({ _id: doc._id }, { $set: failSet, $unset: { analysis: '', groups: '' } });
    console.error(`[추석] 생성 실패 ${doc._id}:`, lastError);
  }
}

/** 재시작 때 processing 에 멈춘 건을 세 갈래로 — 같은 요청을 두 번 결제하지 않게 (리뷰 2026-09-15) */
async function recoverStuck(db) {
  const c = db.collection(ENTRY_COLLECTION);
  const drop = { analysis: '', groups: '', pickedAt: '' };
  // ① 이미 그려 과금됐는데 합성·업로드 중 죽음 → 끝낸다 (genAt 이 있어 회원 횟수·예산에 잡힌다)
  const a = await c.updateMany({ status: 'processing', genAt: { $exists: true } }, { $set: { status: 'failed', lastError: 'restart_after_gen', failedAt: new Date() }, $unset: drop });
  // ② 보냈는데 답을 못 받음 → 다시 보내지 않는다. 예산에만 센다(회원 횟수는 안 깎는다)
  const b = await c.updateMany({ status: 'processing', genSentAt: { $exists: true }, genAt: { $exists: false } }, { $set: { status: 'failed', lastError: 'restart_in_flight', failedAt: new Date(), genUnknownAt: nowKST() }, $unset: drop });
  // ③ 집어 가기만 하고 보내지 않음 → 다시 줄 세운다
  const r = await c.updateMany({ status: 'processing', genSentAt: { $exists: false }, genAt: { $exists: false } }, { $set: { status: 'pending' }, $unset: { pickedAt: '' } });
  const n = k => (k && k.modifiedCount) || 0;
  if (n(a) + n(b) + n(r)) console.log(`[추석] 재시작 복구 — 생성 후 중단 ${n(a)} · 전송 중 중단 ${n(b)} · 다시 줄 세움 ${n(r)}`);
  return { afterGen: n(a), inFlight: n(b), requeued: n(r) };
}

async function tick(getDb) {
  if (inflight >= CONCURRENCY) return;
  try {
    const db = getDb();
    if (!db) return;
    const now = new Date();
    const docs = await db.collection(ENTRY_COLLECTION)
      .find({ status: 'pending', $or: [{ nextAt: { $exists: false } }, { nextAt: null }, { nextAt: { $lte: now } }] })
      .sort({ createdAt: 1 }).limit(CONCURRENCY - inflight).toArray();
    for (const d of docs) {
      inflight++;
      processOne(db, d).catch(e => console.error('[추석] 워커 오류:', e.message)).finally(() => { inflight--; });
    }
  } catch (e) { console.error('[추석] 워커 오류:', e.message); }
}

// ── 접수 검증 (라우트와 테스트가 같이 쓴다) ───────────────────────
/** 입력만 본다(사진 내용 확인 전). 문제가 있으면 { status, body } 를, 없으면 정리된 값을 돌려준다 */
function validateInput(body, files) {
  const type = String((body && body.type) || '');
  const def = TYPES[type];
  if (!def) return { error: { status: 400, body: { ok: false, message: '만들 사진 종류를 골라주세요.' } } };
  const fileList = files || [];
  if (fileList.length < def.photos) {
    return { error: { status: 400, body: { ok: false, message: def.photos === 2 ? '사진 2장을 모두 올려주세요.' : '사진을 올려주세요.' } } };
  }
  if (def.photos && String(body.agreePhoto) !== '1') {
    return { error: { status: 400, body: { ok: false, message: '사진 수집·이용에 동의해주세요.' } } };
  }
  // share: 갤러리 공개 — 고객이 직접 "공개할게요" 를 고른 경우만(기본 비공개)
  const out = { type, def, agreeMarketing: String(body.agreeMarketing) === '1', share: String(body.share) === '1' };
  if (type === 'card') {
    const greeting = cleanText(body.greeting);
    if (!greeting) return { error: { status: 400, body: { ok: false, message: '축전에 넣을 인사말을 적어주세요.' } } };
    if (charLen(greeting) > GREETING_MAX) return { error: { status: 400, body: { ok: false, message: `인사말은 ${GREETING_MAX}자까지 넣을 수 있어요.` } } };
    if (RM.looksInappropriate(greeting)) return { error: { status: 400, body: { ok: false, message: '축전에 넣기 어려운 표현이 있어요. 다른 인사말로 적어주세요.' } } };
    out.greeting = greeting;
  }
  if (type === 'pet') {
    const g = String(body.petGender || '');
    if (g !== 'boy' && g !== 'girl') return { error: { status: 400, body: { ok: false, message: '반려동물 성별을 골라주세요.' } } };
    out.petGender = g;
  }
  if (type === 'moon') {
    // 성별은 고객이 고른다 — 사진 분석으로 추측하면 남자가 여자로 그려지는 일이 있었다
    const g = String(body.personGender || '');
    if (g !== 'male' && g !== 'female') return { error: { status: 400, body: { ok: false, message: '남자·여자 중 하나를 골라주세요.' } } };
    out.personGender = g;
  }
  return out;
}

// ── 라우트 ───────────────────────────────────────────────────────
function mount(app, deps) {
  const { getDb, apiRequest, mallId } = deps;
  const { ObjectId } = require('mongodb');

  const allowRead = (req, res, next) => {
    const origin = req.headers.origin;
    if (origin) { res.setHeader('Access-Control-Allow-Origin', origin); res.setHeader('Vary', 'Origin'); }
    next();
  };
  const allowWrite = (req, res, next) => {
    const origin = req.headers.origin;
    if (!originAllowed(origin)) return res.status(403).json({ ok: false, message: '허용되지 않은 요청입니다.' });
    if (origin) { res.setHeader('Access-Control-Allow-Origin', origin); res.setHeader('Vary', 'Origin'); }
    next();
  };

  // ★ 원본 사진은 메모리에서만. 두 칸(photo1·photo2)까지.
  const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: MAX_PHOTO_BYTES, files: 2 },
    fileFilter: (_req, file, cb) => (/^image\//.test(file.mimetype) ? cb(null, true) : cb(new Error('이미지 파일만 올릴 수 있습니다.'))),
  }).fields([{ name: 'photo1', maxCount: 1 }, { name: 'photo2', maxCount: 1 }]);

  // ── 접수 ──
  app.post('/api/chuseok/entry', allowWrite, (req, res) => {
    upload(req, res, async (uploadErr) => {
      const files = [];
      const dropFiles = () => files.forEach(f => { f.buffer = null; });
      if (uploadErr) return res.status(400).json({ ok: false, message: uploadErr.message || '사진을 읽지 못했습니다.' });
      try {
        const got = req.files || {};
        ['photo1', 'photo2'].forEach((n, slot) => { if (got[n] && got[n][0]) files.push(Object.assign(got[n][0], { slot })); });

        if (!withinEventPeriod(nowKST())) { dropFiles(); return res.status(400).json({ ok: false, message: '이벤트 기간이 아닙니다.' }); }

        const mid = normId(req.body && req.body.memberId);
        if (MEMBERS_ONLY && !mid) { dropFiles(); return res.status(400).json({ ok: false, loginRequired: true, message: '회원만 참여할 수 있는 이벤트예요. 로그인 후 다시 시도해주세요.' }); }

        const v = validateInput(req.body || {}, files);
        if (v.error) { dropFiles(); return res.status(v.error.status).json(v.error.body); }
        const def = v.def;
        // 필요한 장수보다 많이 오면 앞에서부터만 쓴다 (축전은 사진을 받지 않는다)
        const used = files.slice(0, def.photos);
        files.slice(def.photos).forEach(f => { f.buffer = null; });

        const db = getDb();
        const col = db.collection(ENTRY_COLLECTION);
        const master = isMasterId(mid);
        // 한도 = 5 + 공개한 장수(최대 PUBLIC_BONUS). 이번 건을 공개로 만들면 그 한 장도 쳐 준다
        const limitFor = (pub, share) => MAX_PER_MEMBER + Math.min(pub + (share ? 1 : 0), PUBLIC_BONUS);
        const limitReply = async (pub, n) => {
          // 이번 건을 공개로 내면 그 한 장이 실제로 들어갈 자리가 생기는가 (limitFor 와 같은 셈이어야 한다 — 안 그러면 안내대로 눌러도 거절된다)
          const canBonus = !v.share && n < limitFor(pub, true);
          // 새로 만들 자리는 없지만, 보관함에서 이미 만든 사진을 공개하면 자리가 늘어나는 경우
          const canPublishMore = !canBonus && Math.min(pub, PUBLIC_BONUS) < PUBLIC_BONUS && n < MAX_PER_MEMBER + PUBLIC_BONUS;
          return res.status(400).json({
            ok: false, limitReached: true, bonusAvailable: canBonus, publishMore: canPublishMore,
            quota: await quotaFor(db, mid).catch(() => null),
            message: canBonus
              ? `${n}장을 모두 만들었어요. 갤러리에 공개로 만들면 1장 더 만들 수 있어요. (공개한 장수만큼 최대 ${PUBLIC_BONUS}장까지)`
              : canPublishMore
                ? `${n}장을 모두 만들었어요. ‘내가 만든 사진’에서 이미 만든 사진을 공개하면 공개한 장수만큼 더 만들 수 있어요. (최대 ${MAX_PER_MEMBER + PUBLIC_BONUS}장)`
                : `이 아이디로는 ${MAX_PER_MEMBER + PUBLIC_BONUS}장까지 만들 수 있어요.`,
          });
        };
        if (!master) {
          const [n, pub] = await Promise.all([col.countDocuments(usedFilter(mid)), col.countDocuments(publicFilter(mid))]);
          if (n >= limitFor(pub, v.share)) { dropFiles(); return limitReply(pub, n); }
        }

        // ── 사진 확인 (과금 전) — 문제가 있으면 여기서 돌려보내고 횟수도 안 쓴다 ──
        const doc = {
          type: def.key, typeLabel: def.label, memberId: mid, master, agreeMarketing: v.agreeMarketing,
          public: v.share, publicAt: v.share ? nowKST() : null,
          // 갤러리 표시용 가린 아이디 — 저장 때 한 번 정한다(마스터 아이디는 여러 사람처럼 보이게 임의로)
          displayId: master ? fakeDisplayId() : (RM.looksInappropriate(mid) ? '회원***' : maskId(mid)),
          status: 'pending', tries: 0, createdAt: nowKST(), photoCount: 0,
          // 만든 기기만 아는 열쇠 — 나중에 "공개로" 바꿀 때 필요하다. 아이디만 알면 남의 사진을 공개해 버릴 수 있어서 (리뷰 2026-09-15)
          ownerKey: crypto.randomBytes(16).toString('hex'),
        };
        const keep = [];                                          // 모델에 보낼 사진만
        // 사진 확인에서 돌려보낸 기록 — 관리 페이지에서 "왜 안 받아졌는지" 본다. 사진은 남기지 않고 이유·인원 수만
        const reject = async (reason, extra) => {
          console.log(`[추석] 사진 확인 거절 · ${def.key} · ${reason}`, JSON.stringify(extra || {}));
          try { await db.collection(REJECT_COLLECTION).insertOne(Object.assign({ memberId: mid, type: def.key, reason, at: new Date() }, extra || {})); }
          catch (e) { console.warn('[추석] 거절 기록 실패:', e.message); }
        };
        if (def.key === 'card') {
          doc.greeting = v.greeting;
        } else if (def.key === 'pet') {
          const a = await analyzePet(used[0].buffer);
          if (!a) { await reject('analysis_failed'); dropFiles(); return res.status(409).json({ ok: false, message: '사진을 확인하지 못했어요. 잠시 후 다시 시도해주세요.' }); }
          if (a.animal === 'none') { await reject('no_pet', { people: a.people }); dropFiles(); return res.status(400).json({ ok: false, message: '반려동물이 잘 보이는 사진으로 올려주세요.' }); }
          // 반려동물 사진은 사진을 빼고 설명으로 그릴 수 없다 — 아이가 함께 보이면 받지 않는다 (③ 아이 사진은 모델로 보내지 않는다)
          if (a.people > 0 && a.minorPresent) { await reject('minor_with_pet', { people: a.people, animal: a.animal }); dropFiles(); return res.status(400).json({ ok: false, message: '아이가 함께 나온 사진은 쓸 수 없어요. 반려동물만 나온 사진으로 올려주세요.' }); }
          doc.species = a.animal; doc.petGender = v.petGender;
          keep.push(used[0]);
        } else if (def.key === 'moon') {
          const a = await analyzePeople(used[0].buffer);
          if (!a) { await reject('analysis_failed'); dropFiles(); return res.status(409).json({ ok: false, message: '사진을 확인하지 못했어요. 잠시 후 다시 시도해주세요.' }); }
          if (!(a.count > 0)) { await reject('no_face', { count: 0 }); dropFiles(); return res.status(400).json({ ok: false, message: '얼굴이 잘 보이는 사진으로 올려주세요.' }); }
          doc.personGender = v.personGender;
          // 뒤에 다른 어른이 찍혀 있어도 받는다 — 프롬프트가 "가장 크게 나온 한 사람" 만 그리게 한다 (예전엔 2명 이상이면 돌려보냈다)
          if (a.count > 1) doc.extraPeople = a.count - 1;
          if (a.minorPresent) {
            // ③ 아이가 보이면 사진은 보내지 않는다 — 올린 사람이 아이면 캐릭터로, 뒤에 아이가 섞였으면 한 사람 사진으로 다시 받는다
            if (a.count > 1) { await reject('minor_in_group', { count: a.count }); dropFiles(); return res.status(400).json({ ok: false, message: '아이가 함께 나온 사진은 쓸 수 없어요. 내 얼굴만 나온 사진으로 올려주세요.' }); }
            doc.minorFlag = true; doc.analysis = RM.genericizeMinors(a); used[0].buffer = null;
          } else keep.push(used[0]);
        } else if (def.key === 'studio') {
          const [a1, a2] = await Promise.all(used.map(f => analyzePeople(f.buffer)));
          if (!a1 || !a2) { await reject('analysis_failed', { failed: [!a1, !a2] }); dropFiles(); return res.status(409).json({ ok: false, message: '사진을 확인하지 못했어요. 잠시 후 다시 시도해주세요.' }); }
          const empty = [a1, a2].findIndex(a => !(a.count > 0));
          if (empty >= 0) { await reject('no_people', { photo: empty + 1, counts: [a1.count, a2.count] }); dropFiles(); return res.status(400).json({ ok: false, message: `사진 ${empty + 1}에 사람이 잘 보이지 않아요. 다른 사진으로 올려주세요.` }); }
          if (a1.count + a2.count > MAX_PEOPLE_TOTAL) { await reject('too_many_people', { counts: [a1.count, a2.count] }); dropFiles(); return res.status(400).json({ ok: false, message: `두 사진을 합쳐 ${MAX_PEOPLE_TOTAL}명까지 함께 담을 수 있어요.` }); }
          doc.groups = [a1, a2].map((a, i) => {
            if (a.minorPresent) { doc.minorFlag = true; used[i].buffer = null; return { minor: true, analysis: RM.genericizeMinors(a) }; }
            keep.push(used[i]);
            return { minor: false, count: a.count };
          });
        }
        doc.photoCount = keep.length;

        const { insertedId } = await col.insertOne(doc);
        // 동시 접수 경합 — 넣은 뒤 자기 순번이 한도를 넘으면 자기 것만 지운다
        if (!master) {
          const [rank, pub] = await Promise.all([
            col.countDocuments(Object.assign(usedFilter(mid), { _id: { $lte: insertedId } })),
            // 나보다 먼저(이하) 들어온 공개 건만 — 동시에 들어와 곧 스스로 지워질 뒤쪽 공개 건을 근거로 비공개 6번째가 살아남지 않게
            col.countDocuments(Object.assign(publicFilter(mid), { _id: { $lte: insertedId } })),
          ]);
          if (rank > limitFor(pub, false)) {
            await col.deleteOne({ _id: insertedId });
            dropFiles();
            const [pub2, n2] = await Promise.all([col.countDocuments(publicFilter(mid)), col.countDocuments(usedFilter(mid))]);
            return limitReply(pub2, n2);
          }
        }
        if (keep.length) stashPhotos(insertedId, keep.map(f => ({ buffer: f.buffer, mimeType: f.mimetype, slot: f.slot })));
        files.forEach(f => { if (!keep.includes(f)) f.buffer = null; });

        const [eta, quota] = await Promise.all([
          estimateEta(col, Object.assign({ _id: insertedId }, doc)).catch(() => ({ etaSec: null, ahead: 0 })),
          quotaFor(db, mid).catch(() => null),
        ]);
        // ownerKey 는 여기서만 준다 (mine·job·recent·quota 에는 절대 싣지 않는다)
        return res.json({ ok: true, entryId: String(insertedId), jobId: String(insertedId), etaSec: eta.etaSec, ahead: eta.ahead, quota, minorNotice: !!doc.minorFlag, public: !!doc.public, ownerKey: doc.ownerKey });
      } catch (err) {
        dropFiles();
        console.error('[추석] 접수 오류:', err.message);
        return res.status(500).json({ ok: false, message: '잠시 후 다시 시도해주세요.' });
      }
    });
  });

  // ── 진행 상태 ──
  app.get('/api/chuseok/job/:jobId', allowRead, async (req, res) => {
    try {
      let _id;
      try { _id = new ObjectId(req.params.jobId); } catch { return res.status(400).json({ status: 'failed', message: '잘못된 요청입니다.' }); }
      const db = getDb();
      const col = db.collection(ENTRY_COLLECTION);
      const doc = await col.findOne({ _id });
      if (!doc) return res.status(404).json({ status: 'failed', message: '찾을 수 없습니다.' });
      const finished = doc.status === 'done' || doc.status === 'failed';
      const eta = finished ? { etaSec: 0, ahead: 0 } : await estimateEta(col, doc).catch(() => ({ etaSec: null, ahead: null }));
      const quota = finished ? await quotaFor(db, doc.memberId || null).catch(() => null) : null;
      const reason = doc.status === 'failed' ? String(doc.lastError || '') : '';
      return res.json({
        status: doc.status,
        type: doc.type, typeLabel: doc.typeLabel,
        date: doc.doneAt ? ymd(doc.doneAt) : null,
        imageUrl: doc.imageUrl || null,
        greeting: doc.greeting || null,
        minorNotice: !!doc.minorFlag,
        public: !!doc.public,
        // 실패 이유는 고객이 알아들을 말로만
        failCode: doc.status !== 'failed' ? null : /^budget/.test(reason) ? 'budget' : reason === 'photo_lost' ? 'photo_lost' : 'error',
        counted: doc.status === 'failed' ? !!doc.genAt : true,
        rewarded: !!(quota && quota.rewarded),
        etaSec: eta.etaSec, ahead: eta.ahead,
        quota,
      });
    } catch (err) {
      console.error('[추석] 상태 조회 오류:', err.message);
      return res.status(500).json({ status: 'error', message: '잠시 후 다시 시도해주세요.' });
    }
  });

  // ── 남은 횟수 · 적립 상태 ──
  app.get('/api/chuseok/quota', allowRead, async (req, res) => {
    try {
      const quota = await quotaFor(getDb(), normId(req.query.memberId));
      return res.json(Object.assign({ ok: true, avgSec: Math.round(avgGenMs() / 1000), eventEnd: EVENT_END }, quota));
    } catch (err) {
      console.error('[추석] 횟수 조회 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  // ── 내가 만든 사진 ──
  app.get('/api/chuseok/mine', allowRead, async (req, res) => {
    const mid = normId(req.query.memberId);
    if (!mid) return res.json({ ok: true, items: [] });
    try {
      const rows = await getDb().collection(ENTRY_COLLECTION)
        .find({ memberId: mid, status: 'done', imageUrl: { $ne: null } }).sort({ doneAt: -1 }).limit(30).toArray();
      return res.json({ ok: true, items: rows.map(d => ({ id: String(d._id), type: d.type, typeLabel: d.typeLabel, date: d.doneAt ? ymd(d.doneAt) : null, imageUrl: d.imageUrl, greeting: d.greeting || null, public: !!d.public, hidden: !!d.hidden })) });
    } catch (err) {
      console.error('[추석] 내 사진 조회 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  // ── 공개 ↔ 비공개 (본인) ── 동의 철회는 언제든. 공개 보너스는 "공개 중인 사진" 기준이라 quota 를 같이 돌려준다
  app.post('/api/chuseok/visibility', allowWrite, async (req, res) => {
    try {
      const mid = normId(req.body && typeof req.body.memberId === 'string' ? req.body.memberId : null);
      if (!mid) return res.status(400).json({ ok: false, loginRequired: true, message: '로그인 후 바꿀 수 있어요.' });
      let _id;
      try { _id = new ObjectId(String(req.body.entryId)); } catch { return res.status(400).json({ ok: false, message: '잘못된 요청입니다.' }); }
      const want = req.body.public === true || req.body.public === '1' || req.body.public === 'true';
      const db = getDb();
      const col = db.collection(ENTRY_COLLECTION);
      // 비공개로(동의 철회)는 언제나 된다. 공개로는 만든 기기의 열쇠(ownerKey)가 맞아야 한다
      if (want) {
        const cur = await col.findOne({ _id, memberId: mid });
        if (!cur) return res.status(404).json({ ok: false, message: '내가 만든 사진이 아니에요.' });
        const key = typeof req.body.ownerKey === 'string' ? req.body.ownerKey : '';
        if (!cur.ownerKey || !keyEq(key, cur.ownerKey)) {
          return res.status(403).json({ ok: false, needKey: true, message: '공개는 사진을 만든 기기에서만 바꿀 수 있어요.' });
        }
      }
      const r = await col.updateOne({ _id, memberId: mid }, { $set: Object.assign({ public: want }, want ? { publicAt: nowKST() } : { privateAt: nowKST() }) });
      if (!r.matchedCount) return res.status(404).json({ ok: false, message: '내가 만든 사진이 아니에요.' });
      console.log(`[추석] 공개 설정 ${String(_id)} → ${want ? '공개' : '비공개'}`);
      return res.json({ ok: true, public: want, quota: await quotaFor(db, mid).catch(() => null) });
    } catch (err) {
      console.error('[추석] 공개 설정 오류:', err.message);
      return res.status(500).json({ ok: false, message: '잠시 후 다시 시도해주세요.' });
    }
  });

  // ── 갤러리 (new-event-gallery.html) ── 공개를 고른 완성작만. 관리자가 숨긴 건 빠진다
  app.get('/api/chuseok/recent', allowRead, async (req, res) => {
    try {
      const col = getDb().collection(ENTRY_COLLECTION);
      const limit = Math.min(Math.max(Number(req.query.limit) || 12, 1), 48);
      const offset = Math.min(Math.max(Number(req.query.offset) || 0, 0), 5000);
      const visible = { status: 'done', public: true, hidden: { $ne: true }, imageUrl: { $ne: null } };
      const tab = String(req.query.tab || 'all');
      const filter = TYPES[tab] ? Object.assign({}, visible, { type: tab }) : visible;
      const k = nowKST();
      const dayStart = new Date(k.getFullYear(), k.getMonth(), k.getDate());
      const [rows, total, shown, today] = await Promise.all([
        col.find(filter).sort({ doneAt: -1 }).skip(offset).limit(limit + 1).toArray(),
        col.countDocuments(visible),
        col.countDocuments(filter),
        col.countDocuments(Object.assign({}, visible, { doneAt: { $gte: dayStart } })),
      ]);
      const hasMore = rows.length > limit;
      return res.json({
        ok: true, total, shown, today, offset, hasMore,
        items: (hasMore ? rows.slice(0, limit) : rows).map(d => ({
          date: d.doneAt ? ymd(d.doneAt) : null,
          type: d.type, typeLabel: d.typeLabel || (TYPES[d.type] && TYPES[d.type].label) || '',
          imageUrl: d.imageUrl,
          displayId: d.displayId || null,
          caption: d.type === 'card' ? (d.greeting || null) : null,
        })),
      });
    } catch (err) {
      console.error('[추석] 갤러리 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  // ── 관리 (/chuseok-admin.html) ── 「나의 쉼 순간」 관리 페이지와 같은 방식
  // 키는 헤더 x-chuseok-admin-key (없으면 「나의 쉼 순간」과 같은 REST_MOMENT_ADMIN_KEY / x-rest-admin-key 를 쓴다)
  const allowAdmin = (req, res, next) => {
    // 503 은 클라우드타입 게이트웨이가 자기 에러 페이지로 바꿔치기한다 — 앱 상태는 4xx JSON 으로 알린다
    if (!ADMIN_KEY) return res.status(403).json({ ok: false, notConfigured: true, message: 'CHUSEOK_ADMIN_KEY(또는 REST_MOMENT_ADMIN_KEY)가 서버에 없습니다.' });
    const given = req.get('x-chuseok-admin-key') || req.get('x-rest-admin-key');
    if (!keyEq(given, ADMIN_KEY)) return res.status(401).json({ ok: false, message: '관리 키가 맞지 않습니다.' });
    next();
  };
  const oid = v => { try { return new ObjectId(String(v)); } catch { return null; } };
  const STALE_MS = 2 * 60 * 1000;
  // "적립 확인 필요" 의 정의를 한 곳에 — 통계·목록·release 가 같은 기준을 쓴다 (쉼순간과 동일)
  const problemClauses = staleAt => ({
    unknown:  { settled: 'unknown' },
    calling:  { settled: false, phase: 'calling',  reservedAt: { $lt: staleAt } },
    reserved: { settled: false, phase: 'reserved', reservedAt: { $lt: staleAt } },
  });
  const ADMIN_VIEWS = {
    all:       {},
    public:    { status: 'done', public: true, hidden: { $ne: true } },   // 갤러리에 보이는 것
    hidden:    { status: 'done', hidden: true },                           // 관리자가 뺀 것
    private:   { status: 'done', public: { $ne: true } },                  // 나만 보기
    working:   { status: { $in: ['pending', 'processing'] } },
    failed:    { status: 'failed' },
    marketing: { status: 'done', agreeMarketing: true },                   // (선택) 광고·홍보 활용 동의
  };

  app.get('/api/chuseok/admin/stats', allowAdmin, async (req, res) => {
    try {
      const db = getDb();
      const e = db.collection(ENTRY_COLLECTION), r = db.collection(REWARD_COLLECTION);
      const c = (col, f) => col.countDocuments(f);
      const staleAt = new Date(Date.now() - STALE_MS);
      const p = problemClauses(staleAt);
      const [total, pending, processing, done, failed, pub, hidden, priv, marketing, minor, members,
             card, studio, pet, moon, settled, unknown, calling, reserved, budget] = await Promise.all([
        c(e, {}), c(e, { status: 'pending' }), c(e, { status: 'processing' }), c(e, { status: 'done' }), c(e, { status: 'failed' }),
        c(e, ADMIN_VIEWS.public), c(e, ADMIN_VIEWS.hidden), c(e, ADMIN_VIEWS.private), c(e, ADMIN_VIEWS.marketing), c(e, { minorFlag: true }),
        e.distinct('memberId', { memberId: { $ne: null } }),
        c(e, { status: 'done', type: 'card' }), c(e, { status: 'done', type: 'studio' }), c(e, { status: 'done', type: 'pet' }), c(e, { status: 'done', type: 'moon' }),
        c(r, { $or: [{ settled: true }, { settled: { $exists: false } }] }), c(r, p.unknown), c(r, p.calling), c(r, p.reserved),
        genBudget(e),
      ]);
      return res.json({
        ok: true,
        entries: { total, pending, processing, done, failed, public: pub, hidden, private: priv, marketing, minor, byType: { card, studio, pet, moon } },
        members: members.length,
        rewards: { settled, unknown, calling, staleReserved: reserved, problem: unknown + calling + reserved, points: settled * POINT_AMOUNT },
        gen: { total: budget.total, today: budget.today, maxTotal: MAX_GEN_TOTAL, maxDaily: MAX_GEN_DAILY, allowed: budget.allowed, reason: budget.reason },
        config: {
          maxPerMember: MAX_PER_MEMBER, publicBonus: PUBLIC_BONUS, masterIds: MASTER_IDS, pointAmount: POINT_AMOUNT,
          eventStart: EVENT_START || null, eventEnd: EVENT_END, mateRate: MATE_RATE, watermark: WATERMARK,
          model: OPENAI_MODEL, quality: OPENAI_QUALITY, avgGenSec: Math.round(avgGenMs() / 1000),
        },
      });
    } catch (err) {
      console.error('[추석] 관리 통계 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  app.get('/api/chuseok/admin/entries', allowAdmin, async (req, res) => {
    try {
      const db = getDb();
      const col = db.collection(ENTRY_COLLECTION);
      const limit = Math.min(Math.max(Number(req.query.limit) || 30, 1), 200);
      const offset = Math.min(Math.max(Number(req.query.offset) || 0, 0), 20000);
      const view = Object.prototype.hasOwnProperty.call(ADMIN_VIEWS, String(req.query.view)) ? String(req.query.view) : 'all';
      const filter = Object.assign({}, ADMIN_VIEWS[view]);
      if (TYPES[String(req.query.type)]) filter.type = String(req.query.type);
      const qtext = String(req.query.q || '').trim().slice(0, 60);
      if (qtext) {
        const re = new RegExp(qtext.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
        filter.$or = [{ memberId: qtext.toLowerCase() }, { memberId: re }, { displayId: re }, { greeting: re }];
      }
      const rowsAll = await col.find(filter).sort({ createdAt: -1 }).skip(offset).limit(limit + 1).toArray();
      const hasMore = rowsAll.length > limit;
      const rows = hasMore ? rowsAll.slice(0, limit) : rowsAll;
      // 적립 여부는 회원 단위 — 이 쪽에 나온 회원들의 적립 기록만 한 번에 읽는다
      const mids = [...new Set(rows.map(d => d.memberId).filter(Boolean))];
      const rewarded = new Set();
      if (mids.length) {
        const rw = await db.collection(REWARD_COLLECTION).find({ memberId: { $in: mids } }).toArray();
        rw.forEach(x => { if (x.settled === true || x.settled === undefined) rewarded.add(x.memberId); });
      }
      return res.json({ ok: true, view, offset, hasMore, items: rows.map(d => ({
        id: String(d._id), memberId: d.memberId || null, displayId: d.displayId || null, master: !!d.master,
        type: d.type, typeLabel: d.typeLabel || (TYPES[d.type] && TYPES[d.type].label) || '', greeting: d.greeting || null,
        species: d.species || null, petGender: d.petGender || null, personGender: d.personGender || null, extraPeople: d.extraPeople || 0,
        status: d.status, public: !!d.public, hidden: !!d.hidden, agreeMarketing: !!d.agreeMarketing,
        minorFlag: !!d.minorFlag, photoCount: d.photoCount || 0, mate: d.mate || null,
        imageUrl: d.imageUrl || null, rewarded: rewarded.has(d.memberId),
        tries: d.tries || 0, lastError: d.lastError || null, chargeUnknown: !!d.genUnknownAt,
        createdAt: d.createdAt || null, doneAt: d.doneAt || null,
      })) });
    } catch (err) {
      console.error('[추석] 관리 목록 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  /** 응모 한 건을 파일까지 지운다. 휴지통 기록을 파일 삭제보다 먼저 쓴다 — FTP 도중 죽어도 재시도 근거가 남는다 */
  async function deleteEntry(db, _id, by) {
    const col = db.collection(ENTRY_COLLECTION);
    const found = await col.findOneAndDelete({ _id });
    // 드라이버 6 은 문서를, 5 이하는 { value } 를 돌려준다
    const doc = found && Object.prototype.hasOwnProperty.call(found, 'value') && !found._id ? found.value : found;
    if (!doc) return null;
    photoVault.delete(String(_id));
    const trash = db.collection(TRASH_COLLECTION);
    const { insertedId: trashId } = await trash.insertOne({
      entryId: String(_id), memberId: doc.memberId || null, type: doc.type, greeting: doc.greeting || null, status: doc.status,
      public: !!doc.public, imageUrl: doc.imageUrl || null, deletedBy: by || 'admin', deletedAt: new Date(), ftpRemoved: null, error: null,
    });
    const rm = doc.imageUrl ? await impl.remove(doc.imageUrl) : { ok: true };
    await trash.updateOne({ _id: trashId }, { $set: { ftpRemoved: !!rm.ok, error: rm.ok ? null : rm.error } });
    console.log(`[추석] 관리 삭제(${by || 'admin'}) ${String(_id)} · 파일 ${rm.ok ? '삭제됨' : '삭제 실패 — ' + rm.error}`);
    return { ftpRemoved: !!rm.ok, error: rm.ok ? null : rm.error };
  }

  // action: hide(갤러리에서 빼기) · unhide(되돌리기) · delete(기록+이미지 파일 삭제)
  app.post('/api/chuseok/admin/entries/:id', allowAdmin, async (req, res) => {
    try {
      const db = getDb();
      const _id = oid(req.params.id);
      if (!_id) return res.status(400).json({ ok: false, message: '잘못된 id' });
      const action = String((req.body || {}).action || '');
      if (action === 'hide' || action === 'unhide') {
        const r = await db.collection(ENTRY_COLLECTION).updateOne({ _id }, { $set: { hidden: action === 'hide', reviewedAt: new Date(), reviewedBy: 'admin' } });
        if (!r.matchedCount) return res.status(404).json({ ok: false, message: '기록이 없습니다.' });
        console.log(`[추석] 관리 ${action === 'hide' ? '숨김' : '되돌림'} ${String(_id)}`);
        return res.json({ ok: true, hidden: action === 'hide' });
      }
      if (action === 'delete') {
        const r = await deleteEntry(db, _id, 'admin');
        if (!r) return res.status(404).json({ ok: false, message: '기록이 없습니다.' });
        return res.json({ ok: true, ftpRemoved: r.ftpRemoved, warn: r.ftpRemoved ? null : '기록은 지웠지만 이미지 파일 삭제에 실패했습니다(' + r.error + '). URL 은 휴지통(chuseokTrash)에 남겼습니다.' });
      }
      return res.status(400).json({ ok: false, message: '알 수 없는 action' });
    } catch (err) {
      console.error('[추석] 관리 변경 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });
  // 예전 경로 — 그대로 둔다 (entries/:id 의 hide/unhide 와 같다)
  app.post('/api/chuseok/admin/hide', allowAdmin, async (req, res) => {
    try {
      const _id = oid(req.body && req.body.entryId);
      if (!_id) return res.status(400).json({ ok: false, message: '잘못된 id' });
      const hidden = !(req.body && (req.body.hidden === false || req.body.hidden === '0'));
      const r = await getDb().collection(ENTRY_COLLECTION).updateOne({ _id }, { $set: { hidden, reviewedAt: new Date(), reviewedBy: 'admin' } });
      if (!r.matchedCount) return res.status(404).json({ ok: false, message: '기록이 없습니다.' });
      console.log(`[추석] 관리자 ${hidden ? '숨김' : '되돌림'} ${String(_id)}`);
      return res.json({ ok: true, hidden });
    } catch (err) {
      console.error('[추석] 관리 숨김 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  // 사진 확인에서 돌려보낸 기록 — "자동 분석이 잘 안 된다" 를 이유별로 본다
  app.get('/api/chuseok/admin/rejects', allowAdmin, async (req, res) => {
    try {
      const col = getDb().collection(REJECT_COLLECTION);
      const limit = Math.min(Math.max(Number(req.query.limit) || 50, 1), 200);
      const since = new Date(Date.now() - 7 * 24 * 3600 * 1000);
      const [rows, byReason] = await Promise.all([
        col.find({}).sort({ at: -1 }).limit(limit).toArray(),
        Promise.all(['analysis_failed', 'no_face', 'minor_in_group', 'no_pet', 'minor_with_pet', 'no_people', 'too_many_people']
          .map(r => col.countDocuments({ reason: r, at: { $gte: since } }).then(n => [r, n]))),
      ]);
      return res.json({ ok: true, byReason: Object.fromEntries(byReason), items: rows.map(d => ({
        id: String(d._id), memberId: d.memberId || null, type: d.type, reason: d.reason, at: d.at,
        count: d.count, counts: d.counts, people: d.people, animal: d.animal, photo: d.photo,
      })) });
    } catch (err) {
      console.error('[추석] 관리 거절 목록 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  app.get('/api/chuseok/admin/rewards', allowAdmin, async (req, res) => {
    try {
      const r = getDb().collection(REWARD_COLLECTION);
      const staleAt = new Date(Date.now() - STALE_MS);
      const p = problemClauses(staleAt);
      const state = String(req.query.state || 'problem');
      const filter = state === 'all' ? {} : { $or: [p.unknown, p.calling, p.reserved] };
      const limit = Math.min(Math.max(Number(req.query.limit) || 50, 1), 200);
      const offset = Math.min(Math.max(Number(req.query.offset) || 0, 0), 20000);
      const rowsAll = await r.find(filter).sort({ reservedAt: -1 }).skip(offset).limit(limit + 1).toArray();
      const hasMore = rowsAll.length > limit;
      const rows = hasMore ? rowsAll.slice(0, limit) : rowsAll;
      const canRelease = d => d.settled === 'unknown' || (d.settled === false && d.reservedAt && new Date(d.reservedAt) < staleAt);
      return res.json({ ok: true, state, offset, hasMore, items: rows.map(d => ({
        canRelease: canRelease(d),
        id: String(d._id), memberId: d.memberId, entryId: d.entryId, amount: d.amount,
        settled: d.settled === undefined ? true : d.settled, phase: d.phase || null,
        reservedAt: d.reservedAt || null, settledAt: d.settledAt || null, failedAt: d.failedAt || null,
        lastError: d.lastError || null, participatedAt: d.participatedAt || null, settledBy: d.settledBy || null,
      })) });
    } catch (err) {
      console.error('[추석] 관리 적립 목록 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });
  // settle: Cafe24 어드민에서 실제 적립을 확인한 뒤 "지급됨" 으로 확정 · release: 안 들어간 걸 확인한 뒤 예약 삭제(회원이 다시 받을 수 있게)
  app.post('/api/chuseok/admin/rewards/:id', allowAdmin, async (req, res) => {
    try {
      const r = getDb().collection(REWARD_COLLECTION);
      const _id = oid(req.params.id);
      if (!_id) return res.status(400).json({ ok: false, message: '잘못된 id' });
      const doc = await r.findOne({ _id });
      if (!doc) return res.status(404).json({ ok: false, message: '기록이 없습니다.' });
      const action = String((req.body || {}).action || '');
      if (action === 'settle') {
        await r.updateOne({ _id }, { $set: { settled: true, phase: 'settled', settledAt: new Date(), settledBy: 'admin' } });
      } else if (action === 'release') {
        // 조건부 삭제 — 진행 중(2분 안 된 예약/호출)이거나 확정된 기록은 절대 지우지 않는다
        const staleAt = new Date(Date.now() - STALE_MS);
        const { deletedCount } = await r.deleteOne({ _id, $or: [{ settled: 'unknown' }, { settled: false, reservedAt: { $lt: staleAt } }] });
        if (!deletedCount) return res.status(400).json({ ok: false, message: '진행 중이거나 이미 확정된 기록은 되돌릴 수 없습니다. 2분 뒤 다시 확인해주세요.' });
      } else {
        return res.status(400).json({ ok: false, message: '알 수 없는 action' });
      }
      console.log('[추석] 관리 적립:', action, doc.memberId, String(_id));
      return res.json({ ok: true });
    } catch (err) {
      console.error('[추석] 관리 적립 변경 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  // ── 적립금 — 「나의 쉼 순간」과 같은 흐름 (예약 → Cafe24 호출 → 확정). 계정당 1회 ──
  let rewardIndexReady = false;
  async function ensureRewardIndex(rewards) {
    if (rewardIndexReady) return true;
    try { await rewards.createIndex({ memberId: 1 }, { unique: true }); rewardIndexReady = true; return true; }
    catch (e) { console.error('[추석] ★ reward unique index 실패 — 지급 중단:', e.message); return false; }
  }
  app.post('/api/chuseok/reward', allowWrite, async (req, res) => {
    const memberId = normId(req.body && typeof req.body.memberId === 'string' ? req.body.memberId : null);
    if (!memberId) return res.status(400).json({ ok: false, message: '로그인 후 받을 수 있습니다.' });
    const PENDING_MSG = '적립 처리 결과를 확인하고 있어요. 잠시 후 마이페이지에서 확인해주세요.';
    try {
      const db = getDb();
      const entries = db.collection(ENTRY_COLLECTION);
      const rewards = db.collection(REWARD_COLLECTION);
      const isSettled = d => d.settled === true || d.settled === undefined;
      const already = await rewards.findOne({ memberId });
      if (already) {
        const age = Date.now() - new Date(already.reservedAt || 0).getTime();
        const safeToReset = already.settled === false && already.phase === 'reserved' && age > 2 * 60 * 1000;
        if (!safeToReset) return isSettled(already)
          ? res.status(400).json({ ok: false, alreadyDone: true, message: '이미 적립금을 받으셨습니다.' })
          : res.status(202).json({ ok: false, pending: true, message: PENDING_MSG });
        await rewards.deleteOne({ _id: already._id, phase: 'reserved' });
      }
      const entry = await entries.findOne({ memberId, status: 'done' });
      if (!entry) return res.status(400).json({ ok: false, message: '먼저 사진을 받아주세요.' });
      if (!(await ensureRewardIndex(rewards))) return res.status(409).json({ ok: false, message: '잠시 후 다시 시도해주세요.' });

      const { insertedId } = await rewards.insertOne({ memberId, entryId: String(entry._id), amount: POINT_AMOUNT, settled: false, phase: 'reserved', reservedAt: new Date(), participatedAt: nowKST() });
      await rewards.updateOne({ _id: insertedId }, { $set: { phase: 'calling', apiStartedAt: new Date() } });
      try {
        await apiRequest('POST', `https://${mallId}.cafe24api.com/api/v2/admin/points`, {
          shop_no: 1,
          request: { member_id: memberId, order_id: null, amount: POINT_AMOUNT, type: 'increase', reason: '추석 AI 사진관 이벤트 참여 적립금' },
        }, {}, { timeout: 20000 });
      } catch (apiErr) {
        const st = apiErr && apiErr.response && apiErr.response.status;
        if (st >= 400 && st < 500) {
          await rewards.deleteOne({ _id: insertedId });
          console.error('[추석] 적립금 거절:', memberId, st, JSON.stringify((apiErr.response && apiErr.response.data) || {}));
          return res.status(500).json({ ok: false, message: '잠시 후 다시 시도해주세요.' });
        }
        await rewards.updateOne({ _id: insertedId }, { $set: { settled: 'unknown', lastError: String((apiErr && apiErr.message) || apiErr), failedAt: new Date() } });
        console.error('[추석] ★ 적립금 결과 불명 — 정산 필요:', memberId, String(insertedId));
        return res.status(202).json({ ok: false, pending: true, message: PENDING_MSG });
      }
      try {
        await rewards.updateOne({ _id: insertedId }, { $set: { settled: true, phase: 'settled', settledAt: new Date() } });
      } catch (dbErr) {
        console.error('[추석] ★ 지급 후 확정 실패 — 정산 필요:', memberId, String(insertedId), dbErr.message);
        return res.status(202).json({ ok: false, pending: true, message: PENDING_MSG });
      }
      console.log(`[추석] ${memberId} 적립금 ${POINT_AMOUNT}원 지급 완료`);
      return res.json({ ok: true, message: `🎉 적립금 ${POINT_AMOUNT.toLocaleString()}원이 지급되었습니다!` });
    } catch (err) {
      if (err && err.code === 11000) {
        let d = null;
        try { d = await getDb().collection(REWARD_COLLECTION).findOne({ memberId }); } catch { /* 무시 */ }
        if (d && (d.settled === true || d.settled === undefined)) return res.status(400).json({ ok: false, alreadyDone: true, message: '이미 적립금을 받으셨습니다.' });
        return res.status(202).json({ ok: false, pending: true, message: '적립 처리 중이에요. 잠시 후 확인해주세요.' });
      }
      console.error('[추석] 적립금 지급 오류:', (err.response && err.response.data) || err.message);
      return res.status(500).json({ ok: false, message: '잠시 후 다시 시도해주세요.' });
    }
  });

  // 재배포로 processing 에 멈춘 건 정리 — 워커는 이 프로세스 하나뿐이다
  setTimeout(() => { const db = getDb(); if (db) recoverStuck(db).catch(e => console.warn('[추석] processing 복구 실패:', e.message)); }, 6000).unref();

  if (!deps.noWorker) setInterval(() => tick(getDb), 3000);   // noWorker — 테스트가 워커를 직접 돌릴 때
  console.log('✅ [추석] 라우트 등록 완료 · FTP', FTP_DIR);
}

module.exports = { mount, ENTRY_COLLECTION, REWARD_COLLECTION, TRASH_COLLECTION, REJECT_COLLECTION, TYPES, MAX_PER_MEMBER };
module.exports.__internals = { impl, recoverStuck, cardRef, seedInt, CARD_REF_DIR, maskId, publicFilter, PUBLIC_BONUS, validateInput, splitGreeting, greetingSvg, watermarkSvg, renderFinal, addXmp, WATERMARK, loadFont, cardStyleKeyOf, CARD_TEXT_STYLES, buildJob, quotaFor, usedFilter, processOne, genBudget, estimateEta, analyzePeople, analyzePet, stashPhotos, takePhotos, originAllowed, charLen, GREETING_MAX, OUT_W, OUT_H };
