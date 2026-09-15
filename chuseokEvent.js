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
const ftp = require('basic-ftp');
const { Readable } = require('stream');

const CP = require('./chuseokPrompts');
const RM = require('./restMoment');                      // 금칙어 · 아이 일반화 · 분석 정리 · 아이디 정규화 재사용
const { sanitizeAnalysis, normId } = RM.__internals;

// ── 컬렉션 · 고정 규칙 ───────────────────────────────────────────
const ENTRY_COLLECTION = 'chuseokEntry';
const REWARD_COLLECTION = 'chuseokReward';

const MAX_PER_MEMBER = 5;                                 // 고정 (결정 사항)
const MEMBERS_ONLY = true;                                // 고정 (결정 사항)
const POINT_AMOUNT = Number(process.env.CHUSEOK_POINT || 3000);
const EVENT_START = process.env.CHUSEOK_START || '';      // 비우면 바로 열림 (YYYY-MM-DD, KST)
const EVENT_END = process.env.CHUSEOK_END || '2026-09-30';
const MAX_GEN_TOTAL = process.env.CHUSEOK_MAX_GEN === undefined ? 1000 : Math.max(0, Number(process.env.CHUSEOK_MAX_GEN) || 0);
const MAX_GEN_DAILY = Math.max(0, Number(process.env.CHUSEOK_DAILY_GEN || 0) || 0);
const MASTER_IDS = (process.env.CHUSEOK_MASTER_IDS || process.env.REST_MOMENT_MASTER_IDS || 'testid,yogibo')
  .split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
const WATERMARK = !/^(0|false|no|off)$/i.test(String(process.env.CHUSEOK_WATERMARK || '1'));
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

const MATE_REF_PATH = path.join(__dirname, 'public', 'rest-moment', 'ref-mate-fox.png');

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
    const text = (json.choices && json.choices[0] && json.choices[0].message && json.choices[0].message.content) || '{}';
    return JSON.parse(text);
  },

  /** gpt-image 편집 호출. refs = [{buf, mime, name}] — 순서가 프롬프트의 "reference photo N" 과 맞아야 한다 */
  async imageEdit(prompt, refs) {
    const key = process.env.OPENAI_API_KEY;
    if (!key) throw Object.assign(new Error('OPENAI_API_KEY 없음'), { retryable: false });
    const fd = new FormData();
    fd.append('model', OPENAI_MODEL);
    fd.append('prompt', prompt);
    fd.append('size', '1024x1536');
    fd.append('quality', OPENAI_QUALITY);
    fd.append('output_format', 'png');
    fd.append('n', '1');
    refs.forEach(r => fd.append('image[]', new Blob([r.buf], { type: r.mime }), r.name));
    let res;
    try {
      res = await fetch('https://api.openai.com/v1/images/edits', { method: 'POST', headers: { Authorization: `Bearer ${key}` }, body: fd, signal: AbortSignal.timeout(180000) });
    } catch (e) {
      throw Object.assign(new Error('GPT 연결 실패: ' + (e && e.message)), { retryable: true });   // 과금 전
    }
    if (!res.ok) {
      const t = await res.text();
      throw Object.assign(new Error(`GPT ${res.status}: ${t.slice(0, 240)}`), { status: res.status, retryable: res.status === 429 || res.status >= 500 });
    }
    const json = await res.json();
    const b64 = json && json.data && json.data[0] && json.data[0].b64_json;
    if (!b64) throw Object.assign(new Error('GPT 응답에 이미지가 없습니다'), { retryable: true });
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
};

// ── 사진 확인 (접수 시, 과금 전) ─────────────────────────────────
async function smallJpeg(buf, side = 768) {
  return sharp(buf).rotate().resize({ width: side, height: side, fit: 'inside' }).jpeg({ quality: 82 }).toBuffer();
}
/** 사람 사진 — 인원·연령대·14세 미만 여부. 두 번까지 시도하고 안 되면 null (호출부가 막는다 — 아이 사진이 모델로 새지 않게 닫힌 쪽으로) */
async function analyzePeople(buf) {
  const small = await smallJpeg(buf);
  for (let i = 0; i < 2; i++) {
    try { return sanitizeAnalysis(await impl.visionJson([{ type: 'text', text: CP.PEOPLE_ANALYSIS_PROMPT }, { type: 'image_url', image_url: { url: dataUrl(small), detail: 'low' } }])); }
    catch (e) { console.warn(`[추석] 사람 사진 확인 실패(${i + 1}/2):`, e.message); }
  }
  return null;
}
async function analyzePet(buf) {
  const small = await smallJpeg(buf);
  for (let i = 0; i < 2; i++) {
    try {
      const a = await impl.visionJson([{ type: 'text', text: CP.PET_ANALYSIS_PROMPT }, { type: 'image_url', image_url: { url: dataUrl(small), detail: 'low' } }], 120);
      const animal = ['dog', 'cat', 'other', 'none'].includes(a && a.animal) ? a.animal : 'none';
      return { animal, count: Number(a && a.count) || 0 };
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
function greetingSvg(text, W = OUT_W, H = OUT_H) {
  const lines = splitGreeting(text);
  const longest = Math.max(...lines.map(charLen));
  const size = Math.max(70, Math.min(150, Math.floor((W * 0.84) / Math.max(longest, 1))));
  const lineH = Math.round(size * 1.18);
  const centerY = Math.round(H * 0.25);                   // 생성 이미지를 4:5 로 자른 뒤 비워 둔 띠(원본 14~44%)의 가운데
  const firstBase = Math.round(centerY - ((lines.length - 1) * lineH) / 2 + size * 0.36);
  const font = "Pretendard, 'Pretendard Variable', 'Apple SD Gothic Neo', 'Malgun Gothic', sans-serif";
  const t = (cls, extra) => lines.map((ln, i) =>
    `<text x="${W / 2}" y="${firstBase + i * lineH}" class="${cls}" ${extra || ''}>${escXml(ln)}</text>`).join('');
  const sw1 = Math.round(size * 0.24), sw2 = Math.round(size * 0.12);
  const top = firstBase - size, bottom = firstBase + (lines.length - 1) * lineH + size * 0.3;
  const spark = (x, y, r, c) => `<path d="M${x} ${y - r} L${x + r * 0.22} ${y - r * 0.22} L${x + r} ${y} L${x + r * 0.22} ${y + r * 0.22} L${x} ${y + r} L${x - r * 0.22} ${y + r * 0.22} L${x - r} ${y} L${x - r * 0.22} ${y - r * 0.22} Z" fill="${c}"/>`;
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">
  <defs>
    <linearGradient id="gold" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="#FFFBE0"/><stop offset="0.38" stop-color="#FFD84D"/>
      <stop offset="0.55" stop-color="#F0A21A"/><stop offset="1" stop-color="#FFE7A3"/>
    </linearGradient>
    <filter id="blur" x="-10%" y="-30%" width="120%" height="160%"><feGaussianBlur stdDeviation="${Math.round(size * 0.08)}"/></filter>
  </defs>
  <style>text{font-family:${font};font-size:${size}px;font-weight:900;text-anchor:middle;letter-spacing:-0.02em;}</style>
  <g opacity="0.55" filter="url(#blur)">${t('s', `fill="#000" stroke="#000" stroke-width="${sw1}" transform="translate(0 ${Math.round(size * 0.06)})"`)}</g>
  ${t('o', `fill="none" stroke="#7A0F1E" stroke-width="${sw1}" stroke-linejoin="round"`)}
  ${t('i', `fill="none" stroke="#FFFFFF" stroke-width="${sw2}" stroke-linejoin="round"`)}
  ${t('f', 'fill="url(#gold)"')}
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

/** 생성 원본(2:3) → 4:5 카드 → (축전이면 글자) → 워터마크 → JPEG */
async function renderFinal(genBuf, doc) {
  let img = sharp(genBuf).resize(OUT_W, OUT_H, { fit: 'cover', position: 'centre' });
  const layers = [];
  if (doc.type === 'card' && doc.greeting) layers.push({ input: Buffer.from(greetingSvg(doc.greeting)), top: 0, left: 0 });
  if (WATERMARK) layers.push({ input: Buffer.from(watermarkSvg()), top: 0, left: 0 });
  if (layers.length) img = sharp(await img.png().toBuffer()).composite(layers);
  return img.jpeg({ quality: 90, mozjpeg: true }).toBuffer();
}

// ── 프롬프트 + 참조 조립 ─────────────────────────────────────────
let mateRefCache = null;
async function mateRef() {
  if (mateRefCache) return mateRefCache;
  if (!fs.existsSync(MATE_REF_PATH)) return null;
  mateRefCache = await sharp(fs.readFileSync(MATE_REF_PATH)).resize({ width: 768, height: 768, fit: 'inside', withoutEnlargement: true }).png().toBuffer();
  return mateRefCache;
}

/**
 * 응모 한 건의 프롬프트와 참조 이미지 목록.
 * photos: 보관소에서 꺼낸 사진들 — 접수 때 아이가 보여 버린 사진은 애초에 들어 있지 않다(설명만 문서에 있다).
 */
async function buildJob(doc, photos) {
  const seed = String(doc._id);
  const refs = [];
  const toRef = async (p, i) => ({ buf: await sharp(p.buffer).rotate().resize({ width: 1024, height: 1024, fit: 'inside' }).jpeg({ quality: 88 }).toBuffer(), mime: 'image/jpeg', name: `photo${i + 1}.jpg` });
  let prompt;
  if (doc.type === 'card') {
    prompt = CP.cardPrompt(seed);
  } else if (doc.type === 'studio') {
    // groups 순서 = 사진 1·2 순서. 사진이 있는 그룹만 참조로 보낸다
    const groups = (doc.groups || []).map((g, i) => {
      const ph = (photos || []).find(p => p.slot === i);
      return ph ? { source: 'photo', ph } : { source: 'brief', people: (g.analysis && g.analysis.people) || [] };
    });
    for (const g of groups) if (g.source === 'photo') refs.push(await toRef(g.ph, refs.length));
    prompt = CP.studioPrompt(seed, groups);
  } else if (doc.type === 'pet') {
    const ph = (photos || [])[0];
    if (!ph) throw Object.assign(new Error('반려동물 사진이 메모리에 없습니다(재시작·대기 만료)'), { retryable: false, photoLost: true });
    refs.push(await toRef(ph, 0));
    prompt = CP.petPrompt(seed, doc.petGender, doc.species);
  } else if (doc.type === 'moon') {
    const ph = (photos || [])[0];
    const person = ph ? { source: 'photo' } : { source: 'brief', people: (doc.analysis && doc.analysis.people) || [] };
    if (ph) refs.push(await toRef(ph, 0));
    prompt = CP.moonPrompt(seed, person);
  } else {
    throw Object.assign(new Error('알 수 없는 종류: ' + doc.type), { retryable: false });
  }
  const mate = await mateRef();
  if (mate) refs.push({ buf: mate, mime: 'image/png', name: 'mate.png' });   // 메이트는 늘 마지막 참조
  if (!refs.length) throw Object.assign(new Error('참조 이미지가 없습니다'), { retryable: false });
  return { prompt, refs };
}

// ── 예산 · 횟수 · 예상 시간 ───────────────────────────────────────
async function genBudget(col) {
  const k = nowKST();
  const dayStart = new Date(k.getFullYear(), k.getMonth(), k.getDate());
  const [total, today, processing] = await Promise.all([
    col.countDocuments({ genAt: { $exists: true } }),
    col.countDocuments({ genAt: { $gte: dayStart } }),
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

async function quotaFor(db, mid) {
  const base = { loggedIn: false, master: false, unlimited: false, max: MAX_PER_MEMBER, used: 0, left: MAX_PER_MEMBER, done: 0, rewarded: false, membersOnly: MEMBERS_ONLY };
  if (!mid) return base;
  const [used, done, reward] = await Promise.all([
    db.collection(ENTRY_COLLECTION).countDocuments(usedFilter(mid)),
    db.collection(ENTRY_COLLECTION).countDocuments({ memberId: mid, status: 'done' }),
    db.collection(REWARD_COLLECTION).findOne({ memberId: mid }, { projection: { settled: 1 } }),
  ]);
  const master = isMasterId(mid);
  const rewarded = !!reward && (reward.settled === true || reward.settled === undefined);
  return Object.assign(base, { loggedIn: true, master, unlimited: master, used, done, left: master ? null : Math.max(0, MAX_PER_MEMBER - used), rewarded });
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
      $set: { status: 'done', imageUrl, doneAt: nowKST(), totalMs: Date.now() - t0 },
      $unset: { lastError: '', analysis: '', groups: '' },
    });
    console.log(`[추석] 완성 ${doc._id} (${doc.type})`);
  } catch (err) {
    const tries = (doc.tries || 0) + 1;
    const lastError = String(err && err.message || err).slice(0, 300);
    // 과금 전 일시 오류(429·5xx·연결)만 다시 한다 — 사진은 보관소에 되돌려 둔다(메모리, 30분 만료 그대로)
    if (!charged && err && err.retryable && tries < 3) {
      if (photos) { stashPhotos(doc._id, photos); photos = null; }
      await col.updateOne({ _id: doc._id }, { $set: { status: 'pending', tries, nextAt: new Date(Date.now() + 20000 * tries), lastError } });
      console.error(`[추석] 생성 일시 오류 ${doc._id} (${tries}회, 재시도):`, lastError);
      return;
    }
    wipe();
    await col.updateOne({ _id: doc._id }, { $set: { status: 'failed', tries, lastError, failedAt: new Date() }, $unset: { analysis: '', groups: '' } });
    console.error(`[추석] 생성 실패 ${doc._id}:`, lastError);
  }
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
  const out = { type, def, agreeMarketing: String(body.agreeMarketing) === '1' };
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
        if (!master) {
          const n = await col.countDocuments(usedFilter(mid));
          if (n >= MAX_PER_MEMBER) { dropFiles(); return res.status(400).json({ ok: false, limitReached: true, message: `이 아이디로는 ${MAX_PER_MEMBER}번까지 만들 수 있어요.` }); }
        }

        // ── 사진 확인 (과금 전) — 문제가 있으면 여기서 돌려보내고 횟수도 안 쓴다 ──
        const doc = { type: def.key, typeLabel: def.label, memberId: mid, master, agreeMarketing: v.agreeMarketing, status: 'pending', tries: 0, createdAt: nowKST(), photoCount: 0 };
        const keep = [];                                          // 모델에 보낼 사진만
        if (def.key === 'card') {
          doc.greeting = v.greeting;
        } else if (def.key === 'pet') {
          const a = await analyzePet(used[0].buffer);
          if (!a) { dropFiles(); return res.status(409).json({ ok: false, message: '사진을 확인하지 못했어요. 잠시 후 다시 시도해주세요.' }); }
          if (a.animal === 'none') { dropFiles(); return res.status(400).json({ ok: false, message: '반려동물이 잘 보이는 사진으로 올려주세요.' }); }
          doc.species = a.animal; doc.petGender = v.petGender;
          keep.push(used[0]);
        } else if (def.key === 'moon') {
          const a = await analyzePeople(used[0].buffer);
          if (!a) { dropFiles(); return res.status(409).json({ ok: false, message: '사진을 확인하지 못했어요. 잠시 후 다시 시도해주세요.' }); }
          if (!(a.count > 0)) { dropFiles(); return res.status(400).json({ ok: false, message: '얼굴이 잘 보이는 사진으로 올려주세요.' }); }
          if (a.count > 1) { dropFiles(); return res.status(400).json({ ok: false, message: '한 사람만 나온 사진으로 올려주세요.' }); }
          if (a.minorPresent) { doc.minorFlag = true; doc.analysis = RM.genericizeMinors(a); used[0].buffer = null; }   // ③ 아이 사진은 보내지 않는다
          else keep.push(used[0]);
        } else if (def.key === 'studio') {
          const [a1, a2] = await Promise.all(used.map(f => analyzePeople(f.buffer)));
          if (!a1 || !a2) { dropFiles(); return res.status(409).json({ ok: false, message: '사진을 확인하지 못했어요. 잠시 후 다시 시도해주세요.' }); }
          const empty = [a1, a2].findIndex(a => !(a.count > 0));
          if (empty >= 0) { dropFiles(); return res.status(400).json({ ok: false, message: `사진 ${empty + 1}에 사람이 잘 보이지 않아요. 다른 사진으로 올려주세요.` }); }
          if (a1.count + a2.count > MAX_PEOPLE_TOTAL) { dropFiles(); return res.status(400).json({ ok: false, message: `두 사진을 합쳐 ${MAX_PEOPLE_TOTAL}명까지 함께 담을 수 있어요.` }); }
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
          const rank = await col.countDocuments(Object.assign(usedFilter(mid), { _id: { $lte: insertedId } }));
          if (rank > MAX_PER_MEMBER) {
            await col.deleteOne({ _id: insertedId });
            dropFiles();
            return res.status(400).json({ ok: false, limitReached: true, message: `이 아이디로는 ${MAX_PER_MEMBER}번까지 만들 수 있어요.` });
          }
        }
        if (keep.length) stashPhotos(insertedId, keep.map(f => ({ buffer: f.buffer, mimeType: f.mimetype, slot: f.slot })));
        files.forEach(f => { if (!keep.includes(f)) f.buffer = null; });

        const [eta, quota] = await Promise.all([
          estimateEta(col, Object.assign({ _id: insertedId }, doc)).catch(() => ({ etaSec: null, ahead: 0 })),
          quotaFor(db, mid).catch(() => null),
        ]);
        return res.json({ ok: true, entryId: String(insertedId), jobId: String(insertedId), etaSec: eta.etaSec, ahead: eta.ahead, quota, minorNotice: !!doc.minorFlag });
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
      return res.json({ ok: true, items: rows.map(d => ({ id: String(d._id), type: d.type, typeLabel: d.typeLabel, date: d.doneAt ? ymd(d.doneAt) : null, imageUrl: d.imageUrl, greeting: d.greeting || null })) });
    } catch (err) {
      console.error('[추석] 내 사진 조회 오류:', err.message);
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

  // 재배포로 processing 에 멈춘 건 되살리기 — 워커는 이 프로세스 하나뿐이다
  setTimeout(async () => {
    try {
      const db = getDb(); if (!db) return;
      const r = await db.collection(ENTRY_COLLECTION).updateMany({ status: 'processing' }, { $set: { status: 'pending' }, $unset: { pickedAt: '' } });
      if (r && r.modifiedCount) console.log(`[추석] 멈춘 processing ${r.modifiedCount}건 → pending`);
    } catch (e) { console.warn('[추석] processing 복구 실패:', e.message); }
  }, 6000).unref();

  if (!deps.noWorker) setInterval(() => tick(getDb), 3000);   // noWorker — 테스트가 워커를 직접 돌릴 때
  console.log('✅ [추석] 라우트 등록 완료 · FTP', FTP_DIR);
}

module.exports = { mount, ENTRY_COLLECTION, REWARD_COLLECTION, TYPES, MAX_PER_MEMBER };
module.exports.__internals = { impl, validateInput, splitGreeting, greetingSvg, watermarkSvg, renderFinal, buildJob, quotaFor, usedFilter, processOne, genBudget, estimateEta, analyzePeople, analyzePet, stashPhotos, takePhotos, originAllowed, charLen, GREETING_MAX, OUT_W, OUT_H };
