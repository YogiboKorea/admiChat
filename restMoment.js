// ================================================================
// 「나의 쉼 순간」 이벤트 모듈  (2026.09.11 ~ 09.27)
//
//  고객이 칩 1개 + 한 문장(+선택적으로 사진)을 남기면, 그 사람의 쉼 장면을
//  그림 한 장으로 만들어 돌려준다. 참여 완료 시 적립금 3,000원.
//
//  ── 반드시 지켜야 하는 것 (개발요청서 「개인정보·법적 요건」) ──
//  ① 고객이 올린 원본 사진은 메모리에서만 다루고 즉시 파기한다.
//     디스크·로그·캐시 어디에도 남기지 않는다. multer 는 memoryStorage 만 쓴다.
//  ② 공개 URL 에 회원 아이디를 노출하지 않는다.
//     파일명은 `YYYYMMDD_<난수8>` 이고, 누가 만들었는지는 Mongo 에만 있다.
//  ③ 결과 이미지에 "YOGIBO · AI 생성 이미지" 워터마크를 반드시 넣는다.
//  ④ 생성이 실패해도 고객이 빈손으로 나가지 않는다 — 기본 실루엣으로 완성한다.
//
//  라우트는 server.js 에서 mount(app, deps) 로 붙인다.
// ================================================================
'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { execFile } = require('child_process');
const multer = require('multer');
const sharp = require('sharp');
const ftp = require('basic-ftp');
const { Readable } = require('stream');

// ── 컬렉션 ────────────────────────────────────────────────────────
const ENTRY_COLLECTION = 'restMomentEntry';
const REWARD_COLLECTION = 'restMomentReward';
const TRASH_COLLECTION = 'restMomentTrash';   // 관리자가 지운 응모의 URL 기록 — 파일 삭제 실패 시 재시도 근거

// ── 설정 ──────────────────────────────────────────────────────────
const MAX_PHOTO_BYTES = 12 * 1024 * 1024;
const MAX_SENTENCE = 60;
const POINT_AMOUNT = Number(process.env.REST_MOMENT_POINT || 3000);
const EVENT_END = process.env.REST_MOMENT_END || '2026-09-27';
// 아이디당 생성 횟수. 마스터 아이디는 검수·테스트용이라 제한을 받지 않는다.
const MAX_PER_MEMBER = Number(process.env.REST_MOMENT_MAX_PER_MEMBER || 3);
// 이벤트 생성 상한. 넘으면 과금 없이 실루엣으로 완성한다 — 고객은 그림과 적립금을 받고, 우리는 돈이 안 나간다.
//   REST_MOMENT_MAX_GEN    이벤트 전체 GPT 생성 장수. 기본 1000 (≈ $180~200, 건당 ≈ $0.18~0.20). 0 이면 무제한.
//   REST_MOMENT_DAILY_GEN  하루 GPT 생성 장수 (KST 자정 기준). 기본 0 = 무제한.
const MAX_GEN_TOTAL = process.env.REST_MOMENT_MAX_GEN === undefined ? 1000 : Math.max(0, Number(process.env.REST_MOMENT_MAX_GEN) || 0);
const MAX_GEN_DAILY = Math.max(0, Number(process.env.REST_MOMENT_DAILY_GEN || 0) || 0);
const MASTER_IDS = (process.env.REST_MOMENT_MASTER_IDS || 'testid,yogibo')
  .split(',').map(s => s.trim()).filter(Boolean);
// memberId 는 클라이언트가 보내는 값이라 'testid' 는 누구나 보낼 수 있다.
// REST_MOMENT_MASTER_KEY 를 두면 폼의 masterKey 까지 맞아야 마스터다 (페이지는 ?ai_master=키 로 받아 보낸다).
// 키를 두지 않으면 아이디만으로 판정한다 — 베타 동안의 기본값.
const MASTER_KEY = String(process.env.REST_MOMENT_MASTER_KEY || '').trim();
// 갤러리 노출. 기본은 금칙어에 안 걸린 건 자동 승인(베타). REST_MOMENT_REQUIRE_REVIEW=1 이면 관리자가 승인해야 보인다.
const REQUIRE_REVIEW = /^(1|true|yes)$/i.test(String(process.env.REST_MOMENT_REQUIRE_REVIEW || ''));
// 관리 페이지(/rest-admin.html)와 관리 API 를 여는 키. 없으면 관리 API 는 503 으로 닫힌다.
let ADMIN_KEY = String(process.env.REST_MOMENT_ADMIN_KEY || '').trim();
if (ADMIN_KEY && !/^[\x21-\x7E]+$/.test(ADMIN_KEY)) {
  // HTTP 헤더는 ASCII 만 안전하게 실린다. 한글 키는 브라우저가 못 보내므로 아예 닫는다.
  console.error('★ [쉼순간] REST_MOMENT_ADMIN_KEY 는 영문·숫자·기호(ASCII)만 됩니다 — 관리 API 를 닫습니다');
  ADMIN_KEY = '';
}
const sha256 = v => crypto.createHash('sha256').update(String(v)).digest('hex');
function keyEq(given, expected) {
  if (!expected || !given) return false;
  const a = Buffer.from(String(given)), b = Buffer.from(String(expected));
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}
function adminKeyOk(given) { return keyEq(given, ADMIN_KEY); }
// 키는 헤더(x-rest-master-key) 우선, 본문 masterKey 폴백. URL 쿼리로는 받지 않는다 — 접근로그·Referer 에 남는다.
function masterKeyOf(req) {
  const h = req && typeof req.get === 'function' ? req.get('x-rest-master-key') : (req && req.headers && req.headers['x-rest-master-key']);
  return String(h || (req && req.body && req.body.masterKey) || '').trim();
}
function isMaster(id, req) {
  if (!id || !MASTER_IDS.includes(String(id))) return false;
  if (!MASTER_KEY) return true;                       // 베타: 키 미설정이면 아이디만 (생성 무제한 용도)
  return keyEq(masterKeyOf(req), MASTER_KEY);
}
// 파괴적 조치(숨김·삭제)와 아이디 열거(/recent 마스터 모드)는 키가 있어야만 연다 — 아이디만으로는 절대 열리지 않는다.
function isMasterStrict(id, req) { return !!MASTER_KEY && isMaster(id, req); }

// 갤러리에 보여줄 아이디. 앞 두 글자만 남기고 가린다 — 당첨자 발표 관례와 같다.
function maskId(id) {
  let t = String(id || '');
  if (!t) return null;
  const at = t.indexOf('@');
  if (at > 0) t = t.slice(0, at);           // 이메일형 아이디는 도메인을 통째로 가린다
  if (t.length <= 2) return t.charAt(0) + '***';
  return t.slice(0, 2) + '***' + (t.length >= 6 ? t.slice(-1) : '');
}
// 마스터 아이디로 만든 건은 여러 사람이 참여한 것처럼 보여야 한다.
// 저장 시점에 한 번 뽑아 고정한다 — 불러올 때마다 바뀌면 바로 티가 난다.
const FAKE_HEADS = ['su','mi','yo','ji','ha','se','ju','da','so','na','eu','hy','ky','bo','ye','ch','ka','ri','wo','ta','je','in','do','ga'];
function fakeDisplayId() {
  const head = FAKE_HEADS[Math.floor(Math.random() * FAKE_HEADS.length)];
  const tail = Math.random() < 0.6 ? String(Math.floor(Math.random() * 10)) : '';
  return head + '***' + tail;
}

// 적립 기록의 memberId unique index. 동시 클릭을 막는 유일한 장치라 첫 지급 전에 반드시 있어야 한다.
let rewardIndexReady = false;
async function ensureRewardIndex(rewards) {
  if (rewardIndexReady) return true;
  try { await rewards.createIndex({ memberId: 1 }, { unique: true }); rewardIndexReady = true; return true; }
  catch (e) {
    // 같은 키에 옵션이 다른 인덱스가 있으면 여기로 온다. 인덱스 없이는 동시 클릭을 못 막으므로 지급을 멈춘다.
    console.error('[쉼순간] ★ rewards unique index 생성 실패 — 지급 중단:', e.message);
    return false;
  }
}

const FTP_DIR = process.env.FTP_REST_DIR || '/web/img/md/09';
// Cafe24 FTP 는 계정 홈에 갇혀 있어 절대경로(CWD /)를 550 으로 거부한다 — server.js 처럼 상대경로로 쓴다.
const FTP_DIR_REL = FTP_DIR.split('/').filter(Boolean).join('/');
const FTP_PUBLIC = (process.env.FTP_REST_PUBLIC_BASE || 'https://yogibo.openhost.cafe24.com/web/img/md/09').replace(/\/$/, '');

// 쓰기(응모·적립금)를 허용할 오리진. 몰 본 도메인 + Cafe24 스킨 미리보기까지 포함해야
// 실제로 붙여넣고 테스트할 수 있다. `*` 는 서브도메인 한 칸을 뜻한다.
const ALLOWED_ORIGINS = (process.env.REST_ALLOWED_ORIGINS || [
  'https://yogibo.kr',
  'https://www.yogibo.kr',
  'https://yogibo.co.kr',
  'https://www.yogibo.co.kr',
  'https://yogibo.cafe24.com',
  'https://*.yogibo.cafe24.com',
  // 테스트 스킨은 http 로 뜬다 (skin-skin123.yogibo.cafe24.com). Cafe24 내부 도메인이라 허용한다.
  'http://yogibo.cafe24.com',
  'http://*.yogibo.cafe24.com',
].join(',')).split(',').map(s => s.trim()).filter(Boolean);

function originAllowed(origin) {
  if (!origin) return true;                    // 서버 간 호출·직접 접속
  return ALLOWED_ORIGINS.some(rule => {
    if (rule === origin) return true;
    if (!rule.includes('*')) return false;
    const re = new RegExp('^' + rule.split('*').map(s =>
      s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('[^.]+') + '$');
    return re.test(origin);
  });
}

// 칩 → 유형·제품 매핑. 2026.08.31 실재고 기준 확정본.
// 폼 6종이 나오기 전까지는 baseKey 가 임시 시안 이미지를 가리킨다.
const CHIPS = {
  sink:    { type: '무너지는 사람',      product: '맥스',   form: '세운 형태',  color: '라이트그레이', hex: '#E5DED3', baseKey: 'rest-sink' },
  lean:    { type: '기대는 사람',        product: '라운저', form: '기댄 형태',  color: '아쿠아블루',   hex: '#0075BD', baseKey: 'rest-lean' },
  liedown: { type: '눕는 사람',          product: '맥스',   form: '눕힌 형태',  color: '라이트그레이', hex: '#E5DED3', baseKey: 'rest-liedown' },
  floor:   { type: '바닥에 앉는 사람',   product: '드롭',   form: '바닥형',     color: '올리브그린',   hex: '#668B01', baseKey: 'rest-floor' },
  myspot:  { type: '내 자리를 두는 사람', product: '미니',   form: '1인용',      color: '다크그레이',   hex: '#615F5F', baseKey: 'rest-myspot' },
  hug:     { type: '안는 사람',          product: '서포트', form: '안는 형태',  color: '올리브그린',   hex: '#668B01', baseKey: 'rest-hug' },
};

// ── 한글 조판용 폰트 부트스트랩 ───────────────────────────────────
// sharp 의 SVG 렌더러는 fontconfig 를 탄다. 컨테이너에 한글 폰트가 없으면
// 문장이 통째로 두부(□□□)로 나온다 — 조용히 깨지는 종류라 부팅 때 확인한다.
let FONT_READY = false;
function bootstrapFont() {
  const src = path.join(__dirname, 'fonts', 'PretendardVariable.ttf');
  if (!fs.existsSync(src)) {
    console.warn('⚠️ [쉼순간] PretendardVariable.ttf 없음 — 문장 조판이 깨집니다');
    return;
  }
  const dirs = ['/usr/share/fonts/truetype', path.join(os.homedir(), '.fonts')];
  for (const d of dirs) {
    try {
      fs.mkdirSync(d, { recursive: true });
      fs.copyFileSync(src, path.join(d, 'PretendardVariable.ttf'));
      FONT_READY = true;
      break;
    } catch (e) { /* 다음 경로 시도 */ }
  }
  if (!FONT_READY) {
    console.warn('⚠️ [쉼순간] 폰트 설치 실패 — 문장 조판이 깨질 수 있습니다');
    return;
  }
  execFile('fc-cache', ['-f'], () => {
    console.log('✅ [쉼순간] Pretendard 등록 완료');
  });
}

// ── 유틸 ──────────────────────────────────────────────────────────
const pad = n => String(n).padStart(2, '0');

/** KST 기준 현재 시각 */
/**
 * 생성 예산. 완료된 GPT 장면(via:'gpt-scene')과 지금 그리는 중인 건(processing)을 합쳐 상한과 비교한다.
 * processing 에는 이 건 자신도 들어 있으므로 하나 뺀다 — 안 빼면 정확히 상한 장수째가 막힌다.
 */
async function genBudget(col) {
  const k = nowKST();
  const dayStart = new Date(k.getFullYear(), k.getMonth(), k.getDate());
  const [total, today, processing] = await Promise.all([
    col.countDocuments({ via: 'gpt-scene' }),
    col.countDocuments({ via: 'gpt-scene', doneAt: { $gte: dayStart } }),
    col.countDocuments({ status: 'processing' }),
  ]);
  const inflight = Math.max(0, processing - 1);
  let reason = null;
  if (MAX_GEN_TOTAL > 0 && total + inflight >= MAX_GEN_TOTAL) reason = 'total';
  else if (MAX_GEN_DAILY > 0 && today + inflight >= MAX_GEN_DAILY) reason = 'daily';
  return { total, today, inflight, allowed: !reason, reason, maxTotal: MAX_GEN_TOTAL, maxDaily: MAX_GEN_DAILY };
}

function nowKST() {
  return new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
}
function ymd(d) {
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

/**
 * 공개 파일명. 회원 아이디를 넣지 않는다 — /web/img/ 는 공개 경로라
 * 파일명이 곧 신원 노출이 된다. 누구 것인지는 Mongo 에만 둔다.
 */
function publicName(suffix) {
  const d = nowKST();
  const stamp = `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}`;
  return `${stamp}_${crypto.randomBytes(4).toString('hex')}${suffix}.jpg`;
}

function escXml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&apos;' }[c]));
}

/** 부적절 표현 1차 자동 필터. 통과해도 갤러리 공개는 수동 검수 후다. */
const BANNED = ['씨발', '시발', '병신', '좆', '개새', '섹스', 'ㅅㅂ', 'ㅄ', '자살', '죽어'];
// 한글 음절을 호환 자모(초·중·종성)로 풀어 쓴다. '씨.발' '씨1발' 'ㅆㅣㅂㅏㄹ' 같은 우회를 같은 형태로 만들기 위해서다.
const CHO  = ['ㄱ','ㄲ','ㄴ','ㄷ','ㄸ','ㄹ','ㅁ','ㅂ','ㅃ','ㅅ','ㅆ','ㅇ','ㅈ','ㅉ','ㅊ','ㅋ','ㅌ','ㅍ','ㅎ'];
const JUNG = ['ㅏ','ㅐ','ㅑ','ㅒ','ㅓ','ㅔ','ㅕ','ㅖ','ㅗ','ㅘ','ㅙ','ㅚ','ㅛ','ㅜ','ㅝ','ㅞ','ㅟ','ㅠ','ㅡ','ㅢ','ㅣ'];
const JONG = ['','ㄱ','ㄲ','ㄳ','ㄴ','ㄵ','ㄶ','ㄷ','ㄹ','ㄺ','ㄻ','ㄼ','ㄽ','ㄾ','ㄿ','ㅀ','ㅁ','ㅂ','ㅄ','ㅅ','ㅆ','ㅇ','ㅈ','ㅊ','ㅋ','ㅌ','ㅍ','ㅎ'];
function jamo(str) {
  let out = '';
  for (const ch of String(str)) {
    const c = ch.charCodeAt(0);
    if (c >= 0xAC00 && c <= 0xD7A3) {
      const i = c - 0xAC00;
      out += CHO[Math.floor(i / 588)] + JUNG[Math.floor((i % 588) / 28)] + JONG[i % 28];
    } else out += ch;
  }
  return out;
}
const BANNED_FORMS = BANNED.reduce((acc, w) => acc.concat([w, jamo(w)]), []);
function looksInappropriate(text) {
  // 공백·기호를 걷어낸 형태, 숫자까지 걷어낸 형태, 그 둘의 자모 분해형 — 넷 중 하나라도 걸리면 부적절로 본다.
  const base = String(text || '').toLowerCase().replace(/[^가-힣ㄱ-ㅎㅏ-ㅣa-z0-9]/g, '');
  const noDigit = base.replace(/[0-9]/g, '');
  const forms = [base, noDigit, jamo(base), jamo(noDigit)];
  return forms.some(f => BANNED_FORMS.some(w => w && f.includes(w)));
}

function withinEventPeriod(d) {
  const t = ymd(d);
  // 시작일은 막지 않는다 — 오픈 전 테스트를 위해 종료일만 본다.
  return t <= EVENT_END;
}

// ── FTP ───────────────────────────────────────────────────────────
/** 버퍼 1개를 FTP 에 올리고 공개 URL 을 돌려준다. */
async function ftpUpload(buffer, filename) {
  // 잠깐의 FTP 흔들림이 잡 재시도(= 생성 재과금)로 번지지 않게 여기서 3번까지 다시 붙는다.
  let last;
  for (let i = 0; i < 3; i++) {
    try { return await ftpUploadOnce(buffer, filename); }
    catch (e) { last = e; await new Promise(r => setTimeout(r, 1500 * (i + 1))); }
  }
  throw last;
}
async function ftpUploadOnce(buffer, filename) {
  const client = new ftp.Client(30000);
  client.ftp.verbose = false;
  try {
    await client.access({
      host: process.env.FTP_HOST || 'yogibo.ftp.cafe24.com',   // server.js 의 다른 FTP 코드와 같은 기본값
      port: Number(process.env.FTP_PORT || 21),
      user: process.env.FTP_USER,
      password: process.env.FTP_PASS,
      secure: false,
    });
    await client.ensureDir(FTP_DIR_REL);
    await client.uploadFrom(Readable.from(buffer), filename);
    return `${FTP_PUBLIC}/${filename}`;
  } finally {
    client.close();
  }
}

// 공개 URL 하나를 FTP 에서 지운다. 파일명 형식을 검사해 이 이벤트가 올린 파일 밖의 것은 절대 건드리지 않는다.
const PUBLIC_NAME_RE = /^\d{8}_[0-9a-f]{8}(_s)?\.jpg$/;
async function ftpRemoveByUrl(url) {
  if (!url) return { ok: true };
  let name;
  try { name = path.posix.basename(new URL(url).pathname); } catch { return { ok: false, error: '잘못된 URL: ' + url }; }
  if (!PUBLIC_NAME_RE.test(name)) return { ok: false, error: '지울 수 없는 파일명: ' + name };
  if (!process.env.FTP_USER) return { ok: false, error: 'FTP 계정 미설정' };
  const client = new ftp.Client(30000);
  client.ftp.verbose = false;
  try {
    await client.access({
      host: process.env.FTP_HOST || 'yogibo.ftp.cafe24.com',
      port: Number(process.env.FTP_PORT || 21),
      user: process.env.FTP_USER,
      password: process.env.FTP_PASS,
      secure: false,
    });
    await client.remove(`${FTP_DIR_REL}/${name}`);
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  } finally {
    client.close();
  }
}

// ── 이미지 합성 ───────────────────────────────────────────────────
/**
 * 배경 + 폼 + 인물 + (문장) + 워터마크.
 *
 * 지금은 폼 6종·배경 세트가 디자이너 작업 중이라, 칩별 시안 이미지를 베이스로
 * 쓴다. 개발요청서의 "임시 이미지로 흐름을 먼저 완성하고 폼이 나오면 교체"
 * 그대로다. 폼이 나오면 loadBase() 만 바꾸면 된다.
 */
function poseVariants(baseKey, kind) {
  const dir = path.join(__dirname, 'public', 'rest-moment');
  if (!fs.existsSync(dir)) return [];
  const re = new RegExp('^' + baseKey.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '-' + kind + '\\d+\\.jpe?g$', 'i');
  return fs.readdirSync(dir).filter(f => re.test(f)).sort();
}

/**
 * 이 응모가 쓸 포즈 컷의 경로. 없으면 null.
 *   -d1.jpg, -d2.jpg …  둘이 붙어 앉은 컷 (제품 위 2인일 때만)
 *   -p1.jpg, -p2.jpg …  1인 컷
 * 2인인데 duo 컷이 없으면 1인 컷으로 떨어진다.
 */
function posePath(chipKey, seed, seated) {
  const chip = CHIPS[chipKey];
  if (!chip) return null;
  const dir = path.join(__dirname, 'public', 'rest-moment');
  const pick = kind => {
    const list = poseVariants(chip.baseKey, kind);
    return list.length ? path.join(dir, pickPose(list, seed)) : null;
  };
  return (seated >= 2 && pick('d')) || pick('p');
}

/** 같은 응모는 늘 같은 포즈 컷을 쓴다 — 재시도가 다른 그림이 되면 안 된다. */
function pickPose(list, seed) {
  if (list.length < 2) return list[0];
  let h = 0;
  const s = String(seed || '');
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  return list[h % list.length];
}

async function loadBase(chipKey, seed) {
  const chip = CHIPS[chipKey];
  const dir = path.join(__dirname, 'public', 'rest-moment');
  const pose = posePath(chipKey, seed, 1);
  if (pose) { console.log(`[쉼순간] ${chipKey} 포즈 컷 ${path.basename(pose)}`); return fs.readFileSync(pose); }
  const local = path.join(dir, `${chip.baseKey}.jpg`);
  if (fs.existsSync(local)) return fs.readFileSync(local);
  // 로컬에 없으면 FTP 공개본에서 받아 쓴다
  if (!/^https?:\/\//.test(FTP_PUBLIC)) throw new Error(`FTP_REST_PUBLIC_BASE 가 http(s) 주소가 아닙니다: "${FTP_PUBLIC}"`);
  const url = `${FTP_PUBLIC}/${chip.baseKey}.jpg`;
  const res = await fetch(url, { signal: AbortSignal.timeout(15000) });
  if (!res.ok) throw new Error(`베이스 이미지를 찾을 수 없습니다: ${url} (${res.status})`);
  return Buffer.from(await res.arrayBuffer());
}

/**
 * 출력 크기 — 페이지가 최대 1400px 폭으로 깔리므로 가장 긴 변을 1400 에 맞춘다.
 * 4:5 를 유지하니 1120x1400. 원본(gpt-image-2 1024x1536)에서 가로가 9% 확대되는데,
 * 1024 그대로 두고 1400 폭에 늘려 까는 것보다 이쪽이 선명하다.
 */
const OUT_W = 1120, OUT_H = 1400;
const CARD_BAND = 328;                 // 공유 카드 하단 문장 띠 (1120 폭에 맞춰 비례)

/** 갤러리용 — 문장 없이 그림만. 워터마크는 필수라 여기에도 넣는다. */
async function renderArtwork(baseBuf) {
  const W = OUT_W, H = OUT_H;
  const img = sharp(baseBuf).resize(W, H, { fit: 'cover' });
  const mark = Buffer.from(
    `<svg width="${W}" height="${H}" xmlns="http://www.w3.org/2000/svg">
       <text x="${W - 28}" y="${H - 26}" text-anchor="end"
             font-family="Pretendard" font-size="20" font-weight="500"
             fill="#FFFFFF" fill-opacity="0.72">YOGIBO · AI 생성 이미지</text>
     </svg>`);
  return img.composite([{ input: mark, top: 0, left: 0 }]).jpeg({ quality: 86, mozjpeg: true }).toBuffer();
}

/**
 * 저장·공유용 — 그림 아래 여백 띠에 문장을 조판한다.
 * 구성안: "그림 위에 글자가 얹히면 답답해 보인다. 겹치지 않게 그림 아래 여백 띠에."
 */
async function renderShareCard(baseBuf, sentence, typeLabel) {
  const W = OUT_W, IMG_H = OUT_H, BAND = CARD_BAND, H = IMG_H + BAND;

  const art = await sharp(baseBuf).resize(W, IMG_H, { fit: 'cover' }).toBuffer();

  // 60자를 한 줄에 다 못 넣는다. 24자 안팎으로 접는다.
  const text = String(sentence || '').trim();
  const lines = [];
  let cur = '';
  for (const ch of text) {
    cur += ch;
    if (cur.length >= 24) { lines.push(cur); cur = ''; }
  }
  if (cur) lines.push(cur);
  const body = lines.slice(0, 3);

  const d = nowKST();
  const dateStr = `${d.getFullYear()}. ${pad(d.getMonth() + 1)}. ${pad(d.getDate())}`;

  const bandSvg = Buffer.from(
    `<svg width="${W}" height="${H}" xmlns="http://www.w3.org/2000/svg">
       <rect x="0" y="${IMG_H}" width="${W}" height="${BAND}" fill="#FFFFFF"/>
       ${body.map((l, i) =>
         `<text x="${W / 2}" y="${IMG_H + 92 + i * 52}" text-anchor="middle"
                font-family="Pretendard" font-size="40" font-weight="700"
                fill="#1A1A1A">${escXml(l)}</text>`).join('')}
       <text x="${W / 2}" y="${IMG_H + 96 + body.length * 52 + 26}" text-anchor="middle"
             font-family="Pretendard" font-size="24" font-weight="500"
             fill="#6D6D6D">${escXml(dateStr)} · ${escXml(typeLabel)}</text>
       <text x="${W / 2}" y="${H - 34}" text-anchor="middle"
             font-family="Pretendard" font-size="21" font-weight="500"
             fill="#9A9A9A">YOGIBO · AI 생성 이미지 · #나의쉼순간</text>
     </svg>`);

  return sharp({ create: { width: W, height: H, channels: 3, background: '#FFFFFF' } })
    .composite([{ input: art, top: 0, left: 0 }, { input: bandSvg, top: 0, left: 0 }])
    .jpeg({ quality: 88, mozjpeg: true })
    .toBuffer();
}

// ── 고객 사진 보관 (메모리 전용) ──────────────────────────────────
// 생성은 20~40초 걸려 접수와 분리해야 하는데, 사진을 Mongo 나 디스크에
// 두면 "변환 즉시 파기" 약속이 깨진다. 그래서 프로세스 메모리에만 잠깐 둔다.
//  · 생성이 끝나면 즉시 지운다
//  · 10분이 지나면 자동으로 지운다
//  · 컨테이너가 재시작하면 같이 사라진다 → 그 건은 실루엣으로 완성된다
const photoVault = new Map();
const PHOTO_TTL_MS = 10 * 60 * 1000;

function stashPhoto(id, buffer, mimeType) {
  photoVault.set(String(id), { buffer, mimeType, at: Date.now() });
}
function takePhoto(id) {
  const k = String(id);
  const v = photoVault.get(k);
  photoVault.delete(k);          // 꺼내는 즉시 보관소에서 제거
  return v || null;
}
setInterval(() => {
  const cutoff = Date.now() - PHOTO_TTL_MS;
  for (const [k, v] of photoVault) if (v.at < cutoff) photoVault.delete(k);
}, 60 * 1000);

// ── 인물 레이어 생성 ──────────────────────────────────────────────
// 개발요청서: "AI 가 만드는 것은 인물 레이어 하나뿐". 제품·배경은 사전 제작
// 자산에서 고르고, 런타임에 그리지 않는다 — 형상이 달라지면 표시·광고 리스크다.
//
// 주 경로는 GPT 다. gpt-image-2 만 mask 파라미터(제품 영역 잠금)와
// background:"transparent"(인물만 뽑아 합성) 를 공식 지원한다.
// 제미나이에는 두 기능이 없어서 "제품 건드리지 마"를 프롬프트로 부탁하는 수밖에 없다.
const OPENAI_MODEL = process.env.OPENAI_IMAGE_MODEL || 'gpt-image-2';

function personPrompt(chip) {
  return [
    'Replace only the human figure in the first image with a stylized illustrated character',
    'whose build, hair and clothing are loosely inspired by the person in the second photo.',
    'The face must be simplified and non-identifiable — soft painterly strokes, eyes closed,',
    'calm neutral expression. Do NOT reproduce a recognizable likeness.',
    `Keep the ${chip.product} bean bag, the room, the lighting and the illustration style of the`,
    'first image EXACTLY unchanged — same shape, same color, same position, same brushwork.',
    'The bean bag has no legs, no frame, no armrests, no buttons and no seam panels.',
    'No text, no logo, no watermark anywhere in the image.',
  ].join(' ');
}

/** GPT — 마스크가 있으면 제품 영역을 잠그고, 없으면 프롬프트로 통제한다. */
async function generateWithGPT(baseBuf, photo, chip, maskBuf) {
  const key = process.env.OPENAI_API_KEY;
  if (!key) throw new Error('OPENAI_API_KEY 없음');

  const fd = new FormData();
  fd.append('model', OPENAI_MODEL);
  fd.append('prompt', personPrompt(chip));
  fd.append('size', '1024x1536');
  fd.append('n', '1');
  fd.append('image[]', new Blob([baseBuf], { type: 'image/png' }), 'base.png');
  fd.append('image[]', new Blob([photo.buffer], { type: photo.mimeType || 'image/jpeg' }), 'person.jpg');
  // 폼 6종이 나오면 폼별 제품영역 마스크를 함께 넣는다 (Q2 경로)
  if (maskBuf) fd.append('mask', new Blob([maskBuf], { type: 'image/png' }), 'mask.png');

  const res = await fetch('https://api.openai.com/v1/images/edits', {
    method: 'POST',
    headers: { Authorization: `Bearer ${key}` },
    body: fd,
    signal: AbortSignal.timeout(180000),
  });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`GPT ${res.status}: ${t.slice(0, 200)}`);
  }
  const json = await res.json();
  const b64 = json && json.data && json.data[0] && json.data[0].b64_json;
  if (!b64) throw new Error('GPT 응답에 이미지가 없습니다');
  return Buffer.from(b64, 'base64');
}

/** 제미나이 — GPT 가 막혔을 때의 2차. 마스크가 없어 통제가 약하다. */
async function generateWithGemini(baseBuf, photo, chip) {
  const key = process.env.GEMINI_API_KEY;
  if (!key) throw new Error('GEMINI_API_KEY 없음');
  const model = process.env.GEMINI_IMAGE_MODEL || 'gemini-3-pro-image';

  // 요청 본문이 커지면 통째로 실패한다 — 참조는 1024px 로 줄여 보낸다
  const small = await sharp(photo.buffer).resize(1024, 1024, { fit: 'inside', withoutEnlargement: true })
    .jpeg({ quality: 82 }).toBuffer();

  const res = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-goog-api-key': key },
      body: JSON.stringify({
        contents: [{
          role: 'user',
          parts: [
            { text: personPrompt(chip) },
            { inlineData: { mimeType: 'image/png', data: baseBuf.toString('base64') } },
            { inlineData: { mimeType: 'image/jpeg', data: small.toString('base64') } },
          ],
        }],
      }),
    });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Gemini ${res.status}: ${t.slice(0, 200)}`);
  }
  const json = await res.json();
  const parts = json?.candidates?.[0]?.content?.parts || [];
  const img = parts.find(p => p.inlineData && p.inlineData.data);
  if (!img) throw new Error('Gemini 응답에 이미지가 없습니다');
  return Buffer.from(img.inlineData.data, 'base64');
}

/**
 * 3단 폴백. 고객이 빈손으로 돌아가지 않는 것이 이 함수의 계약이다.
 *   GPT → 제미나이 → 실루엣(베이스 그대로)
 * 어느 단계에서 끝났는지는 호출부가 기록해 나중에 성공률을 본다.
 */
// ── GPT 장면 생성 (테마 · 칩 · 메이트 · 사진 분석 → 한 장) ────────────────────
// 시드 이미지를 그대로 내보내던 방식 대신 응모마다 새로 그린다. 프롬프트는 restPrompts.js.
const RP = require('./restPrompts');
const GEN_MODE = process.env.REST_MOMENT_GEN || 'gpt';                 // 'gpt' | 'base'  (base = 시드 + 인물 레이어 옛 경로)
const OPENAI_QUALITY = process.env.OPENAI_IMAGE_QUALITY || 'high';      // 건당 ≈ $0.18 (medium ≈ $0.05)
const OPENAI_VISION = process.env.OPENAI_VISION_MODEL || 'gpt-4.1-mini'; // 사진 분석 · 태그 위치 탐지
const ASSET_DIR = path.join(__dirname, 'public', 'rest-moment');
function loadAsset(name) { const p = path.join(ASSET_DIR, name); return fs.existsSync(p) ? fs.readFileSync(p) : null; }
const dataUrl = (buf, mime = 'image/jpeg') => `data:${mime};base64,${buf.toString('base64')}`;

/** 비전 모델에 이미지+지시를 보내 JSON 만 받는다. */
async function openaiJson(parts, { maxTokens = 400 } = {}) {
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
  if (choice && choice.finish_reason === 'length') throw new Error(`vision 응답 잘림(max_tokens ${maxTokens})`);
  const text = (choice && choice.message && choice.message.content) || '{}';
  try { return JSON.parse(text); } catch { throw new Error('vision JSON 파싱 실패: ' + String(text).slice(0, 80)); }
}

/** 1단계 — 고객 사진 분석. 축소본만 보내고 결과는 이 요청 안에서만 쓴다. */
async function analyzePhoto(photo) {
  const small = await sharp(photo.buffer).rotate().resize({ width: 768, height: 768, fit: 'inside' }).jpeg({ quality: 82 }).toBuffer();
  return openaiJson([{ type: 'text', text: RP.PHOTO_ANALYSIS_PROMPT }, { type: 'image_url', image_url: { url: dataUrl(small), detail: 'low' } }]);
}

/** 3단계 — 생성본에서 무지 태그 위치를 찾는다. */
async function locateTag(buf) {
  const small = await sharp(buf).resize({ width: 768 }).jpeg({ quality: 82 }).toBuffer();
  return openaiJson([{ type: 'text', text: RP.TAG_LOCATE_PROMPT }, { type: 'image_url', image_url: { url: dataUrl(small), detail: 'high' } }], { maxTokens: 120 });
}

/** 태그 자리에 진짜 로고를 얹는다 (imgCreate stamp-logo 방식). 못 찾으면 무지 태그 그대로 둔다. */
/** #RRGGBB 의 상대 밝기(0~255). 밝은 제품이면 태그 대비 가드를 낮춘다. */
function hexLum(hex) {
  const m = /^#?([0-9a-f]{6})$/i.exec(String(hex || ''));
  if (!m) return 0;
  const v = parseInt(m[1], 16);
  return 0.2126 * (v >> 16 & 255) + 0.7152 * (v >> 8 & 255) + 0.0722 * (v & 255);
}

async function stampLogo(buf, box, opts = {}) {
  const logo = loadAsset('logo.png');
  const found = box && (box.found === true || box.found === 'true');
  if (!logo || !found) return { buf, stamped: false };
  for (const k of ['cx', 'cy']) { const v = Number(box[k]); if (!(v > 0 && v < 1)) return { buf, stamped: false }; }
  const meta = await sharp(buf).metadata();
  const W = meta.width, H = meta.height;
  const cx = Math.round(W * Number(box.cx)), cy = Math.round(H * Number(box.cy));
  if (!(cx > 0 && cy > 0 && cx < W && cy < H)) return { buf, stamped: false };
  // 비전 모델이 옷·소품을 태그로 착각하는 일이 있다(실측: 치마 위에 찍힘). 좌표 주변에서 가장 밝은 창을 찾아
  // 재중심하고, 그 창이 밝은 저채도(무지 태그의 천)이며 둘레 원단보다 뚜렷이 밝을 때만 얹는다.
  // 로고가 엉뚱한 데 붙는 것보다 무지 태그가 낫다.
  const rawImg = await sharp(buf).removeAlpha().raw().toBuffer();
  const px = (x, y) => { x = Math.max(0, Math.min(W - 1, x)); y = Math.max(0, Math.min(H - 1, y)); const i = (y * W + x) * 3; return [rawImg[i], rawImg[i + 1], rawImg[i + 2]]; };
  const win = (x, y, r) => { let l = 0, sat = 0, c = 0; for (let dy = -r; dy <= r; dy++) for (let dx = -r; dx <= r; dx++) { const [R, G, B] = px(x + dx, y + dy); l += (R + G + B) / 3; sat += Math.max(R, G, B) - Math.min(R, G, B); c++; } return { lum: l / c, sat: sat / c }; };
  let best = { lum: -1 }, bx = cx, by = cy;
  for (let dy = -8; dy <= 8; dy += 2) for (let dx = -8; dx <= 8; dx += 2) { const w = win(cx + dx, cy + dy, 2); if (w.lum > best.lum) { best = w; bx = cx + dx; by = cy + dy; } }
  const ringR = Math.max(10, Math.round(Math.max(Number(box.w) * W || 0, Number(box.h) * H || 0) * 0.9));
  let ring = 0; for (let k = 0; k < 8; k++) { const a = (k * Math.PI) / 4; ring += win(Math.round(bx + Math.cos(a) * ringR), Math.round(by + Math.sin(a) * ringR), 1).lum; } ring /= 8;
  // 채도 상한은 110 — 무지 태그가 순백이 아니라 따뜻한 크림색으로 그려진다(실측 sat 86). 진짜 판별은 둘레 대비가 한다.
  // 단, 라이트그레이처럼 밝은 제품은 태그와 원단 밝기가 거의 같다(실측: 대비 10 안팎) — 그때는 대비 요구를 8 로 낮춘다.
  const minContrast = opts.lightProduct ? 8 : 40;
  if (best.lum < 170 || best.sat > 110 || best.lum - ring < minContrast) {
    console.warn(`[쉼순간] 태그 좌표가 무지 태그로 보이지 않음(lum ${best.lum.toFixed(0)}, sat ${best.sat.toFixed(0)}, 둘레 ${ring.toFixed(0)}) → 로고 생략`);
    return { buf, stamped: false };
  }
  // 태그의 밝은 면 전체를 모아 무게중심과 긴 축을 낸다. 가장 밝은 한 점만 쓰면 로고가 태그 구석에 박힌다(실측).
  const seedW = Math.max(8, Math.round((Number(box.w) || 0.03) * W));
  const seedH = Math.max(8, Math.round((Number(box.h) || 0.03) * H));
  const scanR = Math.max(10, Math.round(Math.max(seedW, seedH) * 0.8));
  // 임계는 둘레 밝기와 태그 밝기의 중간 — 어두운 원단(둘레 72·태그 212 → 142)이든 밝은 원단(225·245 → 235)이든 원단은 빠지고 태그만 남는다.
  const thr = Math.max(150, ring + (best.lum - ring) * 0.5);
  const pts = [];
  for (let y = Math.max(0, by - scanR); y <= Math.min(H - 1, by + scanR); y++)
    for (let x = Math.max(0, bx - scanR); x <= Math.min(W - 1, bx + scanR); x++) {
      const [R, G, B] = px(x, y);
      if ((R + G + B) / 3 >= thr && Math.max(R, G, B) - Math.min(R, G, B) <= 90) pts.push(x, y);
    }
  let cxx = bx, cyy = by, tagW = seedW, tagH = seedH, major = Number(box.angle) || 0;
  if (pts.length >= 60) {                                   // 픽셀 30개 (x,y 쌍으로 담는다)
    const cnt = pts.length / 2;
    let mx = 0, my = 0;
    for (let i = 0; i < pts.length; i += 2) { mx += pts[i]; my += pts[i + 1]; }
    mx /= cnt; my /= cnt;
    let sxx = 0, syy = 0, sxy = 0, x0 = Infinity, x1 = -Infinity, y0 = Infinity, y1 = -Infinity;
    for (let i = 0; i < pts.length; i += 2) {
      const dx = pts[i] - mx, dy = pts[i + 1] - my;
      sxx += dx * dx; syy += dy * dy; sxy += dx * dy;
      if (pts[i] < x0) x0 = pts[i]; if (pts[i] > x1) x1 = pts[i];
      if (pts[i + 1] < y0) y0 = pts[i + 1]; if (pts[i + 1] > y1) y1 = pts[i + 1];
    }
    const bw = x1 - x0 + 1, bh = y1 - y0 + 1;
    if (bw > seedW * 2.5 || bh > seedH * 2.5) {
      // 덩어리가 비전 박스보다 터무니없이 크면 원단을 삼킨 것 — 박스 값을 믿고 무게중심만 버린다
      console.warn(`[쉼순간] 태그 덩어리가 박스보다 큼(${bw}x${bh} vs ${seedW}x${seedH}) → 박스 값 사용`);
    } else {
      cxx = Math.round(mx); cyy = Math.round(my);
      tagW = bw; tagH = bh;
      major = 0.5 * Math.atan2(2 * sxy / cnt, (sxx - syy) / cnt) * 180 / Math.PI;
    }
  }
  // 실물 태그처럼 워드마크가 태그의 긴 축을 따라 세로로 들어간다 (아래→위로 읽힘).
  // 긴 축 각도를 (-90, 90] 로 정리한 뒤, 세로 태그(|각|>45)는 항상 아래→위로 읽히도록 방향을 맞춘다.
  // 가로로 그려진 드문 태그(|각|≤45)는 그대로 가로.
  let angle = major;
  while (angle > 90) angle -= 180;
  while (angle < -90) angle += 180;
  if (Math.abs(angle) > 45 && angle > 0) angle -= 180;   // 위→아래로 읽히는 쪽이면 뒤집어 아래→위로
  // 회전한 로고의 외접 사각형이 태그 안에 들어가는 최대 가로폭 (예각으로 계산)
  const lmeta = await sharp(logo).metadata();
  const logoAr = (lmeta.height || 160) / (lmeta.width || 400);
  const acute = Math.min(Math.abs(angle) % 180, 180 - (Math.abs(angle) % 180));
  const rr = acute * Math.PI / 180;
  const capW = (tagW * 0.75) / (Math.cos(rr) + logoAr * Math.sin(rr));
  const capH = (tagH * 0.75) / (Math.sin(rr) + logoAr * Math.cos(rr));
  const logoW = Math.max(10, Math.round(Math.min(capW, capH, W * 0.08)));
  const l = await sharp(logo).resize({ width: logoW }).ensureAlpha().png().toBuffer();
  const faded = await sharp(l).composite([{ input: Buffer.from([0, 0, 0, 235]), raw: { width: 1, height: 1, channels: 4 }, tile: true, blend: 'dest-in' }]).png().toBuffer();
  const rot = await sharp(faded).rotate(angle, { background: { r: 0, g: 0, b: 0, alpha: 0 } }).png().toBuffer();
  const rm = await sharp(rot).metadata();
  // 태그가 가장자리에 있으면 좌표가 화면 밖으로 나간다 — sharp 는 음수 left/top 에서 던진다
  const left = Math.max(0, Math.min(W - rm.width, cxx - Math.round(rm.width / 2)));
  const top = Math.max(0, Math.min(H - rm.height, cyy - Math.round(rm.height / 2)));
  const out = await sharp(buf).composite([{ input: rot, left, top }]).png().toBuffer();
  return { buf: out, stamped: true };
}

/** 1024x1536 → 4:5(OUT_W x OUT_H). 한복은 인사말이 위에 있으니 위쪽 기준으로 자른다. */
function cropPoster(buf, theme) {
  return sharp(buf).resize(OUT_W, OUT_H, { fit: 'cover', position: theme === 'hanbok' ? 'top' : 'centre' }).png().toBuffer();
}

/**
 * 응모 한 건의 장면을 그린다.
 * 참조: 제품(칩의 시드 일러스트 — 형태·색), 메이트 팍스, (있으면) 고객 사진.
 * 14세 미만이 보이면 사진을 쓰지 않고 minorFlag 만 남긴다.
 */
async function generateScene(doc, chip, base, photo) {
  const key = process.env.OPENAI_API_KEY;
  if (!key) throw new Error('OPENAI_API_KEY 없음');
  const theme = doc.theme === 'hanbok' ? 'hanbok' : 'interior';

  let analysis = null, minorFlag = false, usePhoto = !!(photo && photo.buffer);
  if (usePhoto) {
    try { analysis = await analyzePhoto(photo); }
    catch (e) { console.warn('[쉼순간] 사진 분석 실패 → 사진 없이 진행:', e.message); analysis = null; usePhoto = false; }
    const minor = analysis && (analysis.minorPresent === true || analysis.minorPresent === 'true');
    if (minor) { minorFlag = true; usePhoto = false; analysis = null; }
    if (analysis && !(Number(analysis.count) > 0)) { usePhoto = false; analysis = null; }
    // 사진을 안 쓰기로 했으면 여기서 파기한다 — 실패해서 옛 경로로 내려가도 이 사진이 다른 모델에 올라가지 않게.
    if (!usePhoto && photo) { photo.buffer = null; photo.discarded = true; }
  }

  // 티렉스 메가메이트 레퍼런스가 있으면 3번 참조로 붙인다 (팍스 다음, 사진 앞) — 없으면 팍스만.
  const mate2 = loadAsset('ref-mate-trex.png') || loadAsset('ref-mate-trex.jpg');
  const refs = ['product', 'mate'].concat(mate2 ? ['mate2'] : []).concat(usePhoto ? ['photo'] : []);
  const { prompt, greeting, double, drawn, omitted } = RP.buildPrompt({ theme, chip: Object.assign({ key: doc.chip }, chip), analysis, refs });
  if (omitted > 0) console.warn(`[쉼순간] ${doc._id} 사진 인원 ${(analysis && analysis.count) || 0}명 중 ${drawn}명만 그립니다(상한 ${RP.MAX_PEOPLE}명)`);
  const mate = loadAsset('ref-mate-fox.png');
  if (!mate) throw new Error('ref-mate-fox.png 없음');
  // 제품 위 인원이 2명으로 정해졌으면 둘이 붙어 앉은 컷으로 바꿔 든다 (없으면 그대로).
  let productSrc = base;
  if (double) {
    const duo = posePath(doc.chip, String(doc._id), 2);
    if (duo && /-d\d+\.jpe?g$/i.test(duo)) {
      try { productSrc = fs.readFileSync(duo); console.log(`[쉼순간] ${doc.chip} 2인 포즈 컷 ${path.basename(duo)}`); }
      catch (e) { console.warn('[쉼순간] 2인 포즈 컷 읽기 실패 → 기본 컷:', e.message); }
    }
  }
  const productRef = await sharp(productSrc).resize({ width: 1024, height: 1024, fit: 'inside' }).png().toBuffer();

  const fd = new FormData();
  fd.append('model', OPENAI_MODEL);
  fd.append('prompt', prompt);
  fd.append('size', '1024x1536');
  fd.append('quality', OPENAI_QUALITY);
  fd.append('output_format', 'png');
  fd.append('n', '1');
  fd.append('image[]', new Blob([productRef], { type: 'image/png' }), 'product.png');
  fd.append('image[]', new Blob([mate], { type: 'image/png' }), 'mate.png');
  if (mate2) {
    const m2 = await sharp(mate2).resize({ width: 1024, height: 1024, fit: 'inside', withoutEnlargement: true }).png().toBuffer();
    fd.append('image[]', new Blob([m2], { type: 'image/png' }), 'mate2.png');
  }
  if (usePhoto) {
    const ph = await sharp(photo.buffer).rotate().resize({ width: 1024, height: 1024, fit: 'inside' }).jpeg({ quality: 85 }).toBuffer();
    fd.append('image[]', new Blob([ph], { type: 'image/jpeg' }), 'person.jpg');
  }
  let res;
  try {
    res = await fetch('https://api.openai.com/v1/images/edits', { method: 'POST', headers: { Authorization: `Bearer ${key}` }, body: fd, signal: AbortSignal.timeout(180000) });
  } catch (e) {
    // 네트워크·타임아웃 — 과금되지 않았으니 잡 재시도 대상
    throw Object.assign(new Error('GPT 연결 실패: ' + (e && e.message)), { retryable: true });
  }
  if (!res.ok) {
    const t = await res.text();
    // 429·5xx 는 잠시 뒤 다시(과금 없음). 4xx(정책·요청 오류)는 다시 해도 같으니 실루엣으로 내려간다.
    throw Object.assign(new Error(`GPT ${res.status}: ${t.slice(0, 200)}`), { status: res.status, retryable: res.status === 429 || res.status >= 500 });
  }
  const json = await res.json();
  const b64 = json && json.data && json.data[0] && json.data[0].b64_json;
  if (!b64) throw Object.assign(new Error('GPT 응답에 이미지가 없습니다'), { retryable: true });

  let buf = await cropPoster(Buffer.from(b64, 'base64'), theme);
  let tagStamped = false;
  try {
    const box = await locateTag(buf);
    const r = await stampLogo(buf, box, { lightProduct: hexLum(chip && chip.hex) >= 180 });
    buf = r.buf; tagStamped = r.stamped;
  } catch (e) { console.warn('[쉼순간] 태그 탐지/로고 실패 → 무지 태그 유지:', e.message); }

  const usage = json.usage || {};
  return {
    buf, via: 'gpt-scene',
    extra: {
      theme, greeting, double, tagStamped, minorFlag, usedPhoto: usePhoto,
      // 인원: 사진에서 센 수 · 실제로 그린 수 · 상한에 걸려 뺀 수. 성별 표현 같은 파생 속성은 저장하지 않는다.
      people: analysis ? { count: Number(analysis.count) || 0, drawn, omitted } : null,
      genTokens: usage.output_tokens || null,
    },
  };
}

async function generatePersonLayer(baseBuf, photo, chip) {
  if (!photo || !photo.buffer) return { buf: baseBuf, via: 'silhouette' };
  try {
    return { buf: await generateWithGPT(baseBuf, photo, chip, null), via: 'gpt' };
  } catch (e1) {
    console.warn('[쉼순간] GPT 실패 →', e1.message);
    try {
      return { buf: await generateWithGemini(baseBuf, photo, chip), via: 'gemini' };
    } catch (e2) {
      console.warn('[쉼순간] 제미나이도 실패 →', e2.message);
      return { buf: baseBuf, via: 'silhouette' };
    }
  }
}

// ── 워커 ──────────────────────────────────────────────────────────
// 잡 상태를 메모리가 아니라 Mongo 에 둔다. 컨테이너가 재시작해도
// 폴링 중인 고객이 영영 대기 화면에 갇히지 않는다.
// 생성은 됐는데 발행(합성·업로드·저장)이 실패한 건을 다시 과금하지 않기 위한 임시 보관. 사진 vault 와 같은 TTL.
const sceneVault = new Map();   // String(_id) → { buf, via, extra, at }
setInterval(() => { const cut = Date.now() - PHOTO_TTL_MS; for (const [k, v] of sceneVault) if (v.at < cut) sceneVault.delete(k); }, 60 * 1000).unref();

// 워커 동시성 — 건당 60~120초라 직렬로는 큐가 밀린다. 클레임(pending→processing)이 원자적이라 중복 처리는 없다.
const CONCURRENCY = Math.max(1, Number(process.env.REST_MOMENT_CONCURRENCY || 3));
let inflight = 0;

async function processOne(db, doc) {
  const col = db.collection(ENTRY_COLLECTION);
  // 집을 때 processing 으로 바꾼다. 그 사이 관리자가 지웠으면 여기서 멈춘다 — 생성 비용도, 고아 파일도 없다.
  const claim = await col.updateOne({ _id: doc._id, status: 'pending' }, { $set: { status: 'processing', pickedAt: new Date() } });
  if (!claim.matchedCount) return;
  const chip = CHIPS[doc.chip];
  let photo = null;
  try {
    const base = await loadBase(doc.chip, String(doc._id));
    photo = doc.hadPhoto ? takePhoto(doc._id) : null;

    // 응모마다 GPT 로 장면을 새로 그린다 (테마 · 칩 · 메이트 · 사진 분석).
    // 이미 그려둔 게 있으면(발행만 실패했던 재시도) 다시 과금하지 않는다.
    let scene = null, via = null, extra = {};
    const cached = sceneVault.get(String(doc._id));
    if (cached) { scene = cached.buf; via = cached.via; extra = cached.extra || {}; }
    // 이벤트 상한 — 넘으면 GPT 를 부르지 않는다. 사진도 여기서 파기해 아래 옛 경로(인물 레이어)로 새지 않게.
    let capped = null;
    if (!scene && GEN_MODE === 'gpt' && (MAX_GEN_TOTAL > 0 || MAX_GEN_DAILY > 0)) {
      const b = await genBudget(col);
      if (!b.allowed) {
        capped = b.reason;
        if (photo) { photo.buffer = null; }
        extra = { theme: 'interior', genCapped: b.reason, usedPhoto: false };
        console.warn(`[쉼순간] 생성 상한 도달(${b.reason} · 전체 ${b.total}/${b.maxTotal || '∞'} · 오늘 ${b.today}/${b.maxDaily || '∞'}) → 과금 없이 실루엣으로 완성`);
      }
    }
    if (!scene && GEN_MODE === 'gpt' && !capped) {
      try { const r = await generateScene(doc, chip, base, photo); scene = r.buf; via = r.via; extra = r.extra; }
      catch (e) {
        // 일시 오류(429·5xx·네트워크)는 과금이 안 됐으니 잡 재시도로 넘긴다 — 사진은 vault 에 다시 넣어 둔다.
        if (e && e.retryable && (doc.tries || 0) < 2) {
          if (photo && photo.buffer) stashPhoto(doc._id, photo.buffer, photo.mimeType);
          throw Object.assign(e, { backoffMs: 20000 * ((doc.tries || 0) + 1) });
        }
        console.warn(`[쉼순간] GPT 장면 생성 실패 → 실루엣 경로: ${e.message}`);
        if (photo) { photo.buffer = null; }            // 같은 사진으로 다른 모델을 또 부르지 않는다
        extra = { theme: 'interior', genError: String(e && e.message || e).slice(0, 200) };
      }
    }
    if (!scene) { const r = await generatePersonLayer(base, photo && photo.buffer ? photo : null, chip); scene = r.buf; via = r.via; }

    // ★ 원본 사진 파기 — 생성에 넘긴 직후 여기서 끝난다.
    if (photo) { photo.buffer = null; photo = null; }
    sceneVault.set(String(doc._id), { buf: scene, via, extra, at: Date.now() });

    const [artBuf, shareBuf] = await Promise.all([
      renderArtwork(scene),
      renderShareCard(scene, doc.sentence, chip.type),
    ]);

    const [imageUrl, shareUrl] = await Promise.all([
      ftpUpload(artBuf, publicName('')),
      ftpUpload(shareBuf, publicName('_s')),
    ]);

    const done = await col.updateOne({ _id: doc._id }, {
      $set: Object.assign({ status: 'done', imageUrl, shareUrl, via, doneAt: nowKST() }, extra),
      $unset: { lastError: '' },
    });
    sceneVault.delete(String(doc._id));
    if (!done.matchedCount) {
      // 생성하는 동안 관리자가 지운 건 — 방금 올린 공개 파일 두 장을 되돌린다
      for (const u of [imageUrl, shareUrl]) {
        const r = await ftpRemoveByUrl(u);
        if (!r.ok) console.error('[쉼순간] 고아 파일 정리 실패:', u, r.error);
      }
      console.log(`[쉼순간] 생성 중 삭제된 응모 ${doc._id} — 파일 회수`);
      return;
    }
    console.log(`[쉼순간] 완성 ${doc._id} (${doc.chip}, ${via})`);
  } catch (err) {
    if (photo) { photo.buffer = null; photo = null; }   // 실패해도 원본은 남기지 않는다 (재시도 대상이면 위에서 이미 vault 에 되돌렸다)
    const tries = (doc.tries || 0) + 1;
    const lastError = String(err && err.message || err).slice(0, 300);
    if (tries >= 3) {
      // 재시도 소진 — 빈손으로 보내지 않는다: 시드(실루엣)로라도 발행한다. 그것마저 안 되면 failed.
      try {
        const base2 = await loadBase(doc.chip);
        const [artBuf, shareBuf] = await Promise.all([renderArtwork(base2), renderShareCard(base2, doc.sentence, chip.type)]);
        const [imageUrl, shareUrl] = await Promise.all([ftpUpload(artBuf, publicName('')), ftpUpload(shareBuf, publicName('_s'))]);
        await col.updateOne({ _id: doc._id }, { $set: { status: 'done', imageUrl, shareUrl, via: 'silhouette', theme: 'interior', usedPhoto: false, doneAt: nowKST(), tries, lastError } });
        sceneVault.delete(String(doc._id));
        console.error(`[쉼순간] 생성 실패 ${doc._id} (${tries}회) → 실루엣으로 발행:`, lastError);
        return;
      } catch (e2) { console.error('[쉼순간] 실루엣 발행도 실패:', e2.message); }
    }
    const backoff = err && err.backoffMs ? err.backoffMs : 3000 * tries;
    await col.updateOne({ _id: doc._id }, {
      $set: { status: tries >= 3 ? 'failed' : 'pending', tries, nextAt: new Date(Date.now() + backoff), lastError },
    });
    console.error(`[쉼순간] 생성 실패 ${doc._id} (${tries}회, ${Math.round(backoff / 1000)}초 뒤 재시도):`, lastError);
  }
}

async function tick(getDb) {
  if (inflight >= CONCURRENCY) return;
  try {
    const db = getDb();
    if (!db) return;
    const col = db.collection(ENTRY_COLLECTION);
    const free = CONCURRENCY - inflight;
    const now = new Date();
    const docs = await col.find({ status: 'pending', $or: [{ nextAt: { $exists: false } }, { nextAt: null }, { nextAt: { $lte: now } }] })
      .sort({ createdAt: 1 }).limit(free).toArray();
    for (const d of docs) {
      inflight++;
      processOne(db, d).catch(e => console.error('[쉼순간] 워커 오류:', e.message)).finally(() => { inflight--; });
    }
  } catch (e) {
    console.error('[쉼순간] 워커 오류:', e.message);
  }
}

// ── 라우트 ────────────────────────────────────────────────────────
function mount(app, deps) {
  const { getDb, apiRequest, mallId } = deps;

  bootstrapFont();

  // 재배포로 processing 에 멈춘 건을 되살린다 — 워커는 이 프로세스 하나뿐이라 부팅 시 processing 은 전부 고아다.
  setTimeout(async () => {
    try {
      const db = getDb(); if (!db) return;
      const r = await db.collection(ENTRY_COLLECTION).updateMany({ status: 'processing' }, { $set: { status: 'pending' }, $unset: { pickedAt: '' } });
      if (r && r.modifiedCount) console.log(`[쉼순간] 멈춘 processing ${r.modifiedCount}건 → pending`);
    } catch (e) { console.warn('[쉼순간] processing 복구 실패:', e.message); }
  }, 5000).unref();

  // 읽기와 쓰기를 다르게 막는다.
  //  · 읽기(갤러리·상태조회)는 공개 데이터라 어디서 불러도 상관없다.
  //    여기까지 조이면 스킨 미리보기·로컬 확인이 통째로 막혀 개발이 안 된다.
  //  · 쓰기(응모 접수·적립금)는 사진이 올라오고 돈이 나가므로 몰 도메인만 받는다.
  const allowRead = (req, res, next) => {
    const origin = req.headers.origin;
    if (origin) {
      res.setHeader('Access-Control-Allow-Origin', origin);
      res.setHeader('Vary', 'Origin');
    }
    next();
  };

  const allowWrite = (req, res, next) => {
    const origin = req.headers.origin;
    if (!originAllowed(origin)) {
      console.warn('[쉼순간] 차단된 오리진:', origin);
      return res.status(403).json({ ok: false, message: '허용되지 않은 요청입니다.' });
    }
    if (origin) {
      res.setHeader('Access-Control-Allow-Origin', origin);
      res.setHeader('Vary', 'Origin');
    }
    next();
  };

  // ★ 원본 사진은 메모리에서만 다룬다. 디스크에 쓰지 않는다.
  const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: MAX_PHOTO_BYTES, files: 1 },
    fileFilter: (_req, file, cb) => {
      if (!/^image\//.test(file.mimetype)) return cb(new Error('이미지 파일만 올릴 수 있습니다.'));
      cb(null, true);
    },
  }).single('photo');

  // ── 응모 접수 ──
  app.post('/api/rest-moment/entry', allowWrite, (req, res) => {
    upload(req, res, async (uploadErr) => {
      if (uploadErr) {
        return res.status(400).json({ ok: false, message: uploadErr.message || '사진을 읽지 못했습니다.' });
      }
      try {
        const db = getDb();
        const { chip, sentence, memberId, agreeMarketing } = req.body || {};
        // 예제에서 고른 스타일. 모르는 값은 기본(interior)으로 — 프론트가 없던 시절 응모도 그대로 통과한다.
        const THEMES = ['interior', 'hanbok'];
        const theme = THEMES.includes(String((req.body || {}).theme || '')) ? String(req.body.theme) : 'interior';

        if (!CHIPS[chip]) {
          return res.status(400).json({ ok: false, message: '쉬는 자세를 하나만 골라주세요.' });
        }
        const text = String(sentence || '').trim();
        if (!text) {
          return res.status(400).json({ ok: false, message: '한 문장을 남겨주세요.' });
        }
        if (text.length > MAX_SENTENCE) {
          return res.status(400).json({ ok: false, message: `문장은 ${MAX_SENTENCE}자까지 쓸 수 있습니다.` });
        }

        const now = nowKST();
        if (!withinEventPeriod(now)) {
          return res.status(400).json({ ok: false, message: '이벤트 기간이 아닙니다.' });
        }

        // 아이디당 최대 횟수. 실패한 건은 사용자 탓이 아니므로 세지 않는다.
        const mid = memberId ? String(memberId) : null;
        const master = isMaster(mid, req);
        if (mid && !master) {
          const used = await db.collection(ENTRY_COLLECTION).countDocuments({ memberId: mid, status: { $ne: 'failed' } });
          if (used >= MAX_PER_MEMBER) {
            return res.status(400).json({ ok: false, limitReached: true,
              message: '이 아이디로는 ' + MAX_PER_MEMBER + '번까지만 만들 수 있어요.' });
          }
        }

        const flagged = looksInappropriate(text);
        // 갤러리가 보여주는 정보만으로는 남의 응모를 가져갈 수 없게, 접수한 브라우저에만 토큰을 준다.
        const claimToken = crypto.randomBytes(16).toString('hex');
        const doc = {
          chip,
          theme,
          type: CHIPS[chip].type,
          sentence: text,
          memberId: mid,                                   // 비회원도 접수한다 (가입 시 지급)
          displayId: mid ? (master ? fakeDisplayId() : (looksInappropriate(mid) ? '회원***' : maskId(mid))) : null,
          claimHash: sha256(claimToken),                   // 비회원 응모를 나중에 가입 후 가져갈 때 본인 증명
          master,
          hadPhoto: !!(req.file && req.file.buffer),
          agreeMarketing: String(agreeMarketing) === '1',
          status: 'pending',
          tries: 0,
          approved: REQUIRE_REVIEW ? false : !flagged,      // 베타: 금칙어만 아니면 바로 공개. 검수 우선은 env 로.
          autoFlag: flagged,
          createdAt: now,
        };

        const { insertedId } = await db.collection(ENTRY_COLLECTION).insertOne(doc);

        // 세고 넣는 사이의 경합을 막는다. 넣은 뒤 자기 순번(_id 순)이 한도를 넘으면 자기 것만 지운다 —
        // 단순 재카운트로 지우면 동시 2건이 서로를 보고 둘 다 사라져 남은 자리도 못 쓴다.
        if (mid && !master) {
          const rank = await db.collection(ENTRY_COLLECTION)
            .countDocuments({ memberId: mid, status: { $ne: 'failed' }, _id: { $lte: insertedId } });
          if (rank > MAX_PER_MEMBER) {
            await db.collection(ENTRY_COLLECTION).deleteOne({ _id: insertedId });
            return res.status(400).json({ ok: false, limitReached: true,
              message: '이 아이디로는 ' + MAX_PER_MEMBER + '번까지만 만들 수 있어요.' });
          }
        }

        // ★ 원본 사진은 Mongo 에도 디스크에도 쓰지 않는다.
        //   생성이 20~40초라 접수와 분리해야 해서, 프로세스 메모리에만 잠깐 둔다.
        //   워커가 꺼내 쓰는 즉시 사라지고, 10분이 지나면 자동으로 지워진다.
        if (req.file && req.file.buffer) {
          stashPhoto(insertedId, req.file.buffer, req.file.mimetype);
          req.file.buffer = null;
          req.file = null;
        }

        return res.json({ ok: true, entryId: String(insertedId), jobId: String(insertedId), claimToken });
      } catch (err) {
        if (req.file) { req.file.buffer = null; req.file = null; }
        console.error('[쉼순간] 접수 오류:', err.message);
        return res.status(500).json({ ok: false, message: '잠시 후 다시 시도해주세요.' });
      }
    });
  });

  // ── 생성 상태 폴링 ──
  app.get('/api/rest-moment/job/:jobId', allowRead, async (req, res) => {
    try {
      const db = getDb();
      const { ObjectId } = require('mongodb');
      let _id;
      try { _id = new ObjectId(req.params.jobId); }
      catch { return res.status(400).json({ status: 'failed', message: '잘못된 요청입니다.' }); }

      const doc = await db.collection(ENTRY_COLLECTION).findOne({ _id });
      if (!doc) return res.status(404).json({ status: 'failed', message: '응모를 찾을 수 없습니다.' });

      return res.json({
        status: doc.status,
        reason: doc.status === 'failed' ? String(doc.lastError || '').slice(0, 160) : undefined,
        tries: doc.tries || 0,
        chip: doc.chip,
        theme: doc.theme || 'interior',
        date: doc.doneAt ? ymd(doc.doneAt) : null,        // 완료 화면 "2026. 09. 11 · 기대는 사람"
        imageUrl: doc.imageUrl || null,
        shareUrl: doc.shareUrl || null,
        type: doc.type,
        sentence: doc.sentence,
        rewarded: !!doc.rewarded,
      });
    } catch (err) {
      console.error('[쉼순간] 상태 조회 오류:', err.message);
      return res.status(500).json({ status: 'failed' });
    }
  });

  // ── 갤러리 · 미리보기 (검수 통과분만) ──
  app.get('/api/rest-moment/recent', allowRead, async (req, res) => {
    try {
      const db = getDb();
      const limit = Math.min(Math.max(Number(req.query.limit) || 8, 1), 48);
      const offset = Math.min(Math.max(Number(req.query.offset) || 0, 0), 5000);
      const col = db.collection(ENTRY_COLLECTION);

      // 마스터 아이디(testid·yogibo)가 ?as=<id>&mk=<key>&all=1 로 부르면 숨김 건까지 보여주고 id 를 준다 (페이지 내 숨김·삭제용)
      const master = isMasterStrict(String(req.query.as || ''), req);   // 키(헤더)가 있어야 열린다
      const filter = { status: 'done', imageUrl: { $ne: null } };
      if (!(master && String(req.query.all) === '1')) filter.approved = true;
      // 실제로 사진이 쓰였는지(usedPhoto)가 있으면 그걸로, 없으면(옛 문서) 첨부 여부(hadPhoto)로 가른다
      if (req.query.tab === 'photo') filter.$or = [{ usedPhoto: true }, { usedPhoto: { $exists: false }, hadPhoto: true }];
      if (req.query.tab === 'ai')    filter.$or = [{ usedPhoto: false }, { usedPhoto: { $exists: false }, hadPhoto: false }];

      // 갤러리 머리의 "오늘 하루에만 N개" — 공개된 완성건 기준, KST 자정 이후
      const k = nowKST();
      const dayStart = new Date(k.getFullYear(), k.getMonth(), k.getDate());
      // limit+1 로 한 장 더 읽어 다음 페이지가 있는지 알아낸다 — count 한 번을 아낀다.
      const [rows, total, shown, today] = await Promise.all([
        col.find(filter).sort({ doneAt: -1 }).skip(offset).limit(limit + 1).toArray(),
        col.countDocuments({ status: 'done' }),
        col.countDocuments(filter),
        col.countDocuments({ status: 'done', approved: true, imageUrl: { $ne: null }, doneAt: { $gte: dayStart } }),
      ]);
      const hasMore = rows.length > limit;
      const items = hasMore ? rows.slice(0, limit) : rows;

      return res.json({
        ok: true,
        total,
        shown,
        today,
        offset,
        hasMore,
        master,
        items: items.map(d => ({
          ...(master ? { id: String(d._id), approved: !!d.approved, rewarded: !!d.rewarded } : {}),
          date: d.doneAt ? ymd(d.doneAt) : null,
          theme: d.theme || 'interior',
          src: (d.usedPhoto === true || d.usedPhoto === false) ? (d.usedPhoto ? 'photo' : 'ai') : (d.hadPhoto ? 'photo' : 'ai'),
          caption: d.sentence,
          type: d.type,
          imageUrl: d.imageUrl,
          displayId: d.displayId || null,
        })),
      });
    } catch (err) {
      console.error('[쉼순간] 갤러리 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  // ── 적립금 지급 ──
  // 카트 이벤트에서 운영 검증된 흐름 그대로. 다만 memberId 만으로 주지 않고
  // "그 회원의 완성된 응모"가 실제로 있는지 확인한다 — 없으면 응모 없이도
  // API 만 때려서 받아갈 수 있다.

  /** 응모 한 건을 파일까지 지운다. 관리 페이지와 마스터 아이디의 페이지 내 삭제가 같이 쓴다. */
  async function deleteEntry(db, _id, by) {
    const col = db.collection(ENTRY_COLLECTION);
    // 문서를 먼저 원자적으로 빼낸다 — 워커의 done 갱신과 겹쳐도 URL 을 잃지 않는다 (드라이버 6: 문서를 직접 돌려준다).
    const doc = await col.findOneAndDelete({ _id });
    if (!doc) return null;
    photoVault.delete(String(_id));
    sceneVault.delete(String(_id));
    // 휴지통 기록을 파일 삭제보다 먼저 쓴다 — FTP 도중 죽어도 재시도 근거가 남는다.
    const trash = db.collection(TRASH_COLLECTION);
    const { insertedId: trashId } = await trash.insertOne({
      entryId: String(_id), memberId: doc.memberId || null, sentence: doc.sentence, status: doc.status,
      imageUrl: doc.imageUrl || null, shareUrl: doc.shareUrl || null, deletedBy: by || 'admin',
      deletedAt: new Date(), ftpRemoved: null, errors: [],
    });
    const results = [];
    for (const u of [doc.imageUrl, doc.shareUrl]) if (u) results.push(Object.assign({ url: u }, await ftpRemoveByUrl(u)));
    const ftpRemoved = results.every(x => x.ok);
    await trash.updateOne({ _id: trashId }, { $set: { ftpRemoved, errors: results.filter(x => !x.ok).map(x => x.url + ' — ' + x.error) } });
    console.log('[쉼순간] 삭제(' + (by || 'admin') + '):', String(_id), ftpRemoved ? '파일 삭제됨' : '파일 삭제 실패');
    return { ftpRemoved };
  }

  // ── 마스터 아이디(testid·yogibo) 페이지 내 숨김·삭제 ──
  // memberId 는 클라이언트 값이라 REST_MOMENT_MASTER_KEY 가 있으면 masterKey 도 맞아야 한다 (isMaster 와 같은 규칙).
  app.post('/api/rest-moment/moderate', allowWrite, async (req, res) => {
    try {
      const db = getDb();
      const { memberId, entryId, action } = req.body || {};
      if (!MASTER_KEY) return res.status(403).json({ ok: false, notConfigured: true, message: 'REST_MOMENT_MASTER_KEY 가 서버에 설정되지 않았습니다.' });
      if (!isMasterStrict(memberId, req)) return res.status(403).json({ ok: false, message: '권한이 없습니다.' });
      const _id = oid(entryId);
      if (!_id) return res.status(400).json({ ok: false, message: '잘못된 id' });
      const col = db.collection(ENTRY_COLLECTION);
      if (action === 'hide' || action === 'unhide') {
        const r = await col.updateOne({ _id }, { $set: { approved: action === 'unhide', reviewedAt: new Date(), reviewedBy: String(memberId) } });
        if (!r.matchedCount) return res.status(404).json({ ok: false, message: '기록이 없습니다.' });
      } else if (action === 'delete') {
        const r = await deleteEntry(db, _id, 'master:' + String(memberId));
        if (!r) return res.status(404).json({ ok: false, message: '기록이 없습니다.' });
      } else {
        return res.status(400).json({ ok: false, message: '알 수 없는 action' });
      }
      console.log('[쉼순간] 마스터 조치:', String(memberId), action, String(_id));
      return res.json({ ok: true });
    } catch (err) {
      console.error('[쉼순간] 마스터 조치 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  // ── 관리 API (/rest-admin.html) ──
  // 같은 서버가 서빙하는 페이지에서만 부른다. 키는 헤더 x-rest-admin-key.
  const allowAdmin = (req, res, next) => {
    // 503 은 클라우드타입 게이트웨이가 자기 에러 페이지로 바꿔치기한다 — 앱 상태는 4xx JSON 으로 알린다.
    if (!ADMIN_KEY) return res.status(403).json({ ok: false, notConfigured: true, message: 'REST_MOMENT_ADMIN_KEY 가 서버에 설정되지 않았습니다.' });
    if (!adminKeyOk(req.get('x-rest-admin-key'))) return res.status(401).json({ ok: false, message: '관리 키가 맞지 않습니다.' });
    next();
  };
  const oid = v => { try { return new (require('mongodb').ObjectId)(String(v)); } catch { return null; } };
  const STALE_MS = 2 * 60 * 1000;
  // "정산 필요" 의 정의를 한 곳에 둔다. 통계·목록·release 가 전부 이걸 써야 숫자와 버튼이 어긋나지 않는다.
  const problemClauses = staleAt => ({
    unknown:  { settled: 'unknown' },
    calling:  { settled: false, phase: 'calling',  reservedAt: { $lt: staleAt } },
    reserved: { settled: false, phase: 'reserved', reservedAt: { $lt: staleAt } },
  });
  const problemOr = staleAt => { const p = problemClauses(staleAt); return [p.unknown, p.calling, p.reserved]; };

  app.get('/api/rest-moment/admin/stats', allowAdmin, async (req, res) => {
    try {
      const db = getDb();
      const e = db.collection(ENTRY_COLLECTION), r = db.collection(REWARD_COLLECTION);
      const c = (col, f) => col.countDocuments(f);
      const staleAt = new Date(Date.now() - STALE_MS);
      const [total, pending, processing, done, failed, approved, review, flagged, withPhoto, master, members,
             settled, unknown, calling, reserved] = await Promise.all([
        c(e, {}), c(e, { status: 'pending' }), c(e, { status: 'processing' }), c(e, { status: 'done' }), c(e, { status: 'failed' }),
        c(e, { status: 'done', approved: true }), c(e, { status: 'done', approved: false }), c(e, { autoFlag: true }),
        c(e, { hadPhoto: true }), c(e, { master: true }), e.distinct('memberId', { memberId: { $ne: null } }),
        c(r, { $or: [{ settled: true }, { settled: { $exists: false } }] }), c(r, { settled: 'unknown' }),
        c(r, problemClauses(staleAt).calling), c(r, problemClauses(staleAt).reserved),
      ]);
      return res.json({
        ok: true,
        entries: { total, pending, processing, done, failed, approved, review, flagged, withPhoto, master },
        members: members.length,
        rewards: { settled, unknown, calling, staleReserved: reserved, problem: unknown + calling + reserved, points: settled * POINT_AMOUNT },
        config: { requireReview: REQUIRE_REVIEW, maxPerMember: MAX_PER_MEMBER, masterIds: MASTER_IDS, masterKeySet: !!MASTER_KEY, pointAmount: POINT_AMOUNT, eventEnd: EVENT_END, maxGen: MAX_GEN_TOTAL, dailyGen: MAX_GEN_DAILY },
        gen: await genBudget(e),            // { total, today, inflight, allowed, reason, maxTotal, maxDaily }
      });
    } catch (err) {
      console.error('[쉼순간] 관리 통계 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  app.get('/api/rest-moment/admin/entries', allowAdmin, async (req, res) => {
    try {
      const db = getDb();
      const col = db.collection(ENTRY_COLLECTION);
      const limit = Math.min(Math.max(Number(req.query.limit) || 30, 1), 100);
      const offset = Math.min(Math.max(Number(req.query.offset) || 0, 0), 20000);
      const VIEWS = {
        all:      {},
        review:   { status: 'done', approved: false },
        approved: { status: 'done', approved: true },
        flagged:  { autoFlag: true },
        failed:   { status: 'failed' },
        working:  { status: { $in: ['pending', 'processing'] } },
      };
      const view = String(req.query.view || 'all');
      const filter = Object.assign({}, Object.prototype.hasOwnProperty.call(VIEWS, view) ? VIEWS[view] : VIEWS.all);
      const qtext = String(req.query.q || '').trim().slice(0, 60);
      if (qtext) {
        const re = new RegExp(qtext.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
        filter.$or = [{ memberId: qtext }, { memberId: re }, { sentence: re }, { displayId: re }];
      }
      const rows = await col.find(filter).sort({ createdAt: -1 }).skip(offset).limit(limit + 1).toArray();
      const hasMore = rows.length > limit;
      const items = (hasMore ? rows.slice(0, limit) : rows).map(d => ({
        id: String(d._id), memberId: d.memberId || null, displayId: d.displayId || null, master: !!d.master,
        chip: d.chip, theme: d.theme || 'interior', type: d.type, sentence: d.sentence, status: d.status, approved: !!d.approved, autoFlag: !!d.autoFlag,
        hadPhoto: !!d.hadPhoto, imageUrl: d.imageUrl || null, rewarded: !!d.rewarded, via: d.via || null,
        tries: d.tries || 0, lastError: d.lastError || null, createdAt: d.createdAt || null, doneAt: d.doneAt || null,
      }));
      return res.json({ ok: true, view, offset, hasMore, items });
    } catch (err) {
      console.error('[쉼순간] 관리 목록 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  app.post('/api/rest-moment/admin/entries/:id', allowAdmin, async (req, res) => {
    try {
      const db = getDb();
      const col = db.collection(ENTRY_COLLECTION);
      const _id = oid(req.params.id);
      if (!_id) return res.status(400).json({ ok: false, message: '잘못된 id' });
      const action = String((req.body || {}).action || '');
      if (action === 'approve' || action === 'unapprove') {
        await col.updateOne({ _id }, { $set: { approved: action === 'approve', reviewedAt: new Date(), reviewedBy: 'admin' } });
      } else if (action === 'delete') {
        const r = await deleteEntry(db, _id, 'admin');
        if (!r) return res.status(404).json({ ok: false, message: '기록이 없습니다.' });
        return res.json({ ok: true, ftpRemoved: r.ftpRemoved, warn: r.ftpRemoved ? null : '이미지 파일 삭제에 실패했습니다. 기록은 지웠고 URL 은 휴지통(restMomentTrash)에 남겼습니다.' });
      } else {
        return res.status(400).json({ ok: false, message: '알 수 없는 action' });
      }
      console.log('[쉼순간] 관리:', action, String(_id));
      return res.json({ ok: true });
    } catch (err) {
      console.error('[쉼순간] 관리 변경 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  app.get('/api/rest-moment/admin/rewards', allowAdmin, async (req, res) => {
    try {
      const db = getDb();
      const r = db.collection(REWARD_COLLECTION);
      const staleAt = new Date(Date.now() - STALE_MS);
      const state = String(req.query.state || 'problem');
      const filter = state === 'all' ? {} : { $or: problemOr(staleAt) };
      const limit = Math.min(Math.max(Number(req.query.limit) || 50, 1), 200);
      const offset = Math.min(Math.max(Number(req.query.offset) || 0, 0), 20000);
      const rowsAll = await r.find(filter).sort({ reservedAt: -1, participatedAt: -1 }).skip(offset).limit(limit + 1).toArray();
      const hasMore = rowsAll.length > limit;
      const rows = hasMore ? rowsAll.slice(0, limit) : rowsAll;
      const canRelease = d => d.settled === 'unknown' || (d.settled === false && d.reservedAt && new Date(d.reservedAt) < staleAt);
      return res.json({ ok: true, state, offset, hasMore, items: rows.map(d => ({
        canRelease: canRelease(d),
        id: String(d._id), memberId: d.memberId, entryId: d.entryId, amount: d.amount,
        settled: d.settled === undefined ? true : d.settled, phase: d.phase || (d.settled === undefined ? 'settled' : null),
        reservedAt: d.reservedAt || null, settledAt: d.settledAt || null, failedAt: d.failedAt || null,
        lastError: d.lastError || null, participatedAt: d.participatedAt || null, settledBy: d.settledBy || null,
      })) });
    } catch (err) {
      console.error('[쉼순간] 관리 적립 목록 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  // settle: Cafe24 어드민에서 실제 적립을 확인한 뒤 "지급됨"으로 확정한다.
  // release: 적립이 안 된 것을 확인한 뒤 예약을 지운다 — 회원이 버튼을 다시 눌러 받을 수 있게 된다.
  app.post('/api/rest-moment/admin/rewards/:id', allowAdmin, async (req, res) => {
    try {
      const db = getDb();
      const r = db.collection(REWARD_COLLECTION), e = db.collection(ENTRY_COLLECTION);
      const _id = oid(req.params.id);
      if (!_id) return res.status(400).json({ ok: false, message: '잘못된 id' });
      const doc = await r.findOne({ _id });
      if (!doc) return res.status(404).json({ ok: false, message: '기록이 없습니다.' });
      const action = String((req.body || {}).action || '');
      if (action === 'settle') {
        await r.updateOne({ _id }, { $set: { settled: true, phase: 'settled', settledAt: new Date(), settledBy: 'admin' } });
        const eid = oid(doc.entryId);
        if (eid) await e.updateOne({ _id: eid }, { $set: { rewarded: true } });
      } else if (action === 'release') {
        // 조건부 원자 삭제 — 진행 중(2분 안 된 예약/호출)이거나 확정된 기록은 절대 지우지 않는다.
        const staleAt = new Date(Date.now() - STALE_MS);
        const { deletedCount } = await r.deleteOne({ _id, $or: [ { settled: 'unknown' }, { settled: false, reservedAt: { $lt: staleAt } } ] });
        if (!deletedCount) {
          return res.status(400).json({ ok: false, message: '진행 중이거나 이미 확정된 기록은 되돌릴 수 없습니다. 2분 뒤 다시 확인해주세요.' });
        }
      } else {
        return res.status(400).json({ ok: false, message: '알 수 없는 action' });
      }
      console.log('[쉼순간] 관리 적립:', action, doc.memberId, String(_id));
      return res.json({ ok: true });
    } catch (err) {
      console.error('[쉼순간] 관리 적립 변경 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  app.post('/api/rest-moment/reward', allowWrite, async (req, res) => {
    const { memberId, entryId, claimToken } = req.body || {};
    if (!memberId || typeof memberId !== 'string' || memberId.startsWith('guest_')) {
      return res.status(400).json({ ok: false, message: '로그인 후 받을 수 있습니다.' });
    }

    try {
      const db = getDb();
      const { ObjectId } = require('mongodb');
      const entries = db.collection(ENTRY_COLLECTION);
      const rewards = db.collection(REWARD_COLLECTION);

      const PENDING_MSG = '적립 처리 결과를 확인하고 있어요. 잠시 후 마이페이지에서 확인해주세요.';
      // 미확정 예약에는 절대 "이미 받았다"고 답하지 않는다 — 그 문구로 버튼이 잠기면 지급 전에 잠기는 셈이다.
      // 옛 코드는 지급 성공 뒤에만 기록을 남겼고 settled 필드가 없다 — 그 기록은 지급 완료로 본다.
      const isSettled = doc => doc.settled === true || doc.settled === undefined;
      const answerFor = doc => isSettled(doc)
        ? res.status(400).json({ ok: false, alreadyDone: true, message: '이미 적립금을 받으셨습니다.' })
        : res.status(202).json({ ok: false, pending: true, message: PENDING_MSG });

      // 1) 기존 예약
      const already = await rewards.findOne({ memberId });
      if (already) {
        // API 를 부르기 전 단계(reserved)에서 2분 넘게 멈춘 것만 되돌린다.
        // calling 에서 멈춘 것은 Cafe24 가 처리했을 수 있으므로 자동으로 지우지 않는다 — 운영에서 정산한다.
        const age = Date.now() - new Date(already.reservedAt || 0).getTime();
        const safeToReset = already.settled === false && already.phase === 'reserved' && age > 2 * 60 * 1000;
        if (!safeToReset) return answerFor(already);
        await rewards.deleteOne({ _id: already._id, phase: 'reserved' });
      }

      // 2) 지급 자격 — 완성된 응모가 있어야 한다
      let entry = null;
      if (entryId) {
        try { entry = await entries.findOne({ _id: new ObjectId(entryId) }); } catch { /* 무시 */ }
      }
      if (!entry) entry = await entries.findOne({ memberId, status: 'done' });

      if (!entry || entry.status !== 'done') {
        return res.status(400).json({ ok: false, message: '먼저 그림을 받아주세요.' });
      }
      // 비회원으로 응모한 뒤 가입해서 받는 경로. 갤러리에 보이는 정보만으로 남의 응모를 가로채지 못하게
      // 접수 때 받은 claimToken 이 맞아야 하고, 실제 귀속은 지급이 성공한 뒤에 한다.
      let bindGuest = false;
      if (!entry.memberId) {
        const ok = !!claimToken && !!entry.claimHash && sha256(claimToken) === entry.claimHash;
        if (!ok) return res.status(403).json({ ok: false, message: '이 응모는 처음 만든 브라우저에서만 받을 수 있어요.' });
        bindGuest = true;
      } else if (entry.memberId !== memberId) {
        return res.status(400).json({ ok: false, message: '먼저 그림을 받아주세요.' });
      }

      // 3) 예약을 먼저 잡는다. unique index 가 이중 지급을 막는 유일한 장치라 없으면 진행하지 않는다.
      if (!(await ensureRewardIndex(rewards))) {
        // 503 은 게이트웨이가 가로채 JSON 이 프론트에 닿지 않는다 — 409 로 보낸다.
        return res.status(409).json({ ok: false, message: '잠시 후 다시 시도해주세요.' });
      }
      const { insertedId } = await rewards.insertOne({
        memberId, entryId: String(entry._id), amount: POINT_AMOUNT,
        settled: false, phase: 'reserved', reservedAt: new Date(), participatedAt: nowKST(),
      });

      // 4) Cafe24 지급. 호출 직전에 calling 으로 바꿔 "결과 불명" 을 구분할 수 있게 한다.
      await rewards.updateOne({ _id: insertedId }, { $set: { phase: 'calling', apiStartedAt: new Date() } });
      try {
        await apiRequest('POST', `https://${mallId}.cafe24api.com/api/v2/admin/points`, {
          shop_no: 1,
          request: {
            member_id: memberId,
            order_id: null,
            amount: POINT_AMOUNT,
            type: 'increase',
            reason: '나의 쉼 순간 이벤트 참여 적립금',
          },
        }, {}, { timeout: 20000 });
      } catch (apiErr) {
        const st = apiErr && apiErr.response && apiErr.response.status;
        if (st >= 400 && st < 500) {
          // Cafe24 가 명시적으로 거절 — 지급 안 됨이 확실하니 예약을 되돌린다
          await rewards.deleteOne({ _id: insertedId });
          console.error('[쉼순간] 적립금 거절:', memberId, st, JSON.stringify(apiErr.response.data || {}));
          return res.status(500).json({ ok: false, message: '잠시 후 다시 시도해주세요.' });
        }
        // 응답 없음·5xx·타임아웃 — 지급됐는지 알 수 없다. 예약을 남겨 잠그고 정산 대상으로 표시한다.
        await rewards.updateOne({ _id: insertedId },
          { $set: { settled: 'unknown', lastError: String((apiErr && apiErr.message) || apiErr), failedAt: new Date() } });
        console.error('[쉼순간] ★ 적립금 결과 불명 — 정산 필요:', memberId, String(insertedId), apiErr && apiErr.message);
        return res.status(202).json({ ok: false, pending: true, message: PENDING_MSG });
      }

      // 5) 확정. 여기서 실패해도 지급은 이미 됐으므로 예약을 절대 지우지 않는다.
      try {
        await rewards.updateOne({ _id: insertedId }, { $set: { settled: true, phase: 'settled', settledAt: new Date() } });
        const bind = bindGuest
          ? { rewarded: true, memberId, displayId: looksInappropriate(memberId) ? '회원***' : maskId(memberId) }
          : { rewarded: true };
        await entries.updateOne({ _id: entry._id }, { $set: bind });
      } catch (dbErr) {
        console.error('[쉼순간] ★ 지급 후 확정 실패 — 정산 필요:', memberId, String(insertedId), dbErr.message);
        return res.status(202).json({ ok: false, pending: true, message: PENDING_MSG });
      }

      console.log(`[쉼순간] ${memberId} 적립금 ${POINT_AMOUNT}원 지급 완료`);
      return res.json({ ok: true, message: `🎉 적립금 ${POINT_AMOUNT.toLocaleString()}원이 지급되었습니다!` });
    } catch (err) {
      if (err && err.code === 11000) {
        // 동시 클릭이 여기로 온다. 상대 예약의 상태를 읽어 그대로 답한다 (확정 전이면 pending).
        let doc = null;
        try { doc = await getDb().collection(REWARD_COLLECTION).findOne({ memberId }); } catch { /* 무시 */ }
        if (doc && (doc.settled === true || doc.settled === undefined)) {
          return res.status(400).json({ ok: false, alreadyDone: true, message: '이미 적립금을 받으셨습니다.' });
        }
        return res.status(202).json({ ok: false, pending: true, message: '적립 처리 중이에요. 잠시 후 확인해주세요.' });
      }
      console.error('[쉼순간] 적립금 지급 오류:', err.response?.data || err.message);
      return res.status(500).json({ ok: false, message: '잠시 후 다시 시도해주세요.' });
    }
  });

  // 워커 시작 — 3초마다 대기 중인 응모를 최대 2건씩 처리
  setInterval(() => tick(getDb), 3000);
  console.log('✅ [쉼순간] 라우트 등록 완료 · FTP', FTP_DIR);
}

module.exports = {
  mount,
  ENTRY_COLLECTION,
  REWARD_COLLECTION,
  CHIPS,
  publicName,
  looksInappropriate,
};

// 테스트·운영 점검용 내부 진입점 (라우트에는 쓰지 않는다)
module.exports.__internals = { generateScene, loadBase, posePath, genBudget, hexLum, CHIPS, stampLogo, locateTag, renderArtwork, renderShareCard };
