// ================================================================
// 「포켓몬 SNS 포토리뷰」 모듈  (2026.10)
//
//  회원이 SNS 에 올린 리뷰를 포켓몬 리뷰 페이지(review.html)에서 인증한다 (2026-10-19 예시 디자인 기준).
//  필수 항목이 모두 있어야 참여 완료 — 인스타그램 ID · 이름 · 게시물 링크 · 리뷰 캡처 이미지 · 개인정보 동의.
//  연락처는 받지 않는다 (2026-10-19 결정) — 회원만 참여하므로 당첨 연락은 회원정보로 한다.
//  이미지는 Cafe24 FTP 에 올리고, 등록하는 즉시 리뷰 갤러리에 공개한다 (2026-10-19 결정 — 검수 없음).
//  관리자는 문제 있는 건만 숨김·삭제한다.
//  이름은 당첨 확인용이라 관리 API 에서만 나간다. 갤러리·내 참여 조회에는 절대 싣지 않는다.
//
//  · 회원만 참여한다. 아이디가 없으면 접수하지 않는다.
//  · 다른 게시물이면 여러 번 참여할 수 있다 (2026-10-19 결정). 같은 게시물 링크는 한 번만 — 누가 올렸든.
//    회원당 상한이 필요하면 POKEMON_REVIEW_MAX_PER_MEMBER=숫자 (기본 0 = 제한 없음).
//    테스트 계정(testid·yogibo)은 같은 링크도 다시 올려 확인할 수 있다.
//  · 이미지는 회원아이디 폴더에 둔다 — <FTP_DIR>/<회원아이디>/YYYYMMDD_<난수8>.jpg (2026-10-19 결정, 회원별로 찾기 쉽게).
//    그래서 이미지 주소에는 아이디가 보인다. 갤러리 화면에 찍는 아이디만 가린다.
//  · 사진은 긴 변 1400px JPEG 로 다시 저장해 올린다 — 이때 EXIF(촬영 위치 등)가 빠진다.
//
//  라우트는 server.js 에서 mount(app, deps) 로 붙인다. 경로는 /api/pokemon-review/*
//  관리 페이지는 /pokemon-review-admin.html — 키는 POKEMON_REVIEW_ADMIN_KEY (없으면 REST_MOMENT_ADMIN_KEY).
// ================================================================
'use strict';

const crypto = require('crypto');
const multer = require('multer');
const sharp = require('sharp');
const ftp = require('basic-ftp');
const { Readable } = require('stream');
const { ObjectId } = require('mongodb');

// ── 컬렉션 · 설정 ─────────────────────────────────────────────────
const COLLECTION = 'pokemonReview';

const MAX_PHOTO_BYTES = 10 * 1024 * 1024;     // 한 장당. 페이지 안내 문구와 같은 값 — 올릴 때 줄인다
const MAX_PHOTOS = 3;                          // 게시물당 이미지 수 (2026-10-20 — 여러 장이면 참여 현황에서 돌아가며 보인다)
const MAX_EDGE = 1400;
const MAX_LINK = 500;
const MAX_PER_MEMBER = Math.max(0, Number(process.env.POKEMON_REVIEW_MAX_PER_MEMBER) || 0);   // 0 = 제한 없음
const TEST_IDS = (process.env.POKEMON_REVIEW_TEST_IDS || 'testid,yogibo')
  .split(',').map(s => s.trim().toLowerCase()).filter(Boolean);

const FTP_DIR = process.env.POKEMON_REVIEW_FTP_DIR || '/web/img/md/pokemon';
// Cafe24 FTP 는 계정 홈에 갇혀 있어 절대경로를 거부한다 — 다른 이벤트 모듈처럼 상대경로로 쓴다.
const FTP_DIR_REL = FTP_DIR.split('/').filter(Boolean).join('/');
const FTP_PUBLIC = (process.env.POKEMON_REVIEW_PUBLIC_BASE || 'https://yogibo.openhost.cafe24.com/web/img/md/pokemon').replace(/\/$/, '');

let ADMIN_KEY = String(process.env.POKEMON_REVIEW_ADMIN_KEY || process.env.REST_MOMENT_ADMIN_KEY || '').trim();
if (ADMIN_KEY && !/^[\x21-\x7E]+$/.test(ADMIN_KEY)) {
  // HTTP 헤더는 ASCII 만 안전하게 실린다. 한글 키는 브라우저가 못 보내므로 아예 닫는다.
  console.error('★ [포켓몬리뷰] 관리 키는 영문·숫자·기호(ASCII)만 됩니다 — 관리 API 를 닫습니다');
  ADMIN_KEY = '';
}

// 쓰기(접수)를 허용할 오리진 — 몰 도메인 + Cafe24 스킨 미리보기. `*` 는 서브도메인 한 칸.
const ALLOWED_ORIGINS = (process.env.POKEMON_REVIEW_ALLOWED_ORIGINS || [
  'https://yogibo.kr',
  'https://www.yogibo.kr',
  'https://m.yogibo.kr',
  'https://yogibo.co.kr',
  'https://www.yogibo.co.kr',
  'https://yogibo.cafe24.com',
  'https://*.yogibo.cafe24.com',
  'http://yogibo.cafe24.com',
  'http://*.yogibo.cafe24.com',
].join(',')).split(',').map(s => s.trim()).filter(Boolean);

// ── 작은 도구 ─────────────────────────────────────────────────────
// Cafe24 아이디는 영문 소문자·숫자라 소문자로 맞춘다 — 대소문자만 바꿔 횟수 제한을 피하지 못하게.
function normId(v) {
  if (v == null) return null;
  const s = String(v).trim().toLowerCase();
  return s && s !== 'null' && s !== 'undefined' && s !== 'guest' && !s.startsWith('guest_') ? s : null;
}
function isTestId(id) { return !!id && TEST_IDS.includes(id); }

// 갤러리에 보여줄 아이디. 앞 두 글자만 남기고 가린다.
function maskId(id) {
  let t = String(id || '');
  if (!t) return null;
  const at = t.indexOf('@');
  if (at > 0) t = t.slice(0, at);
  if (t.length <= 2) return t.charAt(0) + '***';
  return t.slice(0, 2) + '***' + (t.length >= 6 ? t.slice(-1) : '');
}

// 인스타그램 ID — 앞의 @ 는 떼고 저장한다. 인스타그램 규칙대로 영문·숫자·점·밑줄 30자까지.
const SNS_ID_RE = /^[A-Za-z0-9._]{1,30}$/;
function cleanSnsId(v) {
  const s = String(v == null ? '' : v).trim().replace(/^@+/, '');
  return SNS_ID_RE.test(s) ? s : null;
}

// 이름 — 2~50자. 줄바꿈·제어문자는 지운다.
function cleanName(v) {
  const s = String(v == null ? '' : v).replace(/[\u0000-\u001F\u007F]/g, '').trim().replace(/\s+/g, ' ');
  return s.length >= 2 && s.length <= 50 ? s : null;
}

function agreed(v) { return /^(1|true|on|yes)$/i.test(String(v == null ? '' : v)); }

// 고객에게 보여주는 접수 번호 — 문서 id 끝 8자리
function receiptOf(id) { return 'PKR-' + String(id).slice(-8).toUpperCase(); }

// 게시물 링크 — http(s) 만. "instagram.com/p/..." 처럼 주소만 붙여넣어도 https 를 붙여 받는다.
function cleanLink(v) {
  let s = String(v == null ? '' : v).trim();
  if (!s || s.length > MAX_LINK) return null;
  if (!/^[a-z][a-z0-9+.-]*:/i.test(s)) s = 'https://' + s.replace(/^\/+/, '');
  try {
    const u = new URL(s);
    if (u.protocol !== 'http:' && u.protocol !== 'https:') return null;
    if (!u.hostname.includes('.')) return null;
    return u.href.length <= MAX_LINK ? u.href : null;
  } catch { return null; }
}

// 게시물 링크는 주소 형태(http/https)면 어느 사이트든 받는다 — 참여 문턱을 낮추려고 인스타그램 전용 제한을 풀었다 (2026-10-20).
// 대신 등록 즉시 공개되므로, 엉뚱한 사이트로 보내는 링크는 관리 페이지에서 숨김·삭제로 정리한다 (고객 수정 이력은 prev 로 남는다).

function kst(d) { return new Date((d ? new Date(d) : new Date()).getTime() + 9 * 3600 * 1000); }
const pad = n => String(n).padStart(2, '0');
function ymdCompact(d) { const k = kst(d); return `${k.getUTCFullYear()}${pad(k.getUTCMonth() + 1)}${pad(k.getUTCDate())}`; }
function ymdDot(d) { const k = kst(d); return `${k.getUTCFullYear()}.${pad(k.getUTCMonth() + 1)}.${pad(k.getUTCDate())}`; }
function publicName() { return `${ymdCompact()}_${crypto.randomBytes(4).toString('hex')}.jpg`; }
// 회원아이디 → 폴더명. SNS 로그인 아이디(예: 1234567@k)처럼 @ 가 섞여도 경로가 깨지지 않게 영문·숫자·._- 만 남긴다.
function folderOf(memberId) { return String(memberId).replace(/[^a-z0-9._-]/g, '_').slice(0, 64); }
const FOLDER_RE = /^[a-z0-9._-]{1,64}$/;

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

const oid = v => { try { return new ObjectId(String(v)); } catch { return null; } };

// ── 이미지 · FTP ──────────────────────────────────────────────────
/** 고객 사진을 긴 변 1400px JPEG 로 다시 저장한다. 회전(EXIF) 반영 후 메타데이터는 버린다. */
async function normalizePhoto(buf) {
  return sharp(buf, { failOn: 'none' })
    .rotate()
    .resize({ width: MAX_EDGE, height: MAX_EDGE, fit: 'inside', withoutEnlargement: true })
    .flatten({ background: '#ffffff' })        // 투명 PNG 가 검게 나오지 않게
    .jpeg({ quality: 85, mozjpeg: true })
    .toBuffer();
}

function ftpAccess(client) {
  return client.access({
    host: process.env.FTP_HOST || 'yogibo.ftp.cafe24.com',
    port: Number(process.env.FTP_PORT || 21),
    user: process.env.FTP_USER,
    password: process.env.FTP_PASS,
    secure: false,
  });
}

/** 이미지 여러 장을 한 번 접속해서 올리고 공개 URL 들을 돌려준다 (게시물당 최대 MAX_PHOTOS 장). */
async function ftpUploadMany(buffers, folder) {
  const names = buffers.map(() => publicName());
  const urls = names.map(n => `${FTP_PUBLIC}/${folder}/${n}`);
  // 잠깐의 FTP 흔들림에 고객이 다시 올리지 않게 3번까지 다시 붙는다. 같은 이름으로 다시 올리면 덮어쓴다.
  let last;
  for (let i = 0; i < 3; i++) {
    const client = new ftp.Client(30000);
    client.ftp.verbose = false;
    try {
      await ftpAccess(client);
      await client.ensureDir(`${FTP_DIR_REL}/${folder}`);   // 회원 폴더가 없으면 만들고 그 안으로 들어간다
      for (let k = 0; k < buffers.length; k++) await client.uploadFrom(Readable.from(buffers[k]), names[k]);
      return urls;
    } catch (e) {
      last = e;
      await new Promise(r => setTimeout(r, 1500 * (i + 1)));
    } finally {
      client.close();
    }
  }
  // 끝내 실패 — 일부만 올라간 파일이 주인 없이 남지 않게 지워 본다
  await Promise.all(urls.map(u => ftpRemoveByUrl(u).catch(() => {})));
  throw last;
}

// 게시물의 이미지 목록. 여러 장 지원 전에 올라온 문서는 imageUrl 하나뿐이다.
function imagesOf(d) {
  const list = Array.isArray(d && d.images) ? d.images.filter(Boolean) : [];
  return list.length ? list : (d && d.imageUrl ? [d.imageUrl] : []);
}

// 이 모듈이 올린 파일만 지운다 — 리뷰 폴더 아래 <회원폴더>/<파일명> 형식이 정확히 맞아야 한다.
const PUBLIC_NAME_RE = /^\d{8}_[0-9a-f]{8}\.jpg$/;
async function ftpRemoveByUrl(url) {
  if (!url) return { ok: true };
  if (String(url).indexOf(FTP_PUBLIC + '/') !== 0) return { ok: false, error: '리뷰 폴더 파일이 아님: ' + url };
  const parts = String(url).slice(FTP_PUBLIC.length + 1).split('/');
  if (parts.length !== 2 || !FOLDER_RE.test(parts[0]) || !PUBLIC_NAME_RE.test(parts[1])) {
    return { ok: false, error: '지울 수 없는 경로: ' + url };
  }
  if (!process.env.FTP_USER) return { ok: false, error: 'FTP 계정 미설정' };
  const client = new ftp.Client(30000);
  client.ftp.verbose = false;
  try {
    await ftpAccess(client);
    await client.remove(`${FTP_DIR_REL}/${parts[0]}/${parts[1]}`);
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  } finally {
    client.close();
  }
}

// ── 라우트 ────────────────────────────────────────────────────────
function mount(app, deps) {
  const { getDb } = deps;
  const col = () => getDb().collection(COLLECTION);

  // 예전 검수 방식으로 들어와 대기 중인 건을 공개로 돌린다 — 이제는 등록 즉시 공개다. 관리자가 숨긴 건(hidden)은 그대로 둔다.
  Promise.resolve().then(() => col().updateMany({ status: 'pending' }, { $set: { status: 'approved' } }))
    .then(r => { if (r && r.modifiedCount) console.log(`[포켓몬리뷰] 검수 대기 ${r.modifiedCount}건 → 공개`); })
    .catch(e => console.warn('[포켓몬리뷰] 검수 대기 전환 실패:', e.message));

  // 읽기(갤러리·내 참여)는 공개 데이터라 어디서 불러도 된다 — 스킨 미리보기·로컬 확인이 막히지 않게.
  const allowRead = (req, res, next) => {
    const origin = req.headers.origin;
    if (origin) { res.setHeader('Access-Control-Allow-Origin', origin); res.setHeader('Vary', 'Origin'); }
    next();
  };
  // 쓰기(접수)는 사진이 올라오므로 몰 도메인만 받는다.
  const allowWrite = (req, res, next) => {
    const origin = req.headers.origin;
    if (!originAllowed(origin)) {
      console.warn('[포켓몬리뷰] 차단된 오리진:', origin);
      return res.status(403).json({ ok: false, message: '허용되지 않은 요청입니다.' });
    }
    if (origin) { res.setHeader('Access-Control-Allow-Origin', origin); res.setHeader('Vary', 'Origin'); }
    next();
  };
  const allowAdmin = (req, res, next) => {
    // 503 은 클라우드타입 게이트웨이가 자기 에러 페이지로 바꿔치기한다 — 4xx JSON 으로 알린다.
    if (!ADMIN_KEY) return res.status(403).json({ ok: false, notConfigured: true, message: 'POKEMON_REVIEW_ADMIN_KEY(또는 REST_MOMENT_ADMIN_KEY)가 서버에 설정되지 않았습니다.' });
    const given = String(req.get('x-pokemon-admin-key') || '');
    const a = Buffer.from(given), b = Buffer.from(ADMIN_KEY);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return res.status(401).json({ ok: false, message: '관리 키가 맞지 않습니다.' });
    next();
  };

  // ★ 원본 사진은 메모리에서만 다룬다. 줄인 JPEG 만 FTP 에 남는다.
  const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: MAX_PHOTO_BYTES, files: MAX_PHOTOS, fields: 20 },
    fileFilter: (_req, file, cb) => {
      if (!/^image\//.test(file.mimetype)) return cb(new Error('이미지 파일만 올릴 수 있습니다.'));
      cb(null, true);
    },
  }).array('photo', MAX_PHOTOS);
  function uploadErrMsg(e) {
    if (e.code === 'LIMIT_FILE_SIZE') return '이미지는 한 장에 10MB 이하로 등록해주세요.';
    if (e.code === 'LIMIT_FILE_COUNT' || e.code === 'LIMIT_UNEXPECTED_FILE') return `이미지는 최대 ${MAX_PHOTOS}장까지 등록할 수 있어요.`;
    return e.message || '이미지를 읽지 못했습니다.';
  }
  const filesOf = req => (req.files || []).filter(f => f && f.buffer && f.buffer.length);
  // 받은 이미지를 차례로 줄인다. 못 읽는 장이 있으면 몇 번째인지 알려준다.
  async function normalizeAll(files) {
    const out = [];
    for (let i = 0; i < files.length; i++) {
      try { out.push(await normalizePhoto(files[i].buffer)); }
      catch (e) {
        console.warn('[포켓몬리뷰] 사진 변환 실패:', e.message);
        const err = new Error(`${files.length > 1 ? (i + 1) + '번째 ' : ''}이미지를 읽지 못했어요. JPG·PNG·WEBP 이미지로 다시 등록해주세요.`);
        err.userFacing = true;
        throw err;
      }
    }
    return out;
  }

  // ── 등록 (참여) ──
  app.post('/api/pokemon-review/entry', allowWrite, (req, res) => {
    upload(req, res, async (uploadErr) => {
      if (uploadErr) return res.status(400).json({ ok: false, field: 'photo', message: uploadErrMsg(uploadErr) });
      let uploaded = null;
      try {
        const body = req.body || {};
        const memberId = normId(body.memberId);
        if (!memberId) return res.status(401).json({ ok: false, needLogin: true, message: '회원만 참여할 수 있어요. 로그인 후 다시 시도해주세요.' });

        const snsId = cleanSnsId(body.snsId);
        if (!snsId) return res.status(400).json({ ok: false, field: 'snsId', message: '인스타그램 ID를 확인해주세요. (@ 없이 영문·숫자·점·밑줄)' });
        const name = cleanName(body.name);
        if (!name) return res.status(400).json({ ok: false, field: 'name', message: '이름을 2자 이상 입력해주세요.' });
        const snsLink = cleanLink(body.snsLink);
        if (!snsLink) return res.status(400).json({ ok: false, field: 'snsLink', message: '게시물 주소를 확인해주세요. (예: instagram.com/p/...)' });
        const files = filesOf(req);
        if (!files.length) return res.status(400).json({ ok: false, field: 'photo', message: '리뷰 캡처 이미지를 1장 이상 등록해주세요.' });
        if (!agreed(body.agree)) return res.status(400).json({ ok: false, field: 'agree', message: '개인정보 수집 및 이용에 동의해주세요.' });

        const test = isTestId(memberId);
        if (!test) {
          if (MAX_PER_MEMBER) {
            const mine = await col().countDocuments({ memberId });
            if (mine >= MAX_PER_MEMBER) {
              return res.status(409).json({ ok: false, alreadyDone: true, message: `회원당 ${MAX_PER_MEMBER}번까지 참여할 수 있어요.` });
            }
          }
          const dup = await col().findOne({ snsLink }, { projection: { _id: 1 } });
          if (dup) return res.status(409).json({ ok: false, field: 'snsLink', message: '이미 등록된 게시물이에요. 다른 게시물 링크를 넣어주세요.' });
        }

        const jpegs = await normalizeAll(files);
        uploaded = await ftpUploadMany(jpegs, folderOf(memberId));
        const doc = {
          memberId,
          displayId: maskId(memberId),
          snsId,
          snsLink,
          name,                                    // 이름은 당첨 확인용 — 관리 API 에서만 나간다
          agreedAt: new Date(),                    // 개인정보 수집·이용 동의 시각
          images: uploaded,                        // 1~3장, 첫 장이 대표
          imageUrl: uploaded[0],                   // 대표 이미지 — 여러 장 지원 전 코드·문서와 맞추려고 남긴다
          status: 'approved',                      // 등록 즉시 공개. 관리자가 숨기면 hidden (pending 은 예전 검수 방식의 흔적)
          test,
          createdAt: new Date(),
        };
        const { insertedId } = await col().insertOne(doc);
        uploaded = null;                           // 문서가 파일을 가리키게 됐다 — 아래 정리 대상에서 뺀다
        console.log('[포켓몬리뷰] 접수:', memberId, '@' + snsId, String(insertedId), doc.images.length + '장');   // 이름은 로그에 남기지 않는다
        return res.json({ ok: true, id: String(insertedId), receipt: receiptOf(insertedId), images: doc.images, imageUrl: doc.imageUrl, status: doc.status });
      } catch (err) {
        if (err.userFacing) return res.status(400).json({ ok: false, field: 'photo', message: err.message });
        console.error('[포켓몬리뷰] 접수 오류:', err.message);
        // FTP 에는 올렸는데 기록을 못 남긴 경우 — 주인 없는 파일을 남기지 않는다
        if (uploaded) uploaded.forEach(u => ftpRemoveByUrl(u).catch(() => {}));
        return res.status(500).json({ ok: false, message: '등록 중 문제가 생겼어요. 잠시 후 다시 시도해주세요.' });
      }
    });
  });

  // ── 본인 게시물 수정 ──
  // 인스타그램 ID·게시물 링크는 다시 받고, 이름은 새로 넣었을 때만 바꾼다.
  // 이미지는 남길 기존 이미지(keep) + 새로 올린 이미지(photo) 로 다시 짠다 — 합쳐서 1~3장.
  //   페이지는 keepSent=1 을 같이 보낸다. 없으면 여러 장 지원 전 화면이라 "새 이미지를 보내면 통째로 교체, 안 보내면 그대로".
  // 공개 상태는 건드리지 않는다 — 관리자가 숨긴 건은 고쳐도 계속 숨김이다.
  // 바꾸기 직전 값(prev)과 수정 횟수를 남긴다 — 남의 아이디로 바꿔치기했을 때 관리자가 알아보고 되돌릴 수 있게.
  app.post('/api/pokemon-review/entry/:id', allowWrite, (req, res) => {
    upload(req, res, async (uploadErr) => {
      if (uploadErr) return res.status(400).json({ ok: false, field: 'photo', message: uploadErrMsg(uploadErr) });
      let newUrls = null;
      try {
        const _id = oid(req.params.id);
        if (!_id) return res.status(400).json({ ok: false, message: '잘못된 요청입니다.' });
        const body = req.body || {};
        const memberId = normId(body.memberId);
        if (!memberId) return res.status(401).json({ ok: false, needLogin: true, message: '로그인 후 수정할 수 있어요.' });

        const doc = await col().findOne({ _id });
        if (!doc || doc.memberId !== memberId) return res.status(404).json({ ok: false, message: '수정할 게시물을 찾지 못했어요.' });

        const snsId = cleanSnsId(body.snsId);
        if (!snsId) return res.status(400).json({ ok: false, field: 'snsId', message: '인스타그램 ID를 확인해주세요. (@ 없이 영문·숫자·점·밑줄)' });
        const snsLink = cleanLink(body.snsLink);
        if (!snsLink) return res.status(400).json({ ok: false, field: 'snsLink', message: '게시물 주소를 확인해주세요. (예: instagram.com/p/...)' });
        const nameRaw = String(body.name == null ? '' : body.name).trim();
        const name = nameRaw ? cleanName(nameRaw) : doc.name;   // 비워 두면 그대로
        if (!name) return res.status(400).json({ ok: false, field: 'name', message: '이름을 2자 이상 입력해주세요.' });

        if (!isTestId(memberId) && snsLink !== doc.snsLink) {
          const dup = await col().findOne({ snsLink, _id: { $ne: _id } }, { projection: { _id: 1 } });
          if (dup) return res.status(409).json({ ok: false, field: 'snsLink', message: '이미 등록된 게시물이에요. 다른 게시물 링크를 넣어주세요.' });
        }

        // 이미지 다시 짜기 — 문서를 바꾸기 전에 지금 목록을 잡아 둔다 (나중에 뺀 파일을 지울 근거)
        const current = imagesOf(doc);
        const files = filesOf(req);
        let keep;
        if (body.keepSent) {
          // 이 게시물에 실제로 있던 이미지만 남길 수 있다 — 남의 파일 주소를 끼워 넣지 못하게
          keep = [].concat(body.keep == null ? [] : body.keep).map(String).filter((u, i, a) => current.includes(u) && a.indexOf(u) === i);
        } else {
          keep = files.length ? [] : current;
        }
        if (keep.length + files.length < 1) return res.status(400).json({ ok: false, field: 'photo', message: '이미지를 1장 이상 남겨주세요.' });
        if (keep.length + files.length > MAX_PHOTOS) return res.status(400).json({ ok: false, field: 'photo', message: `이미지는 최대 ${MAX_PHOTOS}장까지 등록할 수 있어요.` });

        if (files.length) newUrls = await ftpUploadMany(await normalizeAll(files), folderOf(memberId));
        const images = keep.concat(newUrls || []);

        const now = new Date();
        const set = { snsId, snsLink, name, images, imageUrl: images[0], editedAt: now, prev: { snsId: doc.snsId, snsLink: doc.snsLink, at: now } };

        // 문서를 먼저 바꾼다 — 그 사이 지워졌으면(0건) 방금 올린 이미지를 되돌린다. 뺀 이미지는 문서가 새 목록을 가리킨 뒤에 지운다.
        const r = await col().updateOne({ _id, memberId }, { $set: set, $inc: { editCount: 1 } });
        if (!r.matchedCount) {
          if (newUrls) newUrls.forEach(u => ftpRemoveByUrl(u).catch(() => {}));
          return res.status(404).json({ ok: false, message: '수정할 게시물을 찾지 못했어요.' });
        }
        newUrls = null;
        const removed = current.filter(u => !images.includes(u));
        for (const u of removed) {
          const rm = await ftpRemoveByUrl(u);
          if (!rm.ok) console.warn('[포켓몬리뷰] 뺀 이미지 삭제 실패:', u, rm.error);
        }
        console.log('[포켓몬리뷰] 고객 수정:', memberId, String(_id), `이미지 ${current.length}→${images.length}장`);
        return res.json({ ok: true, receipt: receiptOf(_id), images, imageUrl: images[0], status: doc.status });
      } catch (err) {
        if (err.userFacing) return res.status(400).json({ ok: false, field: 'photo', message: err.message });
        console.error('[포켓몬리뷰] 고객 수정 오류:', err.message);
        if (newUrls) newUrls.forEach(u => ftpRemoveByUrl(u).catch(() => {}));
        return res.status(500).json({ ok: false, message: '수정 중 문제가 생겼어요. 잠시 후 다시 시도해주세요.' });
      }
    });
  });

  // ── 내 참여 상태 ──
  app.get('/api/pokemon-review/mine', allowRead, async (req, res) => {
    try {
      const memberId = normId(req.query.memberId);
      if (!memberId) return res.json({ ok: true, count: 0, items: [], canAdd: false });
      // 완료 화면에 최근 12장만 보여준다 — 건수는 따로 센다
      const [rows, count] = await Promise.all([
        col().find({ memberId }).sort({ createdAt: -1 }).limit(12).project({ images: 1, imageUrl: 1, status: 1, createdAt: 1, snsId: 1, snsLink: 1 }).toArray(),
        col().countDocuments({ memberId }),
      ]);
      const test = isTestId(memberId);
      return res.json({
        ok: true,
        count,
        canAdd: test || !MAX_PER_MEMBER || count < MAX_PER_MEMBER,
        test,
        // 수정 화면을 채우는 값. 인스타그램 ID·링크는 갤러리에도 공개되는 값이라 실어도 된다 — 이름은 싣지 않는다.
        items: rows.map(d => ({ id: String(d._id), receipt: receiptOf(d._id), images: imagesOf(d), imageUrl: imagesOf(d)[0], status: d.status, date: ymdDot(d.createdAt), snsId: d.snsId, snsLink: d.snsLink })),
      });
    } catch (err) {
      console.error('[포켓몬리뷰] 내 참여 조회 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  // ── 리뷰 갤러리 (공개분만) ──
  app.get('/api/pokemon-review/recent', allowRead, async (req, res) => {
    try {
      const limit = Math.min(Math.max(Number(req.query.limit) || 12, 1), 48);
      const offset = Math.min(Math.max(Number(req.query.offset) || 0, 0), 5000);
      const filter = { status: 'approved' };
      // limit+1 로 한 장 더 읽어 다음 페이지가 있는지 알아낸다.
      const [rows, total] = await Promise.all([
        col().find(filter).sort({ createdAt: -1 }).skip(offset).limit(limit + 1).toArray(),
        col().countDocuments(filter),
      ]);
      const hasMore = rows.length > limit;
      return res.json({
        ok: true,
        total,
        offset,
        hasMore,
        items: (hasMore ? rows.slice(0, limit) : rows).map(d => ({
          displayId: d.displayId,
          snsId: d.snsId,
          snsLink: d.snsLink,
          images: imagesOf(d),
          imageUrl: imagesOf(d)[0],
          date: ymdDot(d.createdAt),
        })),
      });
    } catch (err) {
      console.error('[포켓몬리뷰] 갤러리 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  // ── 관리 API (/pokemon-review-admin.html) — 키는 헤더 x-pokemon-admin-key ──
  const VIEWS = {
    pending:  { status: 'pending' },
    approved: { status: 'approved' },
    hidden:   { status: 'hidden' },
    all:      {},
  };

  app.get('/api/pokemon-review/admin/entries', allowAdmin, async (req, res) => {
    try {
      const limit = Math.min(Math.max(Number(req.query.limit) || 30, 1), 100);
      const offset = Math.min(Math.max(Number(req.query.offset) || 0, 0), 20000);
      const view = Object.prototype.hasOwnProperty.call(VIEWS, String(req.query.view)) ? String(req.query.view) : 'pending';
      const filter = Object.assign({}, VIEWS[view]);
      const q = String(req.query.q || '').trim().replace(/^@+/, '').slice(0, 60);
      if (q) {
        const re = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
        filter.$or = [{ memberId: re }, { snsId: re }, { snsLink: re }, { name: re }];
      }
      const [rows, pending, approved, hidden] = await Promise.all([
        col().find(filter).sort({ createdAt: -1 }).skip(offset).limit(limit + 1).toArray(),
        col().countDocuments({ status: 'pending' }),
        col().countDocuments({ status: 'approved' }),
        col().countDocuments({ status: 'hidden' }),
      ]);
      const hasMore = rows.length > limit;
      return res.json({
        ok: true,
        view,
        offset,
        hasMore,
        counts: { pending, approved, hidden, total: pending + approved + hidden },
        config: { maxPerMember: MAX_PER_MEMBER, testIds: TEST_IDS, ftpDir: FTP_DIR },
        items: (hasMore ? rows.slice(0, limit) : rows).map(d => ({
          id: String(d._id),
          receipt: receiptOf(d._id),
          memberId: d.memberId,
          snsId: d.snsId,
          snsLink: d.snsLink,
          name: d.name || '',
          images: imagesOf(d),
          imageUrl: imagesOf(d)[0],
          status: d.status,
          test: !!d.test,
          createdAt: d.createdAt || null,
          reviewedAt: d.reviewedAt || null,
          editCount: d.editCount || 0,             // 고객이 직접 고친 횟수
          prev: d.prev || null,                    // 고객이 마지막으로 고치기 직전의 인스타그램 ID·링크
          editedAt: d.editedAt || null,
        })),
      });
    } catch (err) {
      console.error('[포켓몬리뷰] 관리 목록 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  app.post('/api/pokemon-review/admin/entries/:id', allowAdmin, async (req, res) => {
    try {
      const _id = oid(req.params.id);
      if (!_id) return res.status(400).json({ ok: false, message: '잘못된 id' });
      const action = String((req.body || {}).action || '');
      if (action === 'approve' || action === 'hide') {
        const r = await col().updateOne({ _id }, { $set: { status: action === 'approve' ? 'approved' : 'hidden', reviewedAt: new Date() } });
        if (!r.matchedCount) return res.status(404).json({ ok: false, message: '기록이 없습니다.' });
        console.log('[포켓몬리뷰] 관리:', action, String(_id));
        return res.json({ ok: true });
      }
      if (action === 'edit') {
        // 고객이 잘못 적은 인스타그램 ID·링크·이름을 관리자가 고친다. 이미지는 그대로 둔다.
        const body = req.body || {};
        const snsId = cleanSnsId(body.snsId);
        if (!snsId) return res.status(400).json({ ok: false, message: '인스타그램 ID 형식이 맞지 않습니다. (영문·숫자·점·밑줄, 30자 이내)' });
        const snsLink = cleanLink(body.snsLink);
        if (!snsLink) return res.status(400).json({ ok: false, message: '게시물 링크 형식이 맞지 않습니다. (http:// 또는 https:// 주소)' });
        const name = cleanName(body.name);
        if (!name) return res.status(400).json({ ok: false, message: '이름은 2~50자로 입력해주세요.' });
        const before = await col().findOneAndUpdate({ _id }, { $set: { snsId, snsLink, name, editedAt: new Date() } });
        if (!before) return res.status(404).json({ ok: false, message: '기록이 없습니다.' });
        // 이름은 로그에 남기지 않는다
        console.log(`[포켓몬리뷰] 관리: 수정 ${String(_id)} @${before.snsId} ${before.snsLink} → @${snsId} ${snsLink}`);
        return res.json({ ok: true, snsId, snsLink, name });
      }
      if (action === 'delete') {
        // 문서를 먼저 빼내고 파일을 지운다 — 파일 삭제가 실패해도 갤러리에서는 확실히 빠진다.
        const doc = await col().findOneAndDelete({ _id });
        if (!doc) return res.status(404).json({ ok: false, message: '기록이 없습니다.' });
        const failed = [];
        for (const u of imagesOf(doc)) {
          const rm = await ftpRemoveByUrl(u);
          if (!rm.ok) { failed.push(u); console.warn('[포켓몬리뷰] 파일 삭제 실패:', u, rm.error); }
        }
        console.log('[포켓몬리뷰] 관리: 삭제', String(_id), doc.memberId, failed.length ? `파일 ${failed.length}개 삭제 실패` : '파일 삭제됨');
        return res.json({ ok: true, ftpRemoved: !failed.length, warn: failed.length ? '기록은 지웠지만 이미지 파일 일부를 지우지 못했습니다: ' + failed.join(', ') : null });
      }
      return res.status(400).json({ ok: false, message: '알 수 없는 action' });
    } catch (err) {
      console.error('[포켓몬리뷰] 관리 변경 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  console.log('✅ [포켓몬리뷰] 라우트 등록 완료 · FTP', FTP_DIR);
}

module.exports = { mount, COLLECTION };

// 테스트용 내부 진입점 (라우트에는 쓰지 않는다)
module.exports.__internals = { imagesOf, normId, cleanSnsId, cleanLink, cleanName, agreed, receiptOf, maskId, normalizePhoto, originAllowed, publicName, folderOf, ftpRemoveByUrl, FTP_PUBLIC };
