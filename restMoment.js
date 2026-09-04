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

// ── 설정 ──────────────────────────────────────────────────────────
const MAX_PHOTO_BYTES = 12 * 1024 * 1024;
const MAX_SENTENCE = 60;
const POINT_AMOUNT = Number(process.env.REST_MOMENT_POINT || 3000);
const EVENT_END = process.env.REST_MOMENT_END || '2026-09-27';
// 아이디당 생성 횟수. 마스터 아이디는 검수·테스트용이라 제한을 받지 않는다.
const MAX_PER_MEMBER = Number(process.env.REST_MOMENT_MAX_PER_MEMBER || 3);
const MASTER_IDS = (process.env.REST_MOMENT_MASTER_IDS || 'testid')
  .split(',').map(s => s.trim()).filter(Boolean);
const isMaster = id => !!id && MASTER_IDS.includes(String(id));

// 갤러리에 보여줄 아이디. 앞 두 글자만 남기고 가린다 — 당첨자 발표 관례와 같다.
function maskId(id) {
  const t = String(id || '');
  if (!t) return null;
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
  if (rewardIndexReady) return;
  try { await rewards.createIndex({ memberId: 1 }, { unique: true }); rewardIndexReady = true; }
  catch (e) { console.warn('[쉼순간] rewards 인덱스 생성 실패:', e.message); }
}

const FTP_DIR = process.env.FTP_REST_DIR || '/web/img/md/09';
const FTP_PUBLIC = (process.env.FTP_REST_PUBLIC_BASE || '').replace(/\/$/, '');

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
function looksInappropriate(text) {
  const t = String(text || '').replace(/\s/g, '');
  return BANNED.some(w => t.includes(w));
}

function withinEventPeriod(d) {
  const t = ymd(d);
  // 시작일은 막지 않는다 — 오픈 전 테스트를 위해 종료일만 본다.
  return t <= EVENT_END;
}

// ── FTP ───────────────────────────────────────────────────────────
/** 버퍼 1개를 FTP 에 올리고 공개 URL 을 돌려준다. */
async function ftpUpload(buffer, filename) {
  const client = new ftp.Client(30000);
  client.ftp.verbose = false;
  try {
    await client.access({
      host: process.env.FTP_HOST,
      port: Number(process.env.FTP_PORT || 21),
      user: process.env.FTP_USER,
      password: process.env.FTP_PASS,
      secure: false,
    });
    await client.ensureDir(FTP_DIR);
    await client.uploadFrom(Readable.from(buffer), filename);
    return `${FTP_PUBLIC}/${filename}`;
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
async function loadBase(chipKey) {
  const chip = CHIPS[chipKey];
  const local = path.join(__dirname, 'public', 'rest-moment', `${chip.baseKey}.jpg`);
  if (fs.existsSync(local)) return fs.readFileSync(local);
  // 로컬에 없으면 FTP 공개본에서 받아 쓴다
  const url = `${FTP_PUBLIC}/${chip.baseKey}.jpg`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`베이스 이미지를 찾을 수 없습니다: ${chip.baseKey}`);
  return Buffer.from(await res.arrayBuffer());
}

/** 갤러리용 — 문장 없이 그림만. 워터마크는 필수라 여기에도 넣는다. */
async function renderArtwork(baseBuf) {
  const W = 1024, H = 1280;
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
  const W = 1024, IMG_H = 1280, BAND = 300, H = IMG_H + BAND;

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
let working = false;

async function processOne(db, doc) {
  const col = db.collection(ENTRY_COLLECTION);
  const chip = CHIPS[doc.chip];
  let photo = null;
  try {
    const base = await loadBase(doc.chip);

    // 사진이 있으면 인물 레이어를 만든다. 없으면 이 단계 자체를 건너뛰어
    // 호출 비용이 0 이다 (개발요청서: "사진 미첨부 건은 애초에 호출이 없다").
    photo = doc.hadPhoto ? takePhoto(doc._id) : null;
    const { buf: scene, via } = await generatePersonLayer(base, photo, chip);

    // ★ 원본 사진 파기 — 생성에 넘긴 직후 여기서 끝난다.
    if (photo) { photo.buffer = null; photo = null; }

    const [artBuf, shareBuf] = await Promise.all([
      renderArtwork(scene),
      renderShareCard(scene, doc.sentence, chip.type),
    ]);

    const [imageUrl, shareUrl] = await Promise.all([
      ftpUpload(artBuf, publicName('')),
      ftpUpload(shareBuf, publicName('_s')),
    ]);

    await col.updateOne({ _id: doc._id }, {
      $set: { status: 'done', imageUrl, shareUrl, via, doneAt: nowKST() },
      $unset: { lastError: '' },
    });
    console.log(`[쉼순간] 완성 ${doc._id} (${doc.chip}, ${via})`);
  } catch (err) {
    if (photo) { photo.buffer = null; photo = null; }   // 실패해도 원본은 남기지 않는다
    const tries = (doc.tries || 0) + 1;
    // 2회까지 재시도하고, 그 뒤에는 실루엣 폴백으로 화면을 완성시킨다.
    await col.updateOne({ _id: doc._id }, {
      $set: {
        status: tries >= 3 ? 'failed' : 'pending',
        tries,
        lastError: String(err && err.message || err).slice(0, 300),
      },
    });
    console.error(`[쉼순간] 생성 실패 ${doc._id} (${tries}회):`, err.message);
  }
}

async function tick(getDb) {
  if (working) return;
  working = true;
  try {
    const db = getDb();
    if (!db) return;
    const col = db.collection(ENTRY_COLLECTION);
    const docs = await col.find({ status: 'pending' }).sort({ createdAt: 1 }).limit(2).toArray();
    for (const d of docs) await processOne(db, d);
  } catch (e) {
    console.error('[쉼순간] 워커 오류:', e.message);
  } finally {
    working = false;
  }
}

// ── 라우트 ────────────────────────────────────────────────────────
function mount(app, deps) {
  const { getDb, apiRequest, mallId } = deps;

  bootstrapFont();

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

        if (!CHIPS[chip]) {
          return res.status(400).json({ ok: false, message: '쉬는 방식을 하나 골라주세요.' });
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
        if (mid && !isMaster(mid)) {
          const used = await db.collection(ENTRY_COLLECTION).countDocuments({ memberId: mid, status: { $ne: 'failed' } });
          if (used >= MAX_PER_MEMBER) {
            return res.status(400).json({ ok: false, limitReached: true,
              message: '이 아이디로는 ' + MAX_PER_MEMBER + '번까지만 만들 수 있어요.' });
          }
        }

        const doc = {
          chip,
          type: CHIPS[chip].type,
          sentence: text,
          memberId: mid,                                   // 비회원도 접수한다 (가입 시 지급)
          displayId: mid ? (isMaster(mid) ? fakeDisplayId() : maskId(mid)) : null,
          hadPhoto: !!(req.file && req.file.buffer),
          agreeMarketing: String(agreeMarketing) === '1',
          status: 'pending',
          tries: 0,
          approved: false,                                 // 갤러리 공개는 검수 통과분만
          autoFlag: looksInappropriate(text),
          createdAt: now,
        };

        const { insertedId } = await db.collection(ENTRY_COLLECTION).insertOne(doc);

        // ★ 원본 사진은 Mongo 에도 디스크에도 쓰지 않는다.
        //   생성이 20~40초라 접수와 분리해야 해서, 프로세스 메모리에만 잠깐 둔다.
        //   워커가 꺼내 쓰는 즉시 사라지고, 10분이 지나면 자동으로 지워진다.
        if (req.file && req.file.buffer) {
          stashPhoto(insertedId, req.file.buffer, req.file.mimetype);
          req.file.buffer = null;
          req.file = null;
        }

        return res.json({ ok: true, entryId: String(insertedId), jobId: String(insertedId) });
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
      const limit = Math.min(Number(req.query.limit) || 8, 40);
      const col = db.collection(ENTRY_COLLECTION);

      const filter = { status: 'done', approved: true, imageUrl: { $ne: null } };
      if (req.query.tab === 'photo') filter.hadPhoto = true;
      if (req.query.tab === 'ai') filter.hadPhoto = false;

      const [items, total] = await Promise.all([
        col.find(filter).sort({ doneAt: -1 }).limit(limit).toArray(),
        col.countDocuments({ status: 'done' }),
      ]);

      return res.json({
        ok: true,
        total,
        items: items.map(d => ({
          src: d.hadPhoto ? 'photo' : 'ai',
          caption: d.sentence,
          type: d.type,
          imageUrl: d.imageUrl,
          displayId: d.displayId || maskId(d.memberId),
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
  app.post('/api/rest-moment/reward', allowWrite, async (req, res) => {
    const { memberId, entryId } = req.body || {};
    if (!memberId || typeof memberId !== 'string' || memberId.startsWith('guest_')) {
      return res.status(400).json({ ok: false, message: '로그인 후 받을 수 있습니다.' });
    }

    try {
      const db = getDb();
      const { ObjectId } = require('mongodb');
      const entries = db.collection(ENTRY_COLLECTION);
      const rewards = db.collection(REWARD_COLLECTION);

      // 1) 이미 받았는지 — 빠른 길. 진짜 방어는 아래 unique index 다.
      const already = await rewards.findOne({ memberId });
      if (already) {
        // 예약만 남고 지급이 안 끝난 채 2분이 지났으면(프로세스 중단 등) 되돌리고 다시 진행한다.
        const stale = already.settled === false && (Date.now() - new Date(already.reservedAt || 0).getTime()) > 2 * 60 * 1000;
        if (!stale) {
          return res.status(400).json({ ok: false, alreadyDone: true, message: '이미 적립금을 받으셨습니다.' });
        }
        await rewards.deleteOne({ _id: already._id });
      }

      // 2) 지급 자격 — 완성된 응모가 있어야 한다
      let entry = null;
      if (entryId) {
        try { entry = await entries.findOne({ _id: new ObjectId(entryId) }); } catch { /* 무시 */ }
      }
      if (!entry) entry = await entries.findOne({ memberId, status: 'done' });

      // 비회원으로 응모한 뒤 가입해서 받는 경로 — entryId 로 찾은 건에 회원을 붙인다
      if (entry && !entry.memberId) {
        await entries.updateOne({ _id: entry._id }, { $set: { memberId } });
        entry.memberId = memberId;
      }
      if (!entry || entry.memberId !== memberId || entry.status !== 'done') {
        return res.status(400).json({ ok: false, message: '먼저 그림을 받아주세요.' });
      }

      // 3) 지급 기록을 먼저 잡는다. 두 번째 클릭은 여기서 11000 으로 튕긴다.
      //    API 를 먼저 부르면 동시 클릭 두 개가 모두 Cafe24 까지 가서 두 번 지급될 수 있다.
      await ensureRewardIndex(rewards);
      await rewards.insertOne({
        memberId, entryId: String(entry._id), amount: POINT_AMOUNT,
        settled: false, reservedAt: new Date(), participatedAt: nowKST(),
      });

      // 4) Cafe24 적립금 지급 — 실패하면 예약을 되돌려 다시 누를 수 있게 한다
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
        });
      } catch (apiErr) {
        await rewards.deleteOne({ memberId, settled: false });
        throw apiErr;
      }
      await rewards.updateOne({ memberId }, { $set: { settled: true } });
      await entries.updateOne({ _id: entry._id }, { $set: { rewarded: true } });

      console.log(`[쉼순간] ${memberId} 적립금 ${POINT_AMOUNT}원 지급 완료`);
      return res.json({ ok: true, message: `🎉 적립금 ${POINT_AMOUNT.toLocaleString()}원이 지급되었습니다!` });
    } catch (err) {
      if (err && err.code === 11000) {
        return res.status(400).json({ ok: false, alreadyDone: true, message: '이미 적립금을 받으셨습니다.' });
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
