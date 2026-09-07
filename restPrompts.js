/**
 * 「나의 쉼 순간」 생성 프롬프트 (서버). gpt-image-2 편집 API + 비전 분석용.
 *
 * 흐름
 *   1) analysis  : 고객 사진이 있으면 비전 모델로 먼저 읽는다 (PHOTO_ANALYSIS_PROMPT → JSON).
 *                  성별 표현·인원·연령대·머리·안경·체형만 — 생성 모델에 사진을 그냥 던지면 성별·인원이 뒤집힌다. 특히 한복.
 *   2) buildPrompt: 테마(interior | hanbok) + 칩(제품·색) + 분석 결과 + 인사말 → 최종 프롬프트.
 *                  참조 이미지 순서(refs)를 받아 "reference image #N" 번호를 맞춰 쓴다.
 *   3) TAG_LOCATE_PROMPT: 생성본에서 무지 태그 위치를 찾아 진짜 로고를 얹는 후보정용.
 *
 * 규칙
 *   · 스타일: 추석 Week 비주얼 계열의 한국 애니메이션 풍 평면 일러스트 (글로 고정 — 스타일 이미지는 붙이지 않는다)
 *   · 제품: 칩이 정한 제품·색 고정. 태그는 무지(글자 없음) — 로고는 후보정
 *   · 메이트: 반드시 1개 이상
 *   · 인원 2명 이상 → 더블
 *   · 한복 테마는 인사말을 그림 안에 직접 조판 (gpt-image-2 는 한글이 된다)
 */

// 맥스 몸체 — 승인된 레퍼런스 컷의 형태를 그대로 굳힌다(사람 키만 한 물방울, 뒤로 기대 눕는 자세).
const MAX_BODY = [
  'the Yogibo Max: ONE very large teardrop-shaped bean bag, about as long as a person is tall — a broad rounded top that tapers to a',
  'narrower base, filled soft so it slumps and moulds to the body, with wide gentle creases where the weight presses in',
].join(' ');

const CHIP_EN = {
  sink:    { productEn: MAX_BODY + ', propped at a reclining angle with the wide end raised so the person sinks back into it — head, shoulders and back fully supported, legs stretched down onto the rug', colorEn: 'light grey (warm off-white grey)' },
  lean:    { productEn: 'the Yogibo Lounger bean bag, a low reclining seat with a raised back', colorEn: 'aqua blue' },
  liedown: { productEn: MAX_BODY + ', laid flat on the floor as a long mattress-like cushion with the person lying full length along it', colorEn: 'light grey (warm off-white grey)' },
  floor:   { productEn: 'the Yogibo Drop bean bag, a low teardrop-shaped floor cushion',   colorEn: 'olive green' },
  myspot:  { productEn: 'the Yogibo Mini bean bag, a compact one-person cushion',          colorEn: 'dark grey' },
  hug:     { productEn: 'the Yogibo Support, a U-shaped bolster cushion that wraps around the body', colorEn: 'olive green' },
};

const STYLE = [
  'STYLE (follow exactly): Korean commercial animation-style flat illustration, like a premium Korean brand\'s holiday key visual.',
  'Bold clean line art with slightly varied line weight; flat cel shading with 2-3 tones per surface; no gradients except a soft glow',
  'around lamps or the moon; rich saturated palette (deep navy, warm yellow, lavender, orange, cream) kept harmonious; simplified but',
  'charming faces (small nose, soft closed-eye smile); cozy props drawn as clean shapes. Not photorealistic, no painterly brush texture,',
  'no 3D-render look. Vertical poster composition: the product large and central, the character(s) resting ON the product.',
].join(' ');

function refLabel(refs, kind) {
  const i = refs.indexOf(kind);
  return i < 0 ? null : `reference image #${i + 1}`;
}

function productDirective(chip, refs, opts = {}) {
  const en = CHIP_EN[chip.key] || {};
  const product = opts.double ? 'the Yogibo Double, an extra-wide two-person bean bag' : (en.productEn || `the Yogibo ${chip.product}`);
  const color = en.colorEn || chip.color;
  const ref = refLabel(refs, 'product');
  return [
    `PRODUCT${ref ? ` (shape and color per ${ref})` : ''}: ${product}, in the exact color ${chip.hex} (${color}).`,
    'Keep its true shape, softness and folds; it is the single largest object in the frame.',
    'EXACTLY ONE small sewn-in fabric tag on the product, on a visible edge seam in the upper third of its silhouette, lying flat against the',
    'fabric and facing the viewer, rendered as a plain BLANK cream-white rounded rectangle with NO lettering — slightly taller than it is wide',
    '(about 1/15 of the product width), its face clean and evenly lit so it reads as one flat shape.',
    'Do not draw any brand wordmark or logo anywhere. No other furniture brands.',
  ].join(' ');
}

function mateDirective(refs) {
  const ref = refLabel(refs, 'mate');
  return [
    `MATE${ref ? ` (match ${ref})` : ''}: a plush orange fox character (Yogibo Mate Fox) — round orange body, cream belly and muzzle,`,
    'small dark-grey paws and ear tips, simple dot eyes. It MUST appear in the scene, sitting beside or leaning on the character(s),',
    'clearly visible and complete, drawn in the same flat style. Never omit it.',
  ].join(' ');
}

const SCENES = {
  interior: [
    'SCENE: a cozy, beautifully styled Korean living room at night. Behind the product: a wooden-framed window or balcony door showing a',
    'dark blue evening outside, sheer cream curtains, a slim wooden tripod floor lamp casting a warm amber pool of light, and a low wooden',
    'shelf with a potted plant and a woven basket. On the floor: a thick cream shag rug, a small round wooden table with a mug of tea and a',
    'couple of books. The character(s) recline freely on the product with arms relaxed and legs stretched out, fully at rest.',
    'Mood: quiet, warm, exhaling after a long day. NO TEXT of any kind — no letters, numbers, captions, logos or watermarks.',
  ].join(' '),
  hanbok: (greeting) => [
    'SCENE: Chuseok (Korean harvest festival) night. Through a window: a huge full yellow moon in a deep navy sky with a few stars, a',
    'persimmon or pine branch at the window edge. On the floor a small wooden tray with songpyeon rice cakes and pears, and a cup of tea.',
    'Warm lamp light inside, cool moonlight outside. Mood: abundant, peaceful holiday rest.',
    `TEXT: in the upper part of the image, over the navy night sky, typeset the Korean greeting "${greeting}" in a warm, friendly rounded`,
    'Korean display typeface, cream/pale-yellow color, large and perfectly legible — exactly these characters and nothing else.',
    'Keep the greeting inside the top 28% of the image so it survives cropping. No other text, no logos, no watermark.',
  ].join(' '),
};

const GREETINGS = ['풍요로운 한가위 되세요', '넉넉한 한가위 보내세요', '보름달처럼 꽉 찬 한가위', '마음까지 둥근 한가위', '따뜻한 한가위 되세요'];
const pickGreeting = () => GREETINGS[Math.floor(Math.random() * GREETINGS.length)];

/** 1단계 — 사진 분석. 그리는 데 필요한 것만 묻고, 이 요청 안에서만 쓴다. */
const PHOTO_ANALYSIS_PROMPT = [
  'You are preparing an illustration brief. Look at the photo and describe the people ONLY as needed to draw them as stylized characters.',
  'Return STRICT JSON with this shape and nothing else:',
  '{"people":[{"presentation":"masculine|feminine|ambiguous","ageGroup":"child|teen|adult|senior","hair":"<length, style, color in a few words>",',
  '"glasses":true|false,"facialHair":"none|light|full","build":"slim|average|sturdy","skinTone":"light|medium|deep","notableItems":"<hat, headband, etc. or empty>"}],',
  '"count":<number of people>,"primaryIndex":<index into people>,"minorPresent":true|false,"confidence":"high|medium|low"}',
  'ORDER the "people" array strictly LEFT to RIGHT as they appear in the photo — index 0 is the leftmost person. This order decides where each person is placed, so keep it exact.',
  '"primaryIndex" is the most prominent person — largest in frame, nearest the camera, or most centered. If they are all equal, use 0.',
  'Rules: count every visible person. "presentation" is how the person visually presents (clothing, hair, features) — do not guess identity.',
  '"minorPresent" is true if anyone looks clearly under 14. If the photo has no people, return {"people":[],"count":0,"primaryIndex":0,"minorPresent":false,"confidence":"high"}.',
].join(' ');

/** 3단계 — 태그 위치 탐지. 로고 후보정용. */
const TAG_LOCATE_PROMPT = [
  'This is a flat illustration of a person on a Yogibo bean bag. Find the small BLANK white fabric tag sewn on the BEAN BAG\'s edge —',
  'a tiny plain white/cream rectangle on the bean bag fabric itself. It is NOT on clothing, NOT on the plush toy, NOT a pillow or a cup.',
  'If the only white rectangles you see are on clothing or props, return {"found":false}.',
  'Return STRICT JSON only: {"found":true|false,"cx":<0-1>,"cy":<0-1>,"w":<0-1>,"h":<0-1>,"angle":<degrees>,"confidence":"high|medium|low"}',
  'cx,cy = tag center as fractions of image width/height; w,h = tag size as fractions; angle = tilt of the tag\'s LONG axis in degrees',
  '(0 = horizontal, positive = clockwise, negative = counter-clockwise, range -90..90). If no such tag is visible, return {"found":false}.',
].join(' ');

/**
 * 제품 위에 앉힐 수 있는 정원. 실측 가로폭 기준 — 맥스(70cm)는 연인이 붙어 앉는 실사용 컷이 있어 2명까지.
 * 넘치는 인원은 제품에 태우지 않고 주변에 둔다. 빈백은 정원이 있는 물건이라 억지로 태우면 팔다리가 뭉갠다.
 */
const SEATS = { sink: 2, liedown: 2, lean: 1, floor: 1, myspot: 1, hug: 1 };
const MAX_PEOPLE = 4;                 // 5명 이상은 앞 4명까지. 조용히 지우지 않고 호출부가 omitted 로 기록한다.

/** 주변 자리. 세로 프레임이라 좌우로 늘어세우지 않고 앞뒤(깊이)로 나눈다 — 얼굴이 서로 가리지 않게. */
const AROUND_SPOTS = [
  'sitting on the rug immediately to the LEFT of the bean bag, leaning back against its side',
  'sitting cross-legged on the rug in the FOREGROUND, nearer the viewer and lower in the frame',
  'sitting on the rug to the RIGHT of the bean bag, knees drawn up, one elbow resting on it',
];

/** 분석 결과 → 인물 지시. 한복이면 성별 표현·연령에 맞는 옷을 구체적으로. */
function personDirective(analysis, theme, refs, chip) {
  const all = (analysis && Array.isArray(analysis.people)) ? analysis.people : [];
  const people = all.slice(0, MAX_PEOPLE);
  const photoRef = refLabel(refs, 'photo');
  if (!people.length) {
    return theme === 'hanbok'
      ? 'CHARACTER: one young Korean adult in a modern hanbok (jeogori and chima, soft cream and pastel tones), calm content expression, eyes closed or half-closed, simplified anime-style face.'
      : 'CHARACTER: one young Korean adult in comfortable home clothes, calm content expression, eyes closed or half-closed, simplified anime-style face.';
  }
  // 주인공은 제품 위에. 정원 2인 제품(맥스)이고 인원이 2명 이상이면 옆사람까지 붙여 앉힌다.
  const seats = Math.min(SEATS[chip && chip.key] || 1, people.length);
  let primary = Number(analysis && analysis.primaryIndex);
  if (!(primary >= 0 && primary < people.length)) primary = 0;
  const onProduct = [primary];
  for (let d = 1; onProduct.length < seats && d <= people.length; d++) {
    if (primary - d >= 0 && onProduct.length < seats) onProduct.push(primary - d);
    if (primary + d < people.length && onProduct.length < seats) onProduct.push(primary + d);
  }
  onProduct.sort((a, b) => a - b);
  const around = people.map((_, i) => i).filter(i => onProduct.indexOf(i) < 0);
  const ordinal = i => ['1st', '2nd', '3rd', '4th'][i] || (i + 1) + 'th';

  const lines = people.map((p, i) => {
    const age = { child: 'child', teen: 'teenager', adult: 'adult', senior: 'older adult' }[p.ageGroup] || 'adult';
    const pres = p.presentation === 'masculine' ? 'masculine-presenting' : p.presentation === 'feminine' ? 'feminine-presenting' : 'androgynous';
    const art = /^[aeiou]/i.test(pres) ? 'an' : 'a';
    const feats = [p.hair ? `${p.hair} hair` : '', p.glasses ? 'glasses' : '', p.facialHair && p.facialHair !== 'none' ? `${p.facialHair} facial hair` : '',
      p.build ? `${p.build} build` : '', p.skinTone ? `${p.skinTone} skin tone` : '', p.notableItems].filter(Boolean).join(', ');
    let outfit = 'comfortable home clothes';
    if (theme === 'hanbok') {
      if (p.ageGroup === 'child') outfit = 'a child\'s saekdong hanbok (rainbow-striped sleeves) with a small vest';
      else if (p.presentation === 'masculine') outfit = 'a men\'s hanbok: baji (trousers), jeogori (jacket) and a jokki vest in muted navy, grey and cream tones';
      else if (p.presentation === 'feminine') outfit = 'a women\'s hanbok: a long chima (skirt) and a short jeogori in soft cream and pastel tones';
      else outfit = 'a gender-neutral modern hanbok (durumagi-style overcoat with trousers) in cream and muted tones';
    }
    const spot = onProduct.indexOf(i) >= 0
      ? (onProduct.length > 1
          ? 'reclining ON the bean bag, side by side with the other person, shoulders touching, both sunk comfortably into it'
          : 'reclining ON the bean bag, sunk into it, fully supported and at rest')
      : AROUND_SPOTS[around.indexOf(i) % AROUND_SPOTS.length];
    return `Person ${i + 1} (${ordinal(i)} from the left in the photo): ${art} ${pres} ${age} with ${feats || 'natural features'}, wearing ${outfit} — ${spot}.`;
  });
  return [
    `CHARACTERS (draw exactly ${people.length} ${people.length === 1 ? 'person' : 'people'}${photoRef ? `, the people shown in ${photoRef}` : ''}):`,
    ...lines,
    `STAGING: exactly ${people.length} ${people.length === 1 ? 'person' : 'people'} in the frame — ${onProduct.length} on the bean bag, ${around.length} around it on the rug. Add NOBODY else.`,
    'There is EXACTLY ONE Yogibo product in the whole image. Do not add a second bean bag, cushion or any other Yogibo item.',
    around.length ? 'Arrange them in DEPTH, not in a row: the bean bag and whoever is on it sit higher in the frame; the others sit lower and nearer the viewer. Every face stays fully visible and unobstructed, and nobody covers the product fabric tag.' : '',
    'Keep each person\'s perceived gender presentation, age group, hair, glasses and build EXACTLY as described — never swap, add or "correct" them.',
    photoRef ? `Use ${photoRef} only for who the people are; do NOT copy its background, furniture, clothing or photo look.` : '',
    'Faces are stylized anime-style characters, not photorealistic likenesses; friendly, calm, eyes closed or half-closed.',
  ].filter(Boolean).join(' ');
}

/**
 * @param {object} p { theme, chip:{key,product,color,hex,...}, analysis?, greeting?, refs:['product','mate','photo'?] }
 */
function buildPrompt(p) {
  const refs = Array.isArray(p.refs) ? p.refs : ['product', 'mate'];
  const count = p.analysis && p.analysis.count ? p.analysis.count : 1;
  const drawn = Math.max(1, Math.min(count, MAX_PEOPLE));
  const seated = Math.min(SEATS[p.chip && p.chip.key] || 1, drawn);
  const theme = p.theme === 'hanbok' ? 'hanbok' : 'interior';
  const greeting = p.greeting || pickGreeting();
  const scene = theme === 'hanbok' ? SCENES.hanbok(greeting) : SCENES.interior;
  return {
    prompt: [STYLE, productDirective(p.chip, refs), mateDirective(refs), personDirective(p.analysis, theme, refs, p.chip), scene].join(' '),
    greeting: theme === 'hanbok' ? greeting : null,
    double: seated >= 2,              // 제품 위 2인 (연인 컷)
    drawn,
    omitted: Math.max(0, count - MAX_PEOPLE),
  };
}

module.exports = { CHIP_EN, STYLE, SEATS, MAX_PEOPLE, GREETINGS, pickGreeting, PHOTO_ANALYSIS_PROMPT, TAG_LOCATE_PROMPT, personDirective, productDirective, mateDirective, buildPrompt };
