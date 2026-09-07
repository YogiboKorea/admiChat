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

// 실제 치수와 사람 대비 크기 — 카탈로그 spec/scalePrompt 그대로. 모델은 이게 없으면 빈백을 소파·침대로 부풀린다(실측).
const SIZE_EN = {
  max:     '70 cm wide x 45 cm deep x 170 cm long — as long as an adult is tall, but only ONE adult wide (about shoulder width). Stood upright it tops a 160 cm woman by about 10 cm. Two people fit only ALONG its length, pressed close — never side by side across it',
  lean:    '65 cm wide x 80 cm deep x 60 cm high — a one-person low chair about knee-height of a standing adult; its backrest reaches a seated adult\'s mid-back. Seat for exactly one person',
  floor:   '85 cm wide x 85 cm deep x 75 cm high — a round droplet about the height of a seated adult\'s shoulders; ONE adult sinks into it with knees bent',
  myspot:  '70 cm wide x 45 cm deep x 85 cm high — compact, about hip-height of a standing adult; a single seat where an adult sits with knees bent, child-friendly size',
  hug:     '76 cm wide x 30 cm deep x 94 cm tall — a U-shaped cushion that wraps around ONE seated adult\'s lower back and arms; its armrests reach hip height when seated. It is a cushion, not a seat',
  double:  '120 cm wide x 45 cm deep x 170 cm long — nearly twice the width of a single Max; two adults can sit or lie side by side',
};

const CHIP_EN = {
  sink:    { productEn: MAX_BODY + ', propped at a reclining angle with the wide end raised so the person sinks back into it — head, shoulders and back fully supported, legs stretched down onto the rug', colorEn: 'light grey (warm off-white grey)', sizeEn: SIZE_EN.max },
  lean:    { productEn: 'the Yogibo Lounger bean bag, a low reclining seat with a raised back', colorEn: 'aqua blue', sizeEn: SIZE_EN.lean },
  liedown: { productEn: MAX_BODY + ', laid flat on the floor as a long mattress-like cushion with the person lying full length along it', colorEn: 'light grey (warm off-white grey)', sizeEn: SIZE_EN.max },
  floor:   { productEn: 'the Yogibo Drop bean bag, a low teardrop-shaped floor cushion',   colorEn: 'olive green', sizeEn: SIZE_EN.floor },
  myspot:  { productEn: 'the Yogibo Mini bean bag, a compact one-person cushion',          colorEn: 'dark grey', sizeEn: SIZE_EN.myspot },
  hug:     { productEn: 'the Yogibo Support, a U-shaped bolster cushion that wraps around the body', colorEn: 'olive green', sizeEn: SIZE_EN.hug },
};

const STYLE = [
  'STYLE (follow exactly): Korean commercial animation-style flat illustration, like a premium Korean brand\'s holiday key visual.',
  'Bold clean line art with slightly varied line weight; flat cel shading with 2-3 tones per surface; no gradients except a soft glow',
  'around lamps or the moon; rich saturated palette (deep navy, warm yellow, lavender, orange, cream) kept harmonious; simplified but',
  'charming faces (small nose, soft closed-eye smile); cozy props drawn as clean shapes. Not photorealistic, no painterly brush texture,',
  'no 3D-render look. Vertical poster composition: the product large and central, the character(s) resting ON the product.',
  'TONE (important): BRIGHT and high-key. Clean luminous colours, clear whites, soft warm light filling the room; skin and fabric stay',
  'bright and clear. Even a night scene stays luminous — deep saturated navy sky, glowing lamp and moon, no murky or desaturated areas.',
  'Never muddy, dim, brownish, grey-washed or gloomy. Think cheerful holiday key visual, not a moody illustration.',
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
    `PRODUCT${ref ? ` (shape and proportions per ${ref})` : ''}: ${product}, in the exact color ${chip.hex} (${color}).`,
    ref ? `Take ONLY the form, proportions and the way it deforms under a body from ${ref}. IGNORE that reference's own colour, its background, its room and any people in it — the product colour is ${chip.hex} and nothing else.` : '',
    'Keep its true shape, softness and folds; it is the single largest object in the frame.',
    `TRUE SCALE (important): the product measures ${en.sizeEn || SIZE_EN.max}. Draw it at REAL size relative to the people — never inflate it into a sofa, a couch or a bed. Where a person is bigger than the product, their body visibly overhangs it and the fabric compresses under them.`,
    'EXACTLY ONE small sewn-in fabric tag on the product, on a visible edge seam in the upper third of its silhouette, lying flat against the',
    'fabric and facing the viewer, rendered as a plain BLANK cream-white rounded rectangle with NO lettering — slightly taller than it is wide',
    '(about 1/15 of the product width), its face clean and evenly lit so it reads as one flat shape.',
    'Do not draw any brand wordmark or logo anywhere. No other furniture brands.',
    ref ? `If ${ref} shows a person using the product, copy HOW the product is used — the angle it is propped at, how it folds under the body, where the weight sits — but never that person's face, clothing or identity.` : '',
  ].join(' ');
}

/** 메이트(플러시 캐릭터)는 빈백이 아니라 태그도 로고도 없다 — 1~2개 두어도 로고 파이프라인과 무관하다. */
function mateDirective(refs, count = 1) {
  const ref = refLabel(refs, 'mate');
  const ref2 = refLabel(refs, 'mate2');
  // 티렉스 레퍼런스가 붙어 있으면 팍스+티렉스 둘 다 캐릭터 옆에. 없으면 팍스 1~2개.
  if (ref2) {
    return [
      `MATES (two plush Yogibo Mate characters, BOTH sitting right beside the character(s) on or against the bean bag, clearly visible and complete, drawn in the same flat style):`,
      `(1) the fox Mate — match ${ref}: round orange body, cream belly and muzzle, small dark-grey paws and ear tips, simple dot eyes.`,
      `(2) the T-Rex Mega Mate — match ${ref2}: a friendly plush green T-Rex dinosaur with a lighter belly, tiny arms, small tail and simple dot eyes, a little bigger than the fox.`,
      'Exactly these TWO Mates, one on each side of the character(s) or side by side; never omit either, never merge them, no other plush toys. Mates are plush toys, not bean bags.',
    ].join(' ');
  }
  const two = count >= 2;
  return [
    `MATE${ref ? ` (match ${ref})` : ''}: a plush orange fox character (Yogibo Mate Fox) — round orange body, cream belly and muzzle,`,
    'small dark-grey paws and ear tips, simple dot eyes. It MUST appear in the scene, sitting beside or leaning on the character(s),',
    'clearly visible and complete, drawn in the same flat style. Never omit it.',
    two
      ? 'Add a SECOND Yogibo Mate plush of the same fox design, smaller and further away — sitting on the shelf, on the rug in the foreground, or by the window — so there are exactly TWO Mates in the room. Mates are plush toys, not bean bags.'
      : 'Exactly ONE Mate in the scene.',
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

/** 사람 키 기준. 제품 치수(SIZE_EN)와 같은 자로 그리게 한다 — 남 175 · 여 162 (지정값), 청소년·아이는 비례. */
const BODY_SCALE = 'BODY SCALE: masculine-presenting adults are about 175 cm tall, feminine-presenting adults about 162 cm, teenagers about 160 cm, children about 115 cm. Size every person with these heights and size the product with its dimensions above, on the same scale — a 170 cm Max is about as long as the man is tall, a 60 cm Lounger reaches his knee.';

/** 분석 결과 → 인물 지시. 한복이면 성별 표현·연령에 맞는 옷을 구체적으로. */
function personDirective(analysis, theme, refs, chip) {
  const all = (analysis && Array.isArray(analysis.people)) ? analysis.people : [];
  const people = all.slice(0, MAX_PEOPLE);
  const photoRef = refLabel(refs, 'photo');
  if (!people.length) {
    return theme === 'hanbok'
      ? 'CHARACTER: one young Korean adult in a modern hanbok (jeogori and chima, soft cream and pastel tones), calm content expression, eyes closed or half-closed, simplified anime-style face. ' + BODY_SCALE
      : 'CHARACTER: one young Korean adult in comfortable home clothes, calm content expression, eyes closed or half-closed, simplified anime-style face. ' + BODY_SCALE;
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
          ? 'reclining ON the bean bag together with the other person, snuggled close ALONG its length (it is only about one person wide — never seated side by side across it), shoulders touching, both sunk into it'
          : 'reclining ON the bean bag, sunk into it, fully supported and at rest')
      : AROUND_SPOTS[around.indexOf(i) % AROUND_SPOTS.length];
    return `Person ${i + 1} (${ordinal(i)} from the left in the photo): ${art} ${pres} ${age} with ${feats || 'natural features'}, wearing ${outfit} — ${spot}.`;
  });
  return [
    `CHARACTERS (draw exactly ${people.length} ${people.length === 1 ? 'person' : 'people'}${photoRef ? `, the people shown in ${photoRef}` : ''}):`,
    ...lines,
    BODY_SCALE,
    `STAGING: exactly ${people.length} ${people.length === 1 ? 'person' : 'people'} in the frame — ${onProduct.length} on the bean bag, ${around.length} around it on the rug. Add NOBODY else.`,
    'There is EXACTLY ONE Yogibo bean bag in the whole image. Do not add a second bean bag, cushion or floor seat. (Yogibo Mate plush characters are allowed as described in MATE.)',
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
    prompt: [STYLE, productDirective(p.chip, refs), mateDirective(refs, drawn >= 3 ? 1 : 2), personDirective(p.analysis, theme, refs, p.chip), scene].join(' '),
    greeting: theme === 'hanbok' ? greeting : null,
    double: seated >= 2,              // 제품 위 2인 (연인 컷)
    mates: drawn >= 3 ? 1 : 2,        // 메이트 수 — 1~2명이면 둘, 3명 이상이면 하나(화면이 붐빈다)
    drawn,
    omitted: Math.max(0, count - MAX_PEOPLE),
  };
}

module.exports = { CHIP_EN, SIZE_EN, STYLE, SEATS, MAX_PEOPLE, GREETINGS, pickGreeting, PHOTO_ANALYSIS_PROMPT, TAG_LOCATE_PROMPT, personDirective, productDirective, mateDirective, buildPrompt };
