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

const CHIP_EN = {
  sink:    { productEn: 'Yogibo Max bean bag standing upright in a tall, hugging form', colorEn: 'light grey (warm off-white grey)' },
  lean:    { productEn: 'Yogibo Lounger bean bag, a low reclining seat with a raised back', colorEn: 'aqua blue' },
  liedown: { productEn: 'Yogibo Max bean bag laid flat as a long lounging cushion',        colorEn: 'light grey (warm off-white grey)' },
  floor:   { productEn: 'Yogibo Drop bean bag, a low teardrop-shaped floor cushion',       colorEn: 'olive green' },
  myspot:  { productEn: 'Yogibo Mini bean bag, a compact one-person cushion',              colorEn: 'dark grey' },
  hug:     { productEn: 'Yogibo Support, a U-shaped bolster cushion that wraps around the body', colorEn: 'olive green' },
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
  const product = opts.double ? 'Yogibo Double, an extra-wide two-person bean bag' : (en.productEn || chip.product);
  const color = en.colorEn || chip.color;
  const ref = refLabel(refs, 'product');
  return [
    `PRODUCT${ref ? ` (shape and color per ${ref})` : ''}: a ${product}, in the exact color ${chip.hex} (${color}).`,
    'Keep its true shape, softness and folds; it is the single largest object in the frame.',
    'EXACTLY ONE small sewn-in fabric tag on the product, on a visible edge seam, rendered as a plain BLANK white rectangle with no lettering',
    '(the tag is tiny — about 1/15 of the product width). Do not draw any brand wordmark or logo anywhere. No other furniture brands.',
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
    'SCENE: a cozy, beautifully styled Korean living room in the evening — warm floor lamp, sheer curtains, a wooden side table with a mug',
    'of tea, a small plant, a soft rug. The character(s) recline freely on the product with arms relaxed, fully at rest.',
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
  '"count":<number of people>,"minorPresent":true|false,"confidence":"high|medium|low"}',
  'Rules: count every visible person. "presentation" is how the person visually presents (clothing, hair, features) — do not guess identity.',
  '"minorPresent" is true if anyone looks clearly under 14. If the photo has no people, return {"people":[],"count":0,"minorPresent":false,"confidence":"high"}.',
].join(' ');

/** 3단계 — 태그 위치 탐지. 로고 후보정용. */
const TAG_LOCATE_PROMPT = [
  'This is a flat illustration of a person on a Yogibo bean bag. Find the small BLANK white fabric tag sewn on the bean bag\'s edge.',
  'Return STRICT JSON only: {"found":true|false,"cx":<0-1>,"cy":<0-1>,"w":<0-1>,"h":<0-1>,"angle":<degrees>,"confidence":"high|medium|low"}',
  'cx,cy = tag center as fractions of image width/height; w,h = tag size as fractions; angle = tilt of the tag\'s LONG axis in degrees',
  '(0 = horizontal, positive = clockwise, negative = counter-clockwise, range -90..90). If no such tag is visible, return {"found":false}.',
].join(' ');

/** 분석 결과 → 인물 지시. 한복이면 성별 표현·연령에 맞는 옷을 구체적으로. */
function personDirective(analysis, theme, refs) {
  const people = (analysis && Array.isArray(analysis.people)) ? analysis.people : [];
  const photoRef = refLabel(refs, 'photo');
  if (!people.length) {
    return theme === 'hanbok'
      ? 'CHARACTER: one young Korean adult in a modern hanbok (jeogori and chima, soft cream and pastel tones), calm content expression, eyes closed or half-closed, simplified anime-style face.'
      : 'CHARACTER: one young Korean adult in comfortable home clothes, calm content expression, eyes closed or half-closed, simplified anime-style face.';
  }
  const lines = people.slice(0, 3).map((p, i) => {
    const age = { child: 'a child', teen: 'a teenager', adult: 'an adult', senior: 'an older adult' }[p.ageGroup] || 'an adult';
    const pres = p.presentation === 'masculine' ? 'masculine-presenting' : p.presentation === 'feminine' ? 'feminine-presenting' : 'androgynous';
    const feats = [p.hair ? `${p.hair} hair` : '', p.glasses ? 'glasses' : '', p.facialHair && p.facialHair !== 'none' ? `${p.facialHair} facial hair` : '',
      p.build ? `${p.build} build` : '', p.skinTone ? `${p.skinTone} skin tone` : '', p.notableItems].filter(Boolean).join(', ');
    let outfit = 'comfortable home clothes';
    if (theme === 'hanbok') {
      if (p.ageGroup === 'child') outfit = 'a child\'s saekdong hanbok (rainbow-striped sleeves) with a small vest';
      else if (p.presentation === 'masculine') outfit = 'a men\'s hanbok: baji (trousers), jeogori (jacket) and a jokki vest in muted navy, grey and cream tones';
      else if (p.presentation === 'feminine') outfit = 'a women\'s hanbok: a long chima (skirt) and a short jeogori in soft cream and pastel tones';
      else outfit = 'a gender-neutral modern hanbok (durumagi-style overcoat with trousers) in cream and muted tones';
    }
    return `Person ${i + 1}: ${pres} ${age} with ${feats || 'natural features'}, wearing ${outfit}.`;
  });
  return [
    `CHARACTERS (draw exactly ${Math.min(people.length, 3)} people${photoRef ? `, the people shown in ${photoRef}` : ''}):`,
    ...lines,
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
  const double = count >= 2;
  const theme = p.theme === 'hanbok' ? 'hanbok' : 'interior';
  const greeting = p.greeting || pickGreeting();
  const scene = theme === 'hanbok' ? SCENES.hanbok(greeting) : SCENES.interior;
  return {
    prompt: [STYLE, productDirective(p.chip, refs, { double }), mateDirective(refs), personDirective(p.analysis, theme, refs), scene].join(' '),
    greeting: theme === 'hanbok' ? greeting : null,
    double,
  };
}

module.exports = { CHIP_EN, STYLE, GREETINGS, pickGreeting, PHOTO_ANALYSIS_PROMPT, TAG_LOCATE_PROMPT, personDirective, productDirective, mateDirective, buildPrompt };
