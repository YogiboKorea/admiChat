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
 *   · 메이트: 응모마다 다르게 — 없음 30% · 하나 30%(팍스/티렉스) · 둘 40%(팍스+티렉스) (pickMates, 응모 id 시드). 늘 팍스만·늘 둘·100% 등장은 아니게(결정 사항)
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
  max:     '70 cm wide x 45 cm deep x 170 cm long — as long as an adult is tall, but only ONE adult wide (about shoulder width). Stood upright it tops a 160 cm woman by about 10 cm. It seats ONE person; it is NOT a sofa and never wide enough for two side by side',
  lean:    '65 cm wide x 80 cm deep x 60 cm high overall — a one-person low chair whose seat sits on the floor; the raised back reaches a seated adult\'s shoulder blades when they slouch into it. Seat for exactly one person',
  floor:   '85 cm wide x 85 cm deep x 75 cm high — a round droplet about the height of a seated adult\'s shoulders; ONE adult sinks into it with knees bent',
  myspot:  '70 cm wide x 45 cm deep x 85 cm high — compact, about hip-height of a standing adult; a single seat where an adult sits with knees bent, child-friendly size',
  hug:     '76 cm wide x 30 cm deep x 94 cm tall — a U-shaped cushion that wraps around ONE seated adult\'s lower back and arms; its armrests reach hip height when seated. It is a cushion, not a seat',
  double:  '120 cm wide x 45 cm deep x 170 cm long — nearly twice the width of a single Max; two adults can sit or lie side by side',
};

const CHIP_EN = {
  sink:    { productEn: MAX_BODY + ', propped at a reclining angle with the wide end raised so the person sinks back into it — head, shoulders and back fully supported, legs stretched down onto the rug', colorEn: 'light grey (warm off-white grey)', sizeEn: SIZE_EN.max },
  lean:    { productEn: [
    'the Yogibo Lounger: a ONE-person bean bag CHAIR shaped like a soft letter "L" — a flat low seat cushion lying on the floor that',
    'curves up at the back, through one thick rounded corner, into a tall rounded backrest standing about as high as a seated adult\'s',
    'shoulder blades. The person sits low with legs stretched forward along the seat and the whole back resting on the raised part.',
    'It has NO armrests and NO legs; it is NOT a round pouf, NOT a ball, NOT a teardrop, NOT a sofa or armchair — a single soft L-shaped cushion',
  ].join(' '), colorEn: 'aqua blue', sizeEn: SIZE_EN.lean },
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

/** 시드 문자열 → 0 이상의 정수 (FNV-1a). 같은 응모 id 는 재시도해도 같은 값을 낸다. */
function hashSeed(s) {
  let h = 2166136261;
  for (const ch of String(s == null ? '' : s)) { h ^= ch.charCodeAt(0); h = Math.imul(h, 16777619) >>> 0; }
  return h >>> 0;
}

/**
 * 메이트 구성 — 응모마다 다르게: 없음 30% · 하나 30%(팍스/티렉스 번갈아) · 둘 40%(팍스+티렉스).
 * "늘 팍스만, 늘 둘씩, 100% 등장" 은 아니게 — 결정 사항. 티렉스 레퍼런스가 없으면 그 자리는 팍스.
 * 3명 이상이면 최대 하나(화면이 붐빈다). 시드가 없으면(테스트) 팍스 하나.
 * 돌려주는 값: [] | ['fox'] | ['trex'] | ['fox','trex'] | ['fox','fox']
 */
function pickMates(seed, drawn = 1, hasTrex = true) {
  const noSeed = seed == null || seed === '';
  const h = noSeed ? 0 : hashSeed(seed);
  const r = noSeed ? 50 : h % 100;
  let kinds = r < 30 ? [] : r < 60 ? [((h >>> 8) & 1) ? 'trex' : 'fox'] : ['fox', 'trex'];
  if (!hasTrex) kinds = kinds.map(() => 'fox');
  if (drawn >= 3) kinds = kinds.slice(0, 1);
  return kinds;
}

// 실제 제품 그대로 — 팍스: 주황 여우. 티렉스 메이트(테디): 빨간 몸에 남색 배, 흰 지그재그 이빨, 짙은 회색 발.
const FOX_DESC = ref => `the Fox Mate${ref ? ` — match ${ref}` : ''}: round orange body, cream belly and muzzle, small dark-grey paws and ear tips, simple dot eyes`;
const TREX_DESC = ref => `the T-Rex Mate${ref ? ` — match ${ref}` : ''}: a chubby plush RED T-Rex dinosaur sitting upright on its tail, with a big NAVY-BLUE oval belly patch, a wide friendly open mouth lined with a row of small white zigzag felt teeth, tiny arms, dark-grey feet and simple dot eyes`;

/**
 * 메이트(플러시 캐릭터)는 빈백이 아니라 태그도 로고도 없다 — 1~2개 두어도 로고 파이프라인과 무관하다.
 * kinds: pickMates 결과(배열). 숫자를 주면 옛 방식(팍스 n개). 참조가 안 붙은 종류는 빼고, 남는 게 없으면 금지 문장.
 */
function mateDirective(refs, kinds = ['fox']) {
  const list = Array.isArray(kinds) ? kinds : (kinds >= 2 ? ['fox', 'fox'] : kinds >= 1 ? ['fox'] : []);
  const foxRef = refLabel(refs, 'mate'), trexRef = refLabel(refs, 'mate2');
  const usable = list.filter(k => (k === 'fox' && foxRef) || (k === 'trex' && trexRef)).slice(0, 2);
  if (!usable.length) {
    return 'NO MASCOTS: do not add any plush toys, mascot characters, dolls or stuffed animals anywhere in the scene — only the people, the bean bag and the room props.';
  }
  const desc = k => (k === 'fox' ? FOX_DESC(foxRef) : TREX_DESC(trexRef));
  if (usable.length === 1) {
    return [
      `MATE (exactly ONE plush Yogibo Mate character in the scene): ${desc(usable[0])}.`,
      'It sits beside or leans on the character(s), clearly visible and complete, drawn in the same flat style. Never omit it.',
      'Mates are plush toys, not bean bags. No other plush toys or mascots.',
    ].join(' ');
  }
  if (usable[0] === usable[1]) {
    return [
      `MATE (match ${foxRef}): a plush orange fox character (Yogibo Mate Fox) — round orange body, cream belly and muzzle,`,
      'small dark-grey paws and ear tips, simple dot eyes. It MUST appear in the scene, sitting beside or leaning on the character(s),',
      'clearly visible and complete, drawn in the same flat style. Never omit it.',
      'Add a SECOND Yogibo Mate plush of the same fox design, smaller and further away — sitting on the shelf, on the rug in the foreground, or by the window — so there are exactly TWO Mates in the room. Mates are plush toys, not bean bags.',
    ].join(' ');
  }
  return [
    'MATES (exactly TWO plush Yogibo Mate characters, BOTH sitting right beside the character(s) on or against the bean bag, clearly visible and complete, drawn in the same flat style):',
    `(1) ${desc('fox')}.`,
    `(2) ${desc('trex')}, a little bigger than the fox.`,
    'Exactly these TWO Mates, one on each side of the character(s) or side by side; never omit either, never merge them, no other plush toys. Mates are plush toys, not bean bags.',
  ].join(' ');
}

/**
 * 배경 — 늘 같은 "밤 거실" 이 지루하다(결정 사항). 두 단계:
 *   ① 고객 문장 → 배경 브리프 (gpt-4.1-mini 텍스트, SCENE_BRIEF_PROMPT). 문장이 말하는 장소·시간·소품을 배경으로.
 *   ② 브리프가 없거나 실패하면 시드 풀에서 하나 (pickSetting) — 실내/야외/숲/바다/옥상/캠핑/한옥 … 골고루.
 * 어느 쪽이든 빈백은 평평한 바닥(러그·마루·데크·잔디·매트) 위에, 인물은 그 위에 — 그건 SCENE_RULES 가 잡는다.
 */
const SCENE_POOL = {
  interior: [
    'a cozy Korean living room at night: a wooden-framed balcony door showing a deep blue evening, sheer cream curtains, a slim tripod floor lamp pouring warm amber light, a low wooden shelf with a potted plant and a woven basket, a thick cream shag rug, a small round table with a mug of tea and two books',
    'a bright living room on a Sunday morning: tall windows with soft white daylight, linen curtains lifting in a breeze, hanging plants, a light oak floor with a woven jute rug, a tray with toast and coffee, a cat asleep in a sunbeam',
    'an apartment balcony at sunset: the bean bag on an outdoor rug between potted olive trees and string lights, a low table with iced tea, the city skyline glowing orange and pink beyond the railing',
    'a quiet forest clearing in early autumn afternoon: the bean bag on a checked picnic blanket, tall pines and a few maples turning orange, dappled sunlight, a small thermos and a book, soft moss and fallen leaves around',
    'a park lawn under a big zelkova tree on a clear afternoon: the bean bag on a picnic mat, a wicker basket with fruit, a bicycle leaning nearby, gentle hills and a pond in the distance, high blue sky',
    'a calm beach at golden hour: the bean bag on a straw mat on pale sand, gentle waves and a long shadow, a straw hat and a pair of sandals, sea-grass on a low dune behind',
    'a rooftop at night: the bean bag on a wooden deck with warm string lights overhead, a little herb garden in crates, a lantern, and the city lights and a navy sky with a few stars beyond the parapet',
    'a lakeside campsite at dusk: the bean bag on a camping rug beside a small tent, a hanging lantern and a tiny campfire, pine trees and a violet-orange sky mirrored in the still water',
    'a bedroom on a rainy afternoon: the bean bag by a big window with raindrops, soft grey-blue light, a linen bed with rumpled cream bedding, a bedside lamp glowing warm, a mug of cocoa on the sill',
    'the wooden porch (maru) of a hanok on a clear autumn day: the bean bag on the warm wooden floor, a courtyard with a persimmon tree heavy with fruit, clay pots, sunlight sliding under the tiled eaves',
    'a corner of a small neighbourhood bookshop cafe in the evening: the bean bag on a worn rug between tall bookshelves, a green banker lamp, a cup of latte, rain-streaked window with warm street light outside',
    'a countryside field of pampas grass and cosmos at sunset in early autumn: the bean bag on a picnic mat on a grassy rise, a winding path, low hills, the sky peach and lavender',
    'a riverside walkway at dusk: the bean bag on a wooden deck by the water, paper lanterns on a railing, reflections of a bridge, a warm breeze bending the reeds, a bag of roasted chestnuts',
  ],
  hanbok: [
    'the wooden porch (maru) of a hanok at night: the bean bag on the warm wooden floor, a courtyard with a persimmon tree, clay jars, a paper lantern glowing, and the huge full moon rising over the tiled roof',
    'a cozy living room on Chuseok night: through a big window the huge full yellow moon in a deep navy sky, a pine branch at the window edge, warm lamp light inside, a low table with a tray of songpyeon and pears',
    'a grassy hillside on Chuseok night: the bean bag on a picnic mat, silver pampas grass swaying, a small village with warm windows below, and the enormous full moon filling the sky',
    'an apartment rooftop on Chuseok night: the bean bag on a wooden deck, string lights and a lantern, a tray of songpyeon on a low table, the city skyline and the huge full moon low and golden',
    'a hanok courtyard on Chuseok night: the bean bag on a woven mat on the stone yard, jangdok clay jars, a mulberry tree, lanterns on the gate, the full moon bright above the roofline',
    'a lakeside deck on Chuseok night: the bean bag on a wooden pier, a paper lantern, still water mirroring the huge full moon, distant mountains, a tray of songpyeon and chestnuts',
  ],
};

/** 시드로 풀에서 하나 — 같은 응모는 재시도해도 같은 배경. 시드가 없으면(테스트) 첫 번째(밤 거실). */
function pickSetting(seed, theme) {
  const pool = SCENE_POOL[theme === 'hanbok' ? 'hanbok' : 'interior'];
  if (seed == null || seed === '') return pool[0];
  return pool[hashSeed(String(seed) + ':scene') % pool.length];
}

/** 브리프에서 온 배경 문장 정리 — 한 줄, 제어문자 제거, 너무 짧으면 버린다. */
function sanitizeSetting(v) {
  if (v == null) return null;
  let s = String(v).replace(/[\x00-\x1F\x7F]+/g, ' ').replace(/\s+/g, ' ').trim();
  if (s.length < 25) return null;
  if (s.length > 700) s = s.slice(0, 700);
  return s;
}

// 칩(쉬는 방식) → 브리프에 넣는 영어 힌트. 장소·소품이 그 쉬는 방식에 맞게 (눕는 사람이면 누울 만한 곳, 바닥형이면 바닥 생활 공간).
const REST_HINT_EN = {
  sink:    'sinking deep into the bean bag until the day lets go',
  lean:    'leaning back with the whole back supported',
  liedown: 'lying down full length, half asleep',
  floor:   'sitting low on the floor, close to the ground',
  myspot:  'having one small spot that is only theirs',
  hug:     'hugging something soft until sleep comes',
};

/** ① 고객 문장 + 쉬는 방식(칩) → 배경 브리프. 텍스트만 보내는 작은 호출 (≈ $0.0003). JSON 만 받는다. */
function sceneBriefPrompt(sentence, theme, chip) {
  const s = String(sentence || '').replace(/[\x00-\x1F\x7F"]+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 120);
  const hanbok = theme === 'hanbok';
  const key = chip && chip.key;
  const restLine = key && REST_HINT_EN[key]
    ? `They chose how they rest: "${String(chip.type || key).replace(/"/g, '')}" — ${REST_HINT_EN[key]}. Let the place, the light and the props suit that way of resting.`
    : '';
  return [
    'You design the backdrop for one flat-illustration poster. A customer wrote, in Korean, the moment they most wanted to rest today:',
    `"${s}"`,
    restLine,
    'Turn that moment into ONE calm, restful scene where a large Yogibo bean bag can sit on a flat surface.',
    'Return STRICT JSON and nothing else: {"setting":"<45-80 English words>","indoor":true|false,"time":"morning|day|sunset|night"}',
    '"setting" must name the place, the time of day, the light source and colour of the light, weather if any, and 3-5 concrete props that',
    'echo the sentence (a work bag dropped by the door, exam papers on the floor, a wet umbrella, a half-finished mug…).',
    'The bean bag must rest on a rug, wooden floor, deck, porch, grass, picnic mat or sand mat — never on stairs, water, a bed or a vehicle seat.',
    'If the sentence names a place where a big bean bag would be absurd (subway, bus, car, office desk, classroom, bathroom, kitchen counter),',
    'draw the rest that comes AFTER it — back home, a rooftop, a park lawn, a quiet cafe corner — and keep only a small prop as a hint of the original place.',
    'Vary freely: living room, bedroom, balcony, rooftop, hanok porch, forest clearing, park lawn, riverside, beach, campsite, cafe, countryside field.',
    'Choose what fits the sentence best. If the sentence gives no place at all, pick one that fits its mood and is NOT a night-time living room.',
    'Season: early autumn (September) in Korea. Safe and gentle for all ages. No other people in the description, no text, no brand names.',
    hanbok ? 'It is Chuseok (Korean harvest festival) NIGHT: the setting must include a huge full moon clearly visible (in the sky or through a window); indoors or outdoors both fine (hanok courtyard, wooden porch, hillside, rooftop, living-room window).' : '',
  ].filter(Boolean).join(' ');
}

const SCENE_RULES = [
  'The product sits on that flat surface and stays the single largest object; the character(s) rest ON it as described.',
  'Compose a vertical poster with clear depth: the setting behind, the product in the middle ground, small props low and near.',
].join(' ');

const SCENES = {
  interior: (setting) => [
    `SCENE: ${setting || SCENE_POOL.interior[0]}.`,
    'The character(s) recline freely on the product with arms relaxed and legs stretched out, fully at rest.',
    'Mood: quiet, warm, exhaling after a long day.', SCENE_RULES,
    'NO TEXT of any kind — no letters, numbers, captions, logos or watermarks.',
  ].join(' '),
  hanbok: (greeting, setting) => [
    `SCENE (Chuseok, the Korean harvest festival, at night): ${setting || SCENE_POOL.hanbok[1]}.`,
    'A huge full yellow moon must be clearly visible — in a deep navy sky with a few stars, or through a window. A small wooden tray with',
    'songpyeon rice cakes and pears sits near the product. Warm lamp or lantern light near, cool moonlight far. Mood: abundant, peaceful holiday rest.',
    SCENE_RULES,
    `TEXT: in the upper part of the image, over the night sky, typeset the Korean greeting "${greeting}" in a warm, friendly rounded`,
    'Korean display typeface, cream/pale-yellow color, large and perfectly legible — exactly these characters and nothing else.',
    'Keep the greeting inside the top 28% of the image so it survives cropping. No other text, no logos, no watermark.',
  ].join(' '),
};

// 인사말은 하나로 고정 (결정 사항). 다시 랜덤으로 돌리려면 목록을 늘리면 된다.
const GREETINGS = ['풍요로운 한가위 되세요'];
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
// 전부 1명. 맥스에 둘을 올리면 모델이 빈백을 소파만 하게 부풀려 가로로 나란히 앉힌다(테스트 2회 실측) — 치수를 글로 줘도 안 잡힌다.
// 둘째부터는 빈백 옆 러그에 기대 앉는다. 둘이 붙어 앉는 컷이 꼭 필요하면 sink/liedown 을 2 로 올린다(-d 포즈 컷은 남겨둠).
const SEATS = { sink: 1, liedown: 1, lean: 1, floor: 1, myspot: 1, hug: 1 };
const MAX_PEOPLE = 4;                 // 5명 이상은 앞 4명까지. 조용히 지우지 않고 호출부가 omitted 로 기록한다.

/** 주변 자리. 세로 프레임이라 좌우로 늘어세우지 않고 앞뒤(깊이)로 나눈다 — 얼굴이 서로 가리지 않게. */
const AROUND_SPOTS = [
  'sitting on the rug immediately to the LEFT of the bean bag, leaning back against its side',
  'sitting cross-legged on the rug in the FOREGROUND, nearer the viewer and lower in the frame',
  'sitting on the rug to the RIGHT of the bean bag, knees drawn up, one elbow resting on it',
];

/** 사람 키 기준. 제품 치수(SIZE_EN)와 같은 자로 그리게 한다 — 남 175 · 여 162 (지정값), 청소년·아이는 비례. */
const BODY_SCALE = 'BODY SCALE: masculine-presenting adults are about 175 cm tall, feminine-presenting adults about 162 cm, teenagers about 160 cm, children about 115 cm. Size every person with these heights and size the product with its dimensions above, on the same scale — a 170 cm Max is about as long as the man is tall, a 60 cm Lounger reaches his knee.';

/** 태그 검증 — 후보 주변 좁은 크롭에 대해 묻는다. 양말·옷깃·노리개·컵을 걸러내는 마지막 관문. */
const TAG_VERIFY_PROMPT = [
  'This is a small zoomed-in crop from a flat illustration of a person resting on a Yogibo bean bag.',
  'Question: at the CENTER of this crop, is there a small plain BLANK fabric tag (a tiny light rectangle, no lettering) sewn onto the BEAN BAG\'s own fabric?',
  'It counts ONLY if the tag sits on the bean bag / cushion fabric. It does NOT count if the light shape is part of clothing (collar, cuff, sock, stripe, ribbon,',
  'norigae ornament), a plush toy, a cup, a plate, a pillow, a window, or a wall.',
  'Return STRICT JSON only: {"tag":true|false,"what":"<one or two words for what the light shape at the center actually is>"}',
].join(' ');

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
    'There is EXACTLY ONE Yogibo bean bag in the whole image. Do not add a second bean bag, cushion or floor seat. Plush Mate characters appear ONLY if the MATE section above asks for them.',
    around.length ? 'Arrange them in DEPTH, not in a row: the bean bag and whoever is on it sit higher in the frame; the others sit lower and nearer the viewer. Every face stays fully visible and unobstructed, and nobody covers the product fabric tag.' : '',
    'Keep each person\'s perceived gender presentation, age group, hair, glasses and build EXACTLY as described — never swap, add or "correct" them.',
    photoRef ? `Use ${photoRef} only for who the people are; do NOT copy its background, furniture, clothing or photo look.` : '',
    'Faces are stylized anime-style characters, not photorealistic likenesses; friendly, calm, eyes closed or half-closed.',
  ].filter(Boolean).join(' ');
}

/**
 * @param {object} p { theme, chip:{key,product,color,hex,...}, analysis?, greeting?, refs:['product','mate'?,'mate2'?,'photo'?], mates?, seed? }
 *   mates 를 주면 그대로, 없으면 seed 로 pickMates. refs 에 'mate' 가 없으면 메이트 0 으로 본다(참조 없이 그리게 하지 않는다).
 */
function buildPrompt(p) {
  const refs = Array.isArray(p.refs) ? p.refs : ['product', 'mate'];
  const count = p.analysis && p.analysis.count ? p.analysis.count : 1;
  const drawn = Math.max(1, Math.min(count, MAX_PEOPLE));
  const seated = Math.min(SEATS[p.chip && p.chip.key] || 1, drawn);
  const theme = p.theme === 'hanbok' ? 'hanbok' : 'interior';
  const greeting = p.greeting || pickGreeting();
  // 배경 — 브리프(p.setting)가 있으면 그것, 없으면 시드 풀. 어느 쪽이든 정리(sanitizeSetting)를 거친다.
  const setting = sanitizeSetting(p.setting) || pickSetting(p.seed, theme);
  const scene = theme === 'hanbok' ? SCENES.hanbok(greeting, setting) : SCENES.interior(setting);
  // 메이트 — 배열(['fox','trex'] 등)이 정식. 숫자는 옛 방식(팍스 n개). 없으면 시드로 추첨. 참조가 안 붙은 종류는 뺀다.
  let kinds = Array.isArray(p.mates) ? p.mates.slice()
    : typeof p.mates === 'number' ? (p.mates >= 2 ? ['fox', 'fox'] : p.mates >= 1 ? ['fox'] : [])
    : pickMates(p.seed, drawn, refs.indexOf('mate2') >= 0);
  kinds = kinds.filter(k => (k === 'fox' && refs.indexOf('mate') >= 0) || (k === 'trex' && refs.indexOf('mate2') >= 0));
  if (drawn >= 3) kinds = kinds.slice(0, 1);
  return {
    prompt: [STYLE, productDirective(p.chip, refs), mateDirective(refs, kinds), personDirective(p.analysis, theme, refs, p.chip), scene].join(' '),
    greeting: theme === 'hanbok' ? greeting : null,
    double: seated >= 2,              // 제품 위 2인 (연인 컷)
    mates: kinds.length,              // 실제로 지시한 메이트 수 (0~2)
    mateKinds: kinds,                 // ['fox'] | ['trex'] | ['fox','trex'] | ['fox','fox'] | []
    setting,                          // 실제로 쓴 배경 문장
    settingSource: sanitizeSetting(p.setting) ? 'brief' : 'pool',
    drawn,
    omitted: Math.max(0, count - MAX_PEOPLE),
  };
}

module.exports = { CHIP_EN, SIZE_EN, STYLE, SEATS, MAX_PEOPLE, GREETINGS, SCENE_POOL, pickGreeting, pickMates, pickSetting, sanitizeSetting, sceneBriefPrompt, hashSeed, PHOTO_ANALYSIS_PROMPT, TAG_LOCATE_PROMPT, TAG_VERIFY_PROMPT, personDirective, productDirective, mateDirective, buildPrompt };
