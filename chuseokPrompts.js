// ================================================================
// 「추석 AI 사진관」 프롬프트 모듈  (2026.09)
//
//  종류 4가지 — 기획안(추석_AI사진이벤트_기획안모음) 중 확정된 것:
//    card   엄마 아빠 감성 '한가위 축전'   인사말 입력 · 사진 없음
//    studio 요기보 한가위 사진관          사진 2장 → 한 장의 한복 사진
//    pet    우리 집 댕냥이 한복 입히기     반려동물 사진 1장 + 성별
//    moon   달나라 떡방아 알바생           얼굴 사진 1장 → 스노우 느낌 달토끼
//
//  공통 원칙 (기획안 「모든 안에 공통으로 지킬 것」)
//   · 한글은 AI 가 쓰지 않는다. 축전 인사말은 서버가 이미지 위에 직접 새긴다 → 모든 프롬프트에 "글자 금지".
//   · 요기보는 반전 소품으로만 — 메이트 인형 하나가 작게 들어간다. 로고·브랜드 글자 없음.
//   · 결과가 다 비슷해 보이지 않게 — 응모 id 를 시드로 테두리·장면·색을 고른다.
//   · 아이 얼굴은 생성 모델에 보내지 않는다 (「나의 쉼 순간」 2026-09 결정 그대로) — 호출부가 사진 대신 설명만 넘긴다.
// ================================================================
'use strict';

const RP = require('./restPrompts');       // hashSeed · PHOTO_ANALYSIS_PROMPT 재사용

const NO_TEXT = 'ABSOLUTELY NO text, letters, numbers, hangul, captions, logos, signatures or watermarks anywhere in the image.';
const MATE_PROP = 'Somewhere small and secondary (never the focus), include the plush toy from the reference image, wearing a tiny Korean hanbok — keep its shape and colours faithful to the reference.';

const pick = (seed, salt, arr) => arr[RP.hashSeed(String(seed) + ':' + salt) % arr.length];

// ── 1. 한가위 축전 ─────────────────────────────────────────────────
// 인사말은 이미지 위쪽 가운데 띠(세로 14%~44%)에 서버가 새긴다 → 그 띠는 비워 달라고 한다.
const CARD_BORDERS = [
  'a lush border of big glossy pink peonies, red roses and gold chrysanthemums framing the whole card',
  'a sparkly glitter frame with twinkling star bursts, lens flares and shiny sequins around every edge',
  'an ornate embossed gold-foil frame with traditional Korean cloud and knot patterns and red tassels',
  'a festive frame of autumn persimmon branches, maple leaves and golden rice stalks with tiny sparkles',
];
const CARD_PALETTES = [
  'deep royal blue night sky with a warm golden glow',
  'rich purple-to-magenta sunset sky with gold highlights',
  'dark jade green night sky with shimmering gold and red accents',
  'midnight navy sky with rose-pink and gold sparkles',
];
const CARD_MOTIFS = [
  'a plate of colourful songpyeon rice cakes and a basket of persimmons at the bottom',
  'traditional Korean house roofs (hanok) silhouetted under the moon at the bottom',
  'a pair of cute rabbits looking up at the moon beside songpyeon at the bottom',
  'a table of Chuseok fruits — pears, apples, persimmons, chestnuts — at the bottom',
];

function cardPrompt(seed) {
  return [
    'Create a Korean Chuseok (Hangawi) greeting e-card image in the nostalgic style that Korean parents love to send on KakaoTalk:',
    'glossy, festive, a little kitschy on purpose — bright saturated colours, shiny gold, sparkles and glow effects, like a classic mobile greeting card.',
    `Frame: ${pick(seed, 'border', CARD_BORDERS)}.`,
    `Background: ${pick(seed, 'palette', CARD_PALETTES)}, with a huge luminous full moon in the upper part.`,
    `Decoration: ${pick(seed, 'motif', CARD_MOTIFS)}.`,
    'IMPORTANT LAYOUT: keep a wide horizontal band in the upper-middle of the image — from about 14% to 44% of the image height, spanning most of the width — clean and simple (smooth sky or soft moonlight glow, no objects, no flowers, no busy patterns),',
    'because large greeting text will be placed there afterwards. The moon may sit behind that band as a soft glow.',
    MATE_PROP + ' Put it in a lower corner as a cute sticker, bowing politely.',
    NO_TEXT,
  ].join(' ');
}

// ── 2. 한가위 사진관 ───────────────────────────────────────────────
const STUDIO_SETS = [
  'a premium traditional Korean photo studio: a painted folding screen (byeongpung) with a full moon, pine trees and cranes behind them, a low wooden table, silk cushions, soft warm studio lighting',
  'a moonlit hanok courtyard at night: wooden veranda (maru), paper lanterns glowing, a big full moon above the tiled roof, gentle warm light on faces',
  'in front of a grand Korean palace gate at dusk: colourful dancheong eaves, stone steps, soft golden hour light, a faint full moon in the sky',
];
const STUDIO_COLOURS = [
  'soft pastel hanbok colours — blush pink, mint, lavender, cream',
  'rich traditional hanbok colours — deep red, royal blue, jade green, gold trim',
];

function studioPrompt(seed, groups) {
  // groups: [{ source: 'photo'|'brief', label, people:[{presentation, ageGroup, ...}] }]
  const lines = [];
  let refIndex = 0;
  const refs = [];
  groups.forEach((g, i) => {
    if (g.source === 'photo') {
      refIndex++;
      refs.push(`reference photo ${refIndex} shows group ${i + 1}`);
      lines.push(`Group ${i + 1}: every person visible in reference photo ${refIndex}. Keep each face, hairstyle, skin tone, age and build faithful and recognizable.`);
    } else {
      const who = (g.people || []).map(p => {
        const age = { child: 'child', teen: 'teenager', adult: 'adult', senior: 'older adult' }[p.ageGroup] || 'adult';
        const pres = p.presentation === 'masculine' ? 'masculine' : p.presentation === 'feminine' ? 'feminine' : 'androgynous';
        const feats = [p.hair ? p.hair + ' hair' : '', p.glasses ? 'glasses' : '', p.build ? p.build + ' build' : ''].filter(Boolean).join(', ');
        return `a ${pres} ${age}${feats ? ' (' + feats + ')' : ''}`;
      });
      lines.push(`Group ${i + 1} (described, no photo): ${who.join('; ') || 'one adult'}. Children here are generic, not based on any real child.`);
    }
  });
  return [
    'Create ONE photorealistic formal Chuseok group portrait, as if professionally photographed together at a Korean hanbok photo studio.',
    'Bring the people from the groups below into this single photo, posed naturally side by side (some seated in front, some standing behind), looking at the camera with warm smiles, as if they had always been in the same room.',
    ...lines,
    'Dress every person in an elegant, well-fitted Korean hanbok that suits their age and presentation.',
    `Hanbok palette: ${pick(seed, 'colour', STUDIO_COLOURS)}.`,
    `Setting: ${pick(seed, 'set', STUDIO_SETS)}.`,
    'Consistent lighting and colour grading across everyone so it reads as one real photograph; natural skin texture, correct hands, no duplicated or missing people.',
    MATE_PROP + ' Seat it on a small silk cushion at the edge of the group, as if it were a studio prop.',
    refs.length ? `(Reference order: the plush toy is the last reference; ${refs.join('; ')}.)` : '(The only reference image is the plush toy.)',
    NO_TEXT,
  ].join(' ');
}

// ── 3. 댕냥이 한복 ─────────────────────────────────────────────────
const PET_HANBOK = {
  boy: 'a boy-style pet hanbok: a navy or jade jeogori jacket with a contrasting vest (jokki), a small red bokjumeoni lucky pouch, and optionally a tiny traditional boy\'s hat (bokgeon)',
  girl: 'a girl-style pet hanbok: a saekdong rainbow-striped jeogori jacket with a pink or coral chima skirt, a small floral hair ornament or daenggi ribbon',
};
const PET_SCENES = [
  'curled up napping on a big soft bean bag sofa under a huge full moon seen through a window, cozy warm lamp light',
  'sitting politely in front of a low table with a plate of colourful songpyeon rice cakes, looking up hopefully',
  'sitting on a hanok wooden veranda at night, gazing up at a giant glowing full moon as if making a wish, lanterns softly lit',
];

function petPrompt(seed, gender, species) {
  const kind = species === 'cat' ? 'cat' : species === 'dog' ? 'dog' : 'pet';
  return [
    `Create a photorealistic, heart-warming Chuseok portrait of the exact same ${kind} from the reference photo.`,
    'Keep it unmistakably the same animal: same breed, size proportions, fur colour and pattern, markings, eye colour, ear and tail shape.',
    `Dress it in ${PET_HANBOK[gender === 'girl' ? 'girl' : 'boy']}, tailored naturally to its body so it looks comfortable and real (correct animal anatomy, no human hands).`,
    `Scene: ${pick(seed, 'scene', PET_SCENES)}.`,
    'Soft cinematic lighting, shallow depth of field, warm autumn Chuseok mood.',
    MATE_PROP + ' Place it next to the pet, as its little friend.',
    NO_TEXT,
  ].join(' ');
}

// ── 4. 달나라 떡방아 알바생 (스노우 느낌) ──────────────────────────
const MOON_SCENES = [
  'pounding rice cakes (tteok) with a big wooden mallet in a stone mortar, working hard with a determined cute face',
  'taking a break sitting on the edge of a moon crater, holding a freshly made rice cake and giving a peace sign',
  'mid-swing with the wooden mallet, a little flour dust in the air, winking at the camera',
];

function moonPrompt(seed, person) {
  // person: { source:'photo' } 이면 사진의 얼굴을 살린다. { source:'brief', people:[...] } 이면 설명으로만 (아이 등)
  const who = person && person.source === 'photo'
    ? 'the same person from the reference photo — keep their face, hairstyle, skin tone and age faithful and clearly recognizable'
    : 'a cute generic character (not based on any real person) matching this description: ' +
      (((person && person.people) || []).map(p => `${p.presentation || 'ambiguous'} ${p.ageGroup || 'adult'}`).join('; ') || 'a young adult');
  return [
    'Create a playful selfie-style photo in the look of the Korean SNOW beauty-camera app with cute AR filter effects.',
    `Subject: ${who}, wearing fluffy white moon-rabbit ears and a little hanbok-style apron.`,
    `On the surface of a giant glowing full moon, like the Korean legend of the moon rabbit, the subject is ${pick(seed, 'scene', MOON_SCENES)}.`,
    'Filter look: bright soft beauty-filter skin, rosy blush stickers on the cheeks, floating sparkles, tiny star and heart AR stickers, dreamy pastel pink-and-lavender space background with twinkling stars, slightly wide-angle phone-selfie framing.',
    'Keep it fun and cute, not scary; realistic photo of the person with filter overlays on top.',
    MATE_PROP + ' Let it peek out of a small moon crater nearby.',
    NO_TEXT,
  ].join(' ');
}

// ── 사진 확인용 비전 프롬프트 ───────────────────────────────────────
// 사람 사진(사진관·떡방아)은 「나의 쉼 순간」의 분석 프롬프트를 그대로 쓴다 (인원·연령대·14세 미만 여부).
const PEOPLE_ANALYSIS_PROMPT = RP.PHOTO_ANALYSIS_PROMPT;
// 반려동물 사진 — 동물이 실제로 있는지만 본다. 없으면 생성하지 않고(과금 없음) 다시 올려 달라고 한다.
const PET_ANALYSIS_PROMPT = [
  'Look at the photo and answer ONLY with strict JSON:',
  '{"animal":"dog|cat|other|none","count":<number of pets clearly visible>,"confidence":"high|medium|low"}.',
  '"animal" is the main pet. Use "other" for rabbits, birds, hamsters etc. Use "none" if no animal is clearly visible.',
].join(' ');

module.exports = {
  NO_TEXT, cardPrompt, studioPrompt, petPrompt, moonPrompt,
  PEOPLE_ANALYSIS_PROMPT, PET_ANALYSIS_PROMPT,
  CARD_BORDERS, CARD_PALETTES, CARD_MOTIFS, STUDIO_SETS, STUDIO_COLOURS, PET_SCENES, MOON_SCENES, PET_HANBOK,
};
