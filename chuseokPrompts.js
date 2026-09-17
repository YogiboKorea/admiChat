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
//   · 요기보는 반전 소품으로만 — 메이트(팍스·티렉스 중 하나)가 가끔, 작게 들어간다 (pickMate). 로고·브랜드 글자 없음.
//   · 결과가 다 비슷해 보이지 않게 — 응모 id 를 시드로 테두리·장면·색을 고른다.
//   · 아이 얼굴은 생성 모델에 보내지 않는다 (「나의 쉼 순간」 2026-09 결정 그대로) — 호출부가 사진 대신 설명만 넘긴다.
// ================================================================
'use strict';

const RP = require('./restPrompts');       // hashSeed · PHOTO_ANALYSIS_PROMPT 재사용

const NO_TEXT = 'ABSOLUTELY NO text, letters, numbers, hangul, captions, logos, signatures or watermarks anywhere in the image.';

const pick = (seed, salt, arr) => arr[RP.hashSeed(String(seed) + ':' + salt) % arr.length];

// ── 요기보 메이트 (숨은 소품) ───────────────────────────────────────
// 늘 나오지 않는다 (결정 사항 2026-09-15 "무조건 100% 다 노출되는 건 아니고").
// 응모 id 시드로 정한다 — 재시도해도 같은 결과. rate% 확률로 등장하고, 등장하면 팍스·티렉스 반반.
// 등장할 때는 작게(화면 높이의 1/10 안팎), 주인공을 가리지 않는 자리에. 실제 제품 모양·색을 그대로.
const MATE_KINDS = ['fox', 'trex'];
function pickMate(seed, rate = 60) {
  const r = Math.max(0, Math.min(100, Number(rate)));
  const h = RP.hashSeed(String(seed) + ':mate');
  if (h % 100 >= r) return null;
  return MATE_KINDS[(h >>> 8) % MATE_KINDS.length];
}
// 설명은 「나의 쉼 순간」 restPrompts 의 FOX_DESC / TREX_DESC 와 같은 실제 제품 묘사
const MATE_DESC = {
  // 참조 사진 기준: 귀는 주황+안쪽 크림(검은 귀끝 없음), 짙은 회색은 코와 아래 다리 (리뷰 2026-09-15 — 옛 문구 "dark-grey ear tips" 는 실제 제품과 달랐다)
  fox: 'the Yogibo Fox Mate plush — round orange head and body, cream muzzle, belly and inner ears, dark-grey nose and dark-grey lower legs, simple black dot eyes',
  trex: 'the Yogibo T-Rex Mate plush — a chubby RED T-Rex dinosaur sitting upright on its tail, a big NAVY-BLUE oval belly patch, a wide friendly mouth lined with small white zigzag felt teeth, tiny arms, dark-grey feet, simple dot eyes',
};
const MATE_REF_NOTE = {
  // 팍스 참조 사진에는 다른 캐릭터(너구리 등)와 머리 위 작은 팍스가 함께 찍혀 있다 — 가운데 큰 팍스 하나만 쓰게 한다
  fox: 'Use only the big orange fox plush in that reference; ignore the tiny fox figurine on its head and the other characters beside it.',
  trex: '',
};
/** 메이트 한 줄. kind 가 없으면 반대로 "인형·마스코트 넣지 말 것" — 모델이 제멋대로 곰인형을 넣지 않게 */
function mateLine(kind, placement) {
  if (!MATE_DESC[kind]) {
    return 'Do not add any plush toys, stuffed animals, dolls or mascot characters.';
  }
  return [
    `Hidden prop: include exactly ONE small plush toy, ${MATE_DESC[kind]}, matching the plush toy reference image (the last reference image). ${MATE_REF_NOTE[kind]}`,
    `Keep it SMALL — roughly one tenth of the image height — and secondary, never the focus and never covering a face: ${placement}.`,
    'It may wear a tiny festive hanbok vest or ribbon, but its shape and colours must stay clearly recognisable as that product. No other plush toys or mascots.',
  ].join(' ').replace(/\s+/g, ' ');
}

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

/** 축전 — 참고 카드(ref-card-*.jpg)를 붙여 보낼 때 쓰는 문장.
 *  참고 카드에는 한글 문구가 박혀 있다. 글자는 우리가 나중에 새기므로 "글자는 절대 따라 그리지 말 것" 을 특히 세게 건다. */
// 그림체를 세게 잡아 주는 문장 — 이게 없으면 사진처럼 밋밋하게 빠질 때가 있다 (2026-09-16)
const CARD_ART_LINE = [
  'ART DIRECTION (important): a hand-painted illustrated greeting card, NOT a photograph and not a 3D render screenshot —',
  'clean bold outlines around the characters, painterly brushwork, rich saturated storybook colours, warm rim light and soft glow,',
  'crisp readable shapes with clear separation between foreground characters and background, decorative illustrated framing.',
  'Keep every element illustrated in one consistent style; no photo textures, no realistic skin, no flat grey lighting.',
].join(' ');

const CARD_REF_LINE = [
  'A reference greeting card image is attached. Match it closely: the same two plush mascot characters together —',
  'a fluffy orange fox plush and a round red dinosaur plush, both wearing Korean hanbok — posed in the lower half of the card,',
  'and the same glossy, festive, slightly kitschy Korean mobile greeting-card look (rich colours, soft glow, gentle sparkle).',
  'Use the reference for the characters, colours, lighting and overall composition only.',
  'CRITICAL: the reference image contains Korean lettering. Do NOT copy, imitate or invent any text, letters, numbers, signatures or speech bubbles with writing —',
  'the card you draw must be completely free of any writing, because the greeting text is engraved afterwards.',
].join(' ');

function cardPrompt(seed, mate, styleRef) {
  return [
    'Create a Korean Chuseok (Hangawi) greeting e-card image in the nostalgic style that Korean parents love to send on KakaoTalk:',
    'glossy, festive, a little kitschy on purpose — bright saturated colours, shiny gold, sparkles and glow effects, like a classic mobile greeting card.',
    CARD_ART_LINE,
    styleRef ? CARD_REF_LINE : '',
    `Frame: ${pick(seed, 'border', CARD_BORDERS)}.`,
    `Background: ${pick(seed, 'palette', CARD_PALETTES)}, with a huge luminous full moon in the upper part.`,
    `Decoration: ${pick(seed, 'motif', CARD_MOTIFS)}.`,
    'IMPORTANT LAYOUT: keep a wide horizontal band in the upper-middle of the image — from about 14% to 44% of the image height, spanning most of the width — clean and simple (smooth sky or soft moonlight glow, no objects, no flowers, no busy patterns),',
    'because large greeting text will be placed there afterwards. The moon may sit behind that band as a soft glow.',
    // 참고 카드를 쓰면 캐릭터는 그 카드에서 온다 — 따로 메이트 인형을 하나 더 넣으라고 하지 않는다
    styleRef ? '' : mateLine(mate, 'in a lower corner of the frame like a cute sticker, bowing politely'),
    NO_TEXT,
  ].filter(Boolean).join(' ');
}

// ── 2. 한가위 사진관 ───────────────────────────────────────────────
// 장면마다 "그 장면에 맞는 조명 하나" 를 짝지어 둔다 — 조명이 하나로 정해져야 사람마다 빛이 달라 붙여넣은 티가 나지 않는다 (2026-09-15 "합성티" 피드백)
const STUDIO_SETS = [
  { scene: 'a premium traditional Korean photo studio: a painted folding screen (byeongpung) with a full moon, pine trees and cranes behind the group, a low wooden bench, silk cushions on a warm wooden floor',
    light: 'one large soft key light (softbox) from the front-left, a gentle fill from the right and a faint warm rim light from behind; warm studio colour temperature' },
  { scene: 'a moonlit hanok courtyard at night: wooden veranda (maru) as the seat, paper lanterns glowing, a big full moon above the tiled roof',
    light: 'warm lantern light from the front-left as the main light on every face, soft cool moonlight from behind as a rim light; the same two lights on everyone' },
  { scene: 'in front of a grand Korean palace gate at dusk: colourful dancheong eaves, wide stone steps as the seat, a faint full moon in the sky',
    light: 'low golden-hour sunlight from the left as the single main light, soft skylight fill from the front; every face and hanbok lit from the same side' },
];
const STUDIO_COLOURS = [
  'soft pastel hanbok colours — blush pink, mint, lavender, cream',
  'rich traditional hanbok colours — deep red, royal blue, jade green, gold trim',
];

function studioPrompt(seed, groups, mate, frameRef) {
  // groups: [{ source: 'photo'|'brief', label, people:[{presentation, ageGroup, ...}] }]
  const lines = [];
  let refIndex = 0;
  const refs = [];
  groups.forEach((g, i) => {
    if (g.source === 'photo') {
      refIndex++;
      refs.push(`reference photo ${refIndex} shows group ${i + 1}`);
      lines.push(`Group ${i + 1}: every person visible in reference photo ${refIndex}. Keep each face, hairstyle, skin tone, age and build faithful and recognizable — but re-pose and re-light them for this photo; do not reuse their original background, pose, clothing, lighting, colour cast, blur or image quality.`);
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
  const set = pick(seed, 'set', STUDIO_SETS);
  // 전체 인원 — 사진 그룹은 접수 때 센 count, 설명 그룹은 people 수. 모르면 0(문장 생략)
  const total = groups.reduce((n, g) => n + (g.source === 'photo' ? (Number(g.count) || 0) : ((g.people || []).length || 0)), 0)
    * (groups.every(g => g.source !== 'photo' || Number(g.count) > 0) ? 1 : 0);
  // 옛날 사진관 액자 사진 (2026-09-17 사용자 레퍼런스: 액자1·액자2 + "이렇게 나오게" 결과 예시)
  if (frameRef) {
    return [
      'Create ONE photograph of a vintage Korean family studio portrait displayed inside an ornate antique gilded picture frame, shot straight on against a plain neutral wall.',
      'The frame comes from the picture-frame reference image and nothing else: copy its ornate carved gilded wood, its scrollwork corners and beading, its thickness and depth, its warm aged gold with darker patina in the recesses. Its centre is empty — the portrait goes there. Take nothing else from that image.',
      'Inside the frame is a single old family photograph made in a Korean photo studio in the 1980s, printed on paper and faded with age.',
      ...lines,
      total > 0 ? `There are exactly ${total} people in the portrait — no more, no fewer.` : '',
      'It must read as one real photograph taken in one sitting: nobody pasted in, no cut-out edges, halos or outlines, no person lit, coloured, sharpened or grained differently from the others.',
      'Use the reference photos only to know who each person is — keep every face, hairstyle, skin tone, age and build recognizable, but re-pose, re-dress and re-light them inside this studio.',
      'Avoid these give-aways of a paste-up: a face sharper or grainier than the body it sits on; a seam or colour change at the neck or jaw; different noise or resolution between people; a head too large or too small for its body; two different shadow directions in one picture; a person with no shadow under their feet; a bright outline tracing someone against the backdrop.',
      total > 0 && total <= 3
        ? 'Pose them formally and symmetrically the way an old studio would: shoulders almost touching, one seated on a simple wooden studio chair with the others standing close behind, hands resting quietly on laps or on a shoulder, bodies square to the camera, calm dignified expressions with soft closed-lip smiles, everyone looking straight into the lens.'
        : 'Pose them formally and symmetrically the way an old studio would: a seated front row with a standing back row close behind, hands resting quietly on laps, bodies square to the camera, heads at even staggered heights, calm dignified expressions with soft closed-lip smiles, everyone looking straight into the lens.',
      'Dress everyone in traditional hanbok of that era in muted, slightly faded tones — dusty jade, ivory, soft rose, deep navy, maroon — never neon or modern-bright.',
      'Backdrop: a plain hand-painted studio backdrop, a soft brown-to-grey gradient with a gentle vignette; no props and no furniture beyond a simple wooden studio chair or bench for the front row.',
      'Lighting: a single flat frontal studio light with a soft fill, the way a small photo studio lit everyone at once — even light on every face, shallow shadows under the chins, a slight falloff toward the edges.',
      'Print look: aged colour film of the period — a warm amber-yellow shift, lowered contrast, muted skin tones, gentle lens softness, fine even film grain, faint dust and a few hairline scratches, and the subtle surface texture of an old photographic print. One single grade over the whole portrait.',
      'The glass is almost invisible: at most a very faint sheen, and no reflection covering any face.',
      'Framing: the whole frame must be fully visible with all four sides inside the picture, standing upright in the middle, with roughly 12% empty wall above it and 12% below — the very top and bottom of the image will be cropped away.',
      'No text anywhere: nothing written on the frame, no studio name, no date stamp, no signature, no lettering on the mat board.',
      refOrder(refs, mate, true),
      NO_TEXT,
    ].filter(Boolean).join(' ');
  }
  // "합성티" 줄이기 (2026-09-15 피드백): 사람을 오려 붙이는 게 아니라 한 자리에서 한 번에 다시 찍은 사진으로.
  // 참조 사진은 "누구인지" 만 쓰고, 자세·조명·색·화질은 이 장면 하나로 통일한다.
  return [
    'Create ONE photorealistic Chuseok group portrait that looks like a single real photograph taken in one shot, with everyone physically together in the same place at the same moment.',
    'It must NOT look like a collage or a composite: no people pasted in, no cut-out edges, halos or outlines, no person lit, coloured or sharpened differently from the others.',
    'Use the reference photos only to know who each person is. Re-photograph every person from scratch inside this scene.',
    // 합성티가 나는 구체적인 증상을 하나씩 막는다 (2026-09-16 피드백)
    'Avoid these give-aways of a paste-up: a face sharper or grainier than the body it sits on; a visible seam or colour change at the neck or jaw;',
    'one person crisp while another is soft; different noise, resolution or JPEG texture between people; a head too large or too small for its body;',
    'two different shadow directions in one picture; a person standing on the ground with no shadow under their feet; eyes looking in clearly different directions;',
    'a flat bright outline tracing a person against the background.',
    'Instead: let the people slightly overlap and cast soft shadows on each other, keep the same fine grain and the same gentle lens softness across the whole frame,',
    'and let the scene light wrap onto faces, hair and hanbok from the same side, with a little warm rim light on everyone from the same lamp.',
    ...lines,
    total > 0 ? `There are exactly ${total} people in total — no more, no fewer.` : '',
    total > 0 && total <= 3
      ? 'Pose them as one natural group (family, couple or friends) close together side by side — shoulders touching, one seated and the others standing just behind, or all seated shoulder to shoulder — a hand on a shoulder or linked arms, heads at natural staggered heights, bodies turned a little toward each other, relaxed warm smiles, everyone looking at the camera.'
      : 'Pose them as one natural group (family, couple or friends): a front row seated and a back row standing close behind, shoulders slightly overlapping, a hand resting on a shoulder or in a lap here and there, heads at natural staggered heights, bodies turned a little toward the centre, relaxed warm smiles, everyone looking at the camera.',
    'Keep real-world scale: head and body sizes must match where each person stands (people in the back row slightly smaller), with correct perspective from one camera position; nobody floats or is oversized.',
    'Dress every person in an elegant, well-fitted Korean hanbok that suits their age and presentation.',
    `Hanbok palette: ${pick(seed, 'colour', STUDIO_COLOURS)}.`,
    `Setting: ${set.scene}.`,
    `Lighting (the same for everyone): ${set.light}. Identical light direction, softness and colour temperature on every face and every hanbok, with matching shadows under chins and soft contact shadows on the seat, the floor and where people touch.`,
    'One camera: an 85mm portrait lens at eye level, f/5.6 so the whole group is equally sharp. One colour grade for the whole image — the same white balance, skin rendering and fine natural film grain everywhere; natural skin texture, correct hands, no duplicated or missing people.',
    'Framing: keep the whole group centred with some space above the heads and below the knees, because the top and bottom of the image may be cropped.',
    mateLine(mate, 'seated on a small silk cushion on the floor at the edge of the group, lit by the same light with a soft contact shadow, like a studio prop'),
    refOrder(refs, mate),
    NO_TEXT,
  ].filter(Boolean).join(' ');
}

/** 사진관 참조 순서 안내 — 고객 사진이 앞, 메이트(있을 때만)가 맨 뒤 */
function refOrder(refs, mate, frame) {
  const hasMate = !!MATE_DESC[mate];
  const extras = [];
  if (frame) extras.push('the picture-frame reference (an empty ornate gold frame) comes after the photos — use it only for the frame itself');
  if (hasMate) extras.push('the plush toy is the last reference');
  if (refs.length && extras.length) return `(Reference order: ${refs.join('; ')}; ${extras.join('; ')}.)`;
  if (refs.length) return `(References: ${refs.join('; ')}.)`;
  if (frame && !hasMate) return '(The only reference image is an empty ornate gold picture frame — use it only for the frame itself; every person is described in words above.)';
  if (extras.length) return `(Reference order: ${extras.join('; ')}.)`;
  return '(No reference images: every person is described in words above.)';
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

function petPrompt(seed, gender, species, mate) {
  const kind = species === 'cat' ? 'cat' : species === 'dog' ? 'dog' : 'pet';
  return [
    `Create a photorealistic, heart-warming Chuseok portrait of the exact same ${kind} from the reference photo.`,
    'Keep it unmistakably the same animal: same breed, size proportions, fur colour and pattern, markings, eye colour, ear and tail shape.',
    `Dress it in ${PET_HANBOK[gender === 'girl' ? 'girl' : 'boy']}, tailored naturally to its body so it looks comfortable and real (correct animal anatomy, no human hands).`,
    `Scene: ${pick(seed, 'scene', PET_SCENES)}.`,
    'Show only the animal (and the plush toy if one is requested below) — do not include any people, faces, hands or other human body parts from the reference photo.',
    'Soft cinematic lighting, shallow depth of field, warm autumn Chuseok mood.',
    mateLine(mate, 'sitting a little behind the pet as its toy friend — clearly a plush toy, much smaller than the pet'),
    NO_TEXT,
  ].join(' ');
}

// ── 4. 달나라 떡방아 알바생 (스노우 느낌) ──────────────────────────
const MOON_SCENES = [
  'pounding rice cakes (tteok) with a big wooden mallet in a stone mortar, working hard with a determined cute face',
  'taking a break sitting on the edge of a moon crater, holding a freshly made rice cake and giving a peace sign',
  'mid-swing with the wooden mallet, a little flour dust in the air, winking at the camera',
];

// 성별은 고객이 고른다 (2026-09-15 — 남자 사진인데 여자로 그려지는 일이 있었다. 사진 분석에 맡기지 않는다)
const MOON_GENDER = {
  male: {
    who: 'a man', kid: 'boy',
    keep: 'He is male: keep clearly masculine features, face shape, jawline, eyebrows, facial hair and short or natural hairstyle exactly as in the photo — do NOT feminize him (no makeup look, no lipstick, no long eyelashes, no softened jaw, no feminine hairstyle or accessories).',
    look: 'Filter look: a light, natural SNOW-style filter — clean clear skin (not heavily smoothed), a playful "cool guy" expression, a few floating sparkles and star AR stickers, subtle cheek glow, a dreamy navy-and-violet space background with twinkling stars, slightly wide-angle phone-selfie framing.',
  },
  female: {
    who: 'a woman', kid: 'girl',
    keep: 'She is female: keep her own face, features and hairstyle exactly as in the photo.',
    look: 'Filter look: bright soft SNOW beauty-filter skin, rosy blush stickers on the cheeks, floating sparkles, tiny star and heart AR stickers, a dreamy pastel pink-and-lavender space background with twinkling stars, slightly wide-angle phone-selfie framing.',
  },
};

function moonPrompt(seed, person, mate, gender) {
  // person: { source:'photo' } 이면 사진의 얼굴을 살린다. { source:'brief', people:[...] } 이면 설명으로만 (아이 등)
  // gender: 'male' | 'female' — 고객이 고른 값. 없으면(옛 응모) 성별 문장을 넣지 않는다
  const g = MOON_GENDER[gender] || null;
  let who;
  if (person && person.source === 'photo') {
    who = 'the main person from the reference photo (the largest face closest to the camera; leave out anyone else in the photo)' +
      (g ? `, who is ${g.who},` : '') + ' — keep their face, hairstyle, skin tone and age faithful and clearly recognizable';
  } else {
    const p0 = ((person && person.people) || [])[0] || {};
    const age = p0.ageGroup === 'child' ? (g ? 'young ' + g.kid : 'young child') : p0.ageGroup === 'teen' ? 'teenager' : 'young adult';
    who = 'a cute generic character (not based on any real person): ' + (g && p0.ageGroup !== 'child' ? g.who.replace('a ', 'a young ') : 'a ' + age);
  }
  return [
    'Create a playful selfie-style photo in the look of the Korean SNOW beauty-camera app with cute AR filter effects.',
    `Subject: ${who}, wearing fluffy white moon-rabbit ears and a little hanbok-style apron.`,
    g && person && person.source === 'photo' ? g.keep : '',
    `On the surface of a giant glowing full moon, like the Korean legend of the moon rabbit, the subject is ${pick(seed, 'scene', MOON_SCENES)}.`,
    g ? g.look : MOON_GENDER.female.look,
    'Keep it fun and cute, not scary; realistic photo of the person with filter overlays on top.',
    mateLine(mate, 'peeking out of a small moon crater in the background'),
    NO_TEXT,
  ].filter(Boolean).join(' ');
}

// ── 사진 확인용 비전 프롬프트 ───────────────────────────────────────
// 사람 사진(사진관·떡방아)은 「나의 쉼 순간」의 분석 프롬프트를 그대로 쓴다 (인원·연령대·14세 미만 여부).
const PEOPLE_ANALYSIS_PROMPT = RP.PHOTO_ANALYSIS_PROMPT;
// 반려동물 사진 — 동물이 실제로 있는지만 본다. 없으면 생성하지 않고(과금 없음) 다시 올려 달라고 한다.
// 사람도 함께 본다 — 아이가 반려동물을 안고 있는 사진이 모델로 가지 않게 (리뷰 2026-09-15). 호출부는 답이 없으면 "아이 있음" 으로 본다.
const PET_ANALYSIS_PROMPT = [
  'Look at the photo and answer ONLY with strict JSON:',
  '{"animal":"dog|cat|other|none","count":<number of pets clearly visible>,"people":<number of people or parts of people (faces, hands, bodies) visible>,"minorPresent":true|false,"confidence":"high|medium|low"}.',
  '"animal" is the main pet. Use "other" for rabbits, birds, hamsters etc. Use "none" if no animal is clearly visible.',
  '"minorPresent" is true if any visible person looks under 14 years old; false if there are no people or only adults.',
].join(' ');

module.exports = {
  NO_TEXT, cardPrompt, studioPrompt, petPrompt, moonPrompt, pickMate, mateLine, refOrder, MATE_KINDS,
  PEOPLE_ANALYSIS_PROMPT, PET_ANALYSIS_PROMPT,
  CARD_BORDERS, CARD_PALETTES, CARD_MOTIFS, STUDIO_SETS, STUDIO_COLOURS, PET_SCENES, MOON_SCENES, PET_HANBOK,
};
