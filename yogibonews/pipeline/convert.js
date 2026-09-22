const { askClaude } = require('../lib/claude');
const { getKrCatalog, formatProductList } = require('../lib/krProducts');
const { absoluteUrl, rehostImage, makeThumbnail } = require('../lib/rehost');
const { wrapWithFont, designRules } = require('../lib/v5');
const links = require('../lib/productLinks');
const { today } = require('./classify');

// 원문 HTML에서 스타일·스크립트·속성을 걷어내고 이미지는 [IMG_n] 표시로 바꿔 토큰을 줄인다
function simplifySource(html) {
  const images = [];
  const indexByUrl = new Map();

  let s = String(html || '')
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/<(style|script|noscript)[^>]*>[\s\S]*?<\/\1>/gi, '');

  s = s.replace(/<img\b[^>]*>/gi, (tag) => {
    const url = absoluteUrl((tag.match(/\ssrc=["']([^"']+)["']/i) || [])[1]);
    if (!url) return '';
    const alt = (tag.match(/\salt=["']([^"']*)["']/i) || [])[1] || '';
    let n = indexByUrl.get(url);
    if (!n) {
      images.push({ url, alt });
      n = images.length;
      indexByUrl.set(url, n);
    }
    return alt ? `[IMG_${n}: ${alt}]` : `[IMG_${n}]`;
  });

  s = s.replace(/<([a-z][a-z0-9]*)\b([^>]*)>/gi, (m, tag, attrs) => {
    const href = (attrs.match(/\shref=["']([^"']+)["']/i) || [])[1];
    return tag.toLowerCase() === 'a' && href ? `<a href="${href}">` : `<${tag}>`;
  });

  return { text: s.replace(/\s+/g, ' ').trim(), images };
}

function buildSystemPrompt(productList, year) {
  return `당신은 요기보 코리아(yogibo.kr) 매거진 에디터입니다. 일본 요기보 공식 블로그 글을 한국 독자용 매거진 기사로 재편집합니다.
결과물은 사람이 검토한 뒤 최소한의 수정만 거쳐 바로 발행되므로, 그대로 게시할 수 있는 완성본으로 씁니다.

## 재편집 원칙
- 직역하지 말고 한국 독자가 자연스럽게 읽는 매거진 톤으로 다시 씁니다. 제목도 한국 독자 기준으로 새로 짓습니다.
- 원문의 사실(제품 특징, 수치, 사용법)은 지키고, 원문에 없는 사실·수치·후기·인용은 만들지 않습니다.
- 한국 독자에게 의미 없는 내용은 뺍니다: 엔화 가격, 일본 매장·배송·캠페인 정보, 일본 한정 이벤트, 일본 외부 사이트 안내.
- 일본 고유 행사·계절 이야기(경로의 날, 오본, 운동회 등)는 한국 상황에 맞게 바꾸거나(부모님 선물 등) 일반적인 이야기로 풀어씁니다.
- 기간 정보는 모두 뺍니다: 행사·할인 기간, 주문 마감일, 배송 일정, 특정 기념일·명절 날짜, '이번 달' 같은 시점 표현.
  기사는 언제 발행해도, 나중에 다시 읽어도 어색하지 않아야 합니다. 뺀 기간 정보는 reviewNotes에 적습니다.

## 제품과 링크
- 원문 제품 링크 옆에 [국내 판매: 이름 · no=번호], [국내 미판매: 이름], [판매 여부 확인 필요: 코드] 중 하나가 표시돼 있고, 사용자 메시지에 매칭표가 있습니다.
- 국내 판매 제품은 한국 판매명으로 부르고, 처음 언급하는 곳에 https://yogibo.kr/product/detail.html?product_no=번호 링크를 겁니다.
  번호는 매칭표의 번호(프리미엄 등 옵션 상품 포함)나 아래 목록의 번호를 씁니다. 매칭표에 있는 번호는 목록에 없어도 판매 중인 상품입니다.
- [판매 여부 확인 필요]는 일본몰에서 내려가 자동 매칭을 못 한 제품입니다. 본문의 제품명(영문·일본어)을 아래 목록과 대조해
  같은 제품이 있으면 국내 판매로 다루고, 없으면 미판매로 뺍니다. 어느 쪽으로 판단했는지 reviewNotes에 적습니다.
- 국내 미판매 제품은 언급·이미지에서 뺍니다. 비슷한 국내 제품으로 바꿔치기하지 않습니다.
- yogibo.jp 등 일본 사이트 링크와 원문 출처 링크는 쓰지 않습니다.
- 추천 제품 박스 안에는 칩 대신 {{PRODUCT_CARDS:번호,번호}} 한 줄만 씁니다(2~5개, 기사와 관련된 국내 판매 제품). 시스템이 상품 이미지 카드로 바꿉니다.

## 이미지
- 원문 본문에 이미지 위치가 [IMG_n] 또는 [IMG_n: 원문 설명]으로 표시돼 있습니다. 필요한 이미지를 골라 흐름에 맞게 배치합니다.
- 이미지는 반드시 <img src="{{IMG_n}}" alt="한국어 설명" style="width: 100%; display: block; border-radius: 6px; margin: 20px 0;"> 형식으로만 씁니다. 다른 URL은 쓰지 않습니다.
- 국내 미판매 제품이나 일본 매장·행사 사진처럼 한국 기사에 맞지 않는 이미지는 뺍니다.

${designRules(year)}
- 푸터 첫 문구는 "본 콘텐츠는 요기보 재팬 공식 블로그 콘텐츠를 바탕으로 한국 독자에 맞게 재구성되었습니다."로 씁니다.

## 국내 판매 제품 (대표 모델, 이름 (영문명) | 번호)
${productList || '(목록을 불러오지 못했습니다. 제품 링크와 추천 제품 카드는 넣지 말고, reviewNotes에 확인 필요로 남기세요.)'}

## 출력
- title: 목록 카드에 쓰일 한국어 제목(HTML 없이 한 줄)
- content: v5 형식 HTML 전체
- reviewNotes: 검수자가 확인할 점을 짧은 문장 목록으로. 뺀 내용(예: 국내 미판매 OO 언급 삭제), 한국 상황에 맞게 바꾼 부분, 원문 확인이 필요한 사실. 없으면 빈 배열.`;
}

const SCHEMA = {
  type: 'object',
  properties: {
    title: { type: 'string' },
    content: { type: 'string' },
    reviewNotes: { type: 'array', items: { type: 'string' } },
  },
  required: ['title', 'content', 'reviewNotes'],
  additionalProperties: false,
};

// Claude 결과의 {{IMG_n}}을 Cafe24로 재업로드한 URL로 바꾸고, 목록 밖 이미지·스크립트를 걸러낸다
async function placeImages(html, images) {
  const notes = [];
  const used = [...new Set([...html.matchAll(/\{\{IMG_(\d+)\}\}/g)].map((m) => Number(m[1])))].filter(
    (n) => n >= 1 && n <= images.length
  );

  const urlByIndex = {};
  for (const n of used) {
    try {
      urlByIndex[n] = await rehostImage(images[n - 1].url);
    } catch (err) {
      urlByIndex[n] = images[n - 1].url;
      notes.push(`이미지 ${n}번 Cafe24 재업로드 실패(${err.message}) — 일본 원본 주소를 그대로 사용 중`);
    }
  }
  const allowed = new Set(Object.values(urlByIndex));

  let out = html.replace(/\{\{IMG_(\d+)\}\}/g, (m, n) => urlByIndex[n] || '');
  out = out.replace(/<img\b[^>]*>/gi, (tag) => {
    const src = (tag.match(/\ssrc=["']([^"']*)["']/i) || [])[1];
    return src && allowed.has(src) ? tag : '';
  });
  out = out.replace(/<script[\s\S]*?<\/script>/gi, '');

  let thumbnail = null;
  const firstRehosted = used.find((n) => urlByIndex[n] !== images[n - 1].url);
  const thumbSource = images[(firstRehosted || used[0] || 1) - 1];
  if (thumbSource) {
    try {
      thumbnail = await makeThumbnail(thumbSource.url);
    } catch (err) {
      notes.push(`썸네일 자동 생성 실패(${err.message}) — 직접 업로드해 주세요.`);
    }
  }

  return { html: out, thumbnail, notes };
}

/**
 * Claude 결과를 게시 가능한 HTML로 마무리한다 (토큰 없음).
 * 이미지 재업로드 → 일본 링크 정리 → 상품 카드 렌더링 → UTM → 프리텐다드 래퍼
 */
async function finalizeHtml(html, images, { resolved = new Map(), campaign } = {}) {
  const placed = await placeImages(html, images);
  const notes = [...placed.notes];

  const jp = links.replaceJpLinks(placed.html, resolved);
  if (jp.converted) notes.push(`남아 있던 일본 제품 링크 ${jp.converted}개를 국내 상품 페이지로 바꿨습니다.`);
  if (jp.removed) notes.push(`일본 사이트 링크 ${jp.removed}개를 제거했습니다(텍스트는 유지).`);

  const cards = await links.renderProductCards(jp.html);
  if (cards.missing.length) notes.push(`추천 카드에서 판매 중이 아닌 상품번호 ${cards.missing.join(', ')}를 뺐습니다.`);

  const unsold = [...resolved.values()].filter((r) => !r.sold).map((r) => r.jpTitle);
  if (unsold.length) notes.push(`국내 미판매로 링크하지 않은 원문 제품: ${unsold.join(', ')}`);

  return {
    content: wrapWithFont(links.applyUtm(cards.html, campaign)),
    thumbnail: placed.thumbnail,
    notes,
  };
}

/**
 * 일본 블로그 원문을 한국 독자용 v5 매거진 기사로 재편집한다. (Claude 호출 — 과금 발생)
 * 반환: { title, content, thumbnail, reviewNotes }
 */
async function convertToKoreanMagazine({ title, content, link, campaign }) {
  const resolved = await links.resolveJpHandles(links.extractJpHandles(content));
  const { text, images } = simplifySource(content);
  const kr = await getKrCatalog();

  const result = await askClaude({
    system: buildSystemPrompt(formatProductList(kr.base), new Date().getFullYear()),
    content: `오늘 날짜: ${today()}\n원문 제목: ${title}\n원문 링크: ${link || '-'}\n\n## 원문 제품 → 국내 매칭\n${
      links.matchSummary(resolved) || '(원문에 제품 링크 없음)'
    }\n\n## 원문 본문\n${links.annotateJpProductLinks(text, resolved)}`,
    schema: SCHEMA,
    effort: 'medium',
    maxTokens: 32000,
    stream: true,
  });

  const finalized = await finalizeHtml(result.content, images, { resolved, campaign });
  return {
    title: result.title.trim(),
    content: finalized.content,
    thumbnail: finalized.thumbnail,
    reviewNotes: [...result.reviewNotes, ...finalized.notes],
  };
}

module.exports = { convertToKoreanMagazine, simplifySource, finalizeHtml, buildSystemPrompt };
