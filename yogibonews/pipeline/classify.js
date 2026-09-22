const { askClaude, hasKey } = require('../lib/claude');
const { getKrCatalog, formatProductList } = require('../lib/krProducts');
const { extractJpHandles, resolveJpHandles, matchSummary } = require('../lib/productLinks');

function stripHtml(html) {
  return String(html || '')
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, ' ')
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// 기간 한정 여부 판단용 (한국 시간 기준 YYYY-MM-DD). 시스템 프롬프트가 아닌 사용자 메시지에 넣어 캐시를 깨지 않는다
function today() {
  return new Date(Date.now() + 9 * 60 * 60 * 1000).toISOString().slice(0, 10);
}

const EXCLUDE_CATEGORIES = ['none', 'local_event', 'store_notice', 'jp_only_product', 'jp_collab', 'period_limited'];

function buildSystemPrompt(productList) {
  return `당신은 요기보(Yogibo) 한국 마케팅팀을 위해 일본 공식 블로그 게시물을 1차 심사하는 에디터입니다.
한국 매거진(yogibo.kr)에 한국 독자용으로 재편집해 실을 가치가 있는지 판단합니다.

## exclude — 한국 매거진에 실을 필요가 없으면 true
excludeCategory로 제외 사유를 분류합니다.
- local_event: 일본 특정 지역·장소에서 열리는 오프라인 이벤트, 팝업, 전시, 협찬(【協賛】) 행사, 현지 체험 행사 레포트
- store_notice: 일본 매장 오픈·휴점·리뉴얼, 일본 연휴 휴업·배송 일정, 일본 온라인스토어 공지(배송비 개정, 시스템 점검 등)
- jp_only_product: 한국에서 판매하지 않는 제품이 글의 중심인 경우 (신제품 발표, 해당 제품 전용 커버 출시 등)
- jp_collab: 일본 한정 콜라보(일본 캐릭터·호텔·기업·연예인 협업, 일본 한정 라이브 방송)
- period_limited: 특정 기간에만 진행하는 할인·캠페인·기간 한정 판매·예약 접수·기념일 한정 기획처럼 글의 핵심이 그 기간에 묶여 있는 경우.
  이미 기간이 지났거나(사용자 메시지의 오늘 날짜 기준) 한국 매거진에 실을 즈음엔 의미가 없는 경우입니다. 사유에 기간과 종료 여부를 적습니다.
해당 없으면 exclude=false, excludeCategory="none". 여러 개에 해당하면 가장 직접적인 사유 하나를 고릅니다.

한국에서도 유효한 콘텐츠는 제외하지 않습니다: 국내 판매 제품의 소개·활용법·선택 가이드, 브랜드 스토리,
라이프스타일 콘텐츠, 계절·상황별 추천. 일본 기념일·시즌을 계기로 쓴 글이라도 날짜를 빼고 다시 읽어도 유효한
가이드·제품 추천이면 유지합니다(재편집 단계에서 기간 정보를 뺍니다).
여러 제품을 소개하는 글에서 일부만 국내 미판매라면 제외하지 않습니다(재편집 단계에서 해당 부분을 뺍니다).

## 국내 판매 제품 (yogibo.kr에서 지금 판매 중인 대표 모델)
사용자 메시지의 "원문 제품 → 국내 매칭"은 제품 링크를 영문명으로 대조한 결과이니 우선 참고합니다.
"판매 여부 확인 필요"로 나온 제품은 일본몰에서 내려가 자동 대조를 못 한 것이니, 본문의 제품명을 아래 목록과 직접 대조합니다
(이것만으로 jp_only_product 제외를 하지 않습니다).
링크 없이 언급된 제품은 일본 제품명(ヨギボー マックス 등)을 아래 목록과 대조합니다. 목록은 대표 모델만 있으니,
커버·컬러 등 파생 상품은 기본 모델이 있으면 판매 중으로 봅니다. 목록에 없고 일본 고유 제품임이 분명할 때만 미판매로 봅니다.
${productList || '(목록을 불러오지 못했습니다. 제품 판매 여부는 판단하지 말고 지역·콜라보 기준만 적용하세요.)'}

## translatable — 재편집해 게시할 만한 실질적인 본문이 있으면 true
본문이 거의 없거나 이미지 나열뿐이라 게시물이 되지 않으면 false.

사유는 각각 한국어 한 문장으로, 판단 근거(지명·제품명 등)를 포함해 씁니다.`;
}

const SCHEMA = {
  type: 'object',
  properties: {
    exclude: { type: 'boolean' },
    excludeCategory: { type: 'string', enum: EXCLUDE_CATEGORIES },
    excludeReason: { type: 'string' },
    translatable: { type: 'boolean' },
    translatableReason: { type: 'string' },
  },
  required: ['exclude', 'excludeCategory', 'excludeReason', 'translatable', 'translatableReason'],
  additionalProperties: false,
};

/**
 * RSS로 들어온 일본 블로그 게시물 1건을 분류한다.
 * Claude를 쓸 수 없으면 자동 제외/자동 변환을 하지 않고 "수동 확인 필요"로 폴백한다.
 */
async function classifyItem({ title, content }) {
  if (!hasKey()) {
    const reason = 'Claude 비활성화 상태라 자동 판별을 건너뛰었습니다. 수동 확인이 필요합니다.';
    return {
      exclude: false,
      excludeCategory: 'none',
      excludeReason: reason,
      translatable: false,
      translatableReason: reason,
      skipped: true,
    };
  }

  const [kr, resolved] = await Promise.all([getKrCatalog(), resolveJpHandles(extractJpHandles(content))]);
  const result = await askClaude({
    system: buildSystemPrompt(formatProductList(kr.base)),
    content: `오늘 날짜: ${today()}\n제목: ${title}\n\n## 원문 제품 → 국내 매칭\n${matchSummary(resolved) || '(제품 링크 없음)'}\n\n## 본문(텍스트만 추출)\n${stripHtml(content)}`,
    schema: SCHEMA,
    effort: 'low',
    maxTokens: 4000,
  });

  return { ...result, skipped: false };
}

module.exports = { classifyItem, stripHtml, buildSystemPrompt, today, EXCLUDE_CATEGORIES };
