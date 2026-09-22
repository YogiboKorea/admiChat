const cafe24 = require('./cafe24');

const TTL_MS = 6 * 60 * 60 * 1000;

// 특수 판매용 상품([협력사 특별할인], [LAST CHANCE], 예비맘 전용 등)과 컬러 에디션은 빼고 원래 상품만 쓴다
const SPECIAL_LISTING = /^\s*\[|\(|예비맘|고객결제|개인결제|365|세트상품/;
// 분류·재편집 프롬프트에 보여줄 "대표 모델" — 옵션·부속 상품은 제외 (보충용 비즈 단품은 남긴다)
const VARIANT = /프리미엄|플러스|커버|이너|비즈|패키지/;
const REFILL_BEADS = /^(스탠다드|프리미엄) 비즈/;

let cache = null;
let cachedAt = 0;

function productUrl(productNo) {
  return `https://yogibo.kr/product/detail.html?product_no=${productNo}`;
}

async function fromCafe24() {
  const rows = await cafe24.adminPaginate(
    '/products',
    { fields: 'product_no,product_name,eng_product_name,display,selling,list_image' },
    'products'
  );
  return rows
    .filter((p) => p.selling === 'T' && p.display === 'T' && !SPECIAL_LISTING.test(p.product_name))
    .sort((a, b) => a.product_no - b.product_no)
    .map((p) => ({
      productNo: p.product_no,
      name: p.product_name.trim(),
      engName: (p.eng_product_name || '').trim(),
      image: p.list_image || null,
      url: productUrl(p.product_no),
    }));
}

// Cafe24 토큰을 못 읽을 때의 대체 경로: 공개 sitemap (영문명·이미지 없음)
async function fromSitemap() {
  const res = await fetch('https://yogibo.kr/sitemap.xml', { headers: { 'User-Agent': 'Mozilla/5.0 yogibonews' } });
  if (!res.ok) throw new Error(`sitemap HTTP ${res.status}`);
  const xml = await res.text();
  const seen = new Set();
  const out = [];
  for (const m of xml.matchAll(/<loc>https:\/\/yogibo\.kr\/product\/([^<\/]+)\/(\d+)\/<\/loc>/g)) {
    const name = decodeURIComponent(m[1]).replace(/-/g, ' ');
    if (SPECIAL_LISTING.test(name) || seen.has(name)) continue;
    seen.add(name);
    out.push({ productNo: Number(m[2]), name, engName: '', image: null, url: productUrl(m[2]) });
  }
  return out;
}

/**
 * 국내에서 지금 판매·진열 중인 상품 목록 (6시간 캐시).
 * 반환: { all: 판매 중 전체(매칭·카드용), base: 대표 모델(프롬프트용), source: 'cafe24' | 'sitemap' | 'none' }
 */
async function getKrCatalog() {
  if (cache && Date.now() - cachedAt < TTL_MS) return cache;

  let all = [];
  let source = 'none';
  try {
    if (!cafe24.enabled()) throw new Error('CAFE24_TOKEN_URI 미설정');
    all = await fromCafe24();
    source = 'cafe24';
  } catch (err) {
    console.warn('⚠️ Cafe24 상품 조회 실패, sitemap으로 대체:', err.message);
    try {
      all = await fromSitemap();
      source = 'sitemap';
    } catch (err2) {
      console.warn('⚠️ sitemap 조회도 실패:', err2.message);
    }
  }

  if (all.length) {
    cache = { all, base: all.filter((p) => REFILL_BEADS.test(p.name) || !VARIANT.test(p.name)), source };
    cachedAt = Date.now();
  }
  return cache || { all: [], base: [], source: 'none' };
}

function formatProductList(products) {
  return products.map((p) => `- ${p.name}${p.engName ? ` (${p.engName})` : ''} | no=${p.productNo}`).join('\n');
}

module.exports = { getKrCatalog, formatProductList, productUrl };
