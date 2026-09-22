const TTL_MS = 24 * 60 * 60 * 1000;

let cache = null;
let cachedAt = 0;

/**
 * 일본 요기보(Shopify) 공개 제품 목록: handle → { title, engName } (24시간 캐시)
 * 제목 형식이 "Yogibo Max（ヨギボー マックス）"라 괄호 앞 영문명을 국내 상품 영문명과 매칭한다.
 */
async function getJpCatalog() {
  if (cache && Date.now() - cachedAt < TTL_MS) return cache;
  const map = new Map();
  try {
    for (let page = 1; page <= 20; page++) {
      const res = await fetch(`https://yogibo.jp/products.json?limit=250&page=${page}`, {
        headers: { 'User-Agent': 'Mozilla/5.0 yogibonews' },
      });
      if (!res.ok) throw new Error(`products.json HTTP ${res.status}`);
      const { products = [] } = await res.json();
      for (const p of products) {
        map.set(p.handle, { title: p.title, engName: p.title.split(/[（(]/)[0].trim() });
      }
      if (products.length < 250) break;
    }
    cache = map;
    cachedAt = Date.now();
  } catch (err) {
    console.warn('⚠️ 일본 제품 목록 조회 실패:', err.message);
  }
  return cache || map;
}

module.exports = { getJpCatalog };
