const { getDB } = require('../db');
const { getKrCatalog } = require('./krProducts');
const { getJpCatalog } = require('./jpProducts');

const UTM_SOURCE = 'yogibo_magazine';
const UTM_MEDIUM = 'article';

// 'Traybo2.0' ↔ 'Traybo'처럼 글자와 숫자가 붙은 모델명도 앞부분이 맞으면 같은 계열로 잡히게 띄어 쓴다
function normalize(name) {
  return String(name || '')
    .toLowerCase()
    .replace(/yogibo/g, ' ')
    .replace(/([a-z])(\d)/g, '$1 $2')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);
}

// 같은 영문명이 여러 개면 상품번호가 가장 낮은(원래) 상품을 쓴다 — catalog.all은 번호 오름차순
function buildEngIndex(products) {
  const index = new Map();
  for (const p of products) {
    const key = normalize(p.engName);
    if (key && !index.has(key)) index.set(key, p);
  }
  return index;
}

// 영문명이 없는 일본 상품명(예: 'Yogibo専用 補充ビーズ')은 키워드로 국내 상품을 찾는다
const JP_KEYWORD_ALIASES = [
  { jp: /Premium\s*補充ビーズ|プレミアム補充ビーズ/i, kr: /^프리미엄 비즈/ },
  { jp: /補充ビーズ/, kr: /^스탠다드 비즈/ },
];

function matchJpKeyword(title, products) {
  const alias = JP_KEYWORD_ALIASES.find((a) => a.jp.test(title));
  const product = alias && products.find((p) => alias.kr.test(p.name));
  return product ? { product, method: 'keyword' } : null;
}

// 정확히 같은 영문명 → exact, 국내 영문명이 일본 영문명의 앞부분이면(예: Max ↔ Max ○○ Edition) → family
function matchEngName(engName, index) {
  const key = normalize(engName);
  if (!key) return null;
  if (index.has(key)) return { product: index.get(key), method: 'exact' };
  let best = null;
  for (const [k, p] of index) {
    if (`${key} `.startsWith(`${k} `) && (!best || k.length > best.k.length)) best = { k, product: p };
  }
  return best ? { product: best.product, method: 'family' } : null;
}

function extractJpHandles(html) {
  return [...String(html || '').matchAll(/yogibo\.jp\/products\/([a-z0-9\-_]+)/gi)].map((m) => m[1].toLowerCase());
}

/**
 * 일본 제품 handle → 국내 상품 매칭. 관리자가 productMap 컬렉션에 지정한 값이 있으면 그걸 우선한다.
 * 반환: Map(handle → { handle, jpTitle, sold, product?, method: exact|family|manual|none|unknown })
 */
async function resolveJpHandles(handles) {
  const unique = [...new Set(handles)];
  const out = new Map();
  if (!unique.length) return out;

  const [kr, jp, overrideRows] = await Promise.all([
    getKrCatalog(),
    getJpCatalog(),
    getDB().collection('productMap').find({ handle: { $in: unique } }).toArray(),
  ]);
  const index = buildEngIndex(kr.all);
  const byNo = new Map(kr.all.map((p) => [p.productNo, p]));
  const overrides = new Map(overrideRows.map((r) => [r.handle, r]));

  for (const handle of unique) {
    const jpItem = jp.get(handle);
    const jpTitle = jpItem?.engName || handle;
    const override = overrides.get(handle);
    if (override) {
      const product = override.krProductNo ? byNo.get(override.krProductNo) : null;
      out.set(handle, { handle, jpTitle, sold: Boolean(product), product, method: 'manual' });
    } else if (!jpItem) {
      out.set(handle, { handle, jpTitle, sold: false, method: 'unknown' });
    } else {
      const m = matchEngName(jpItem.engName, index) || matchJpKeyword(jpItem.title, kr.all);
      out.set(handle, m ? { handle, jpTitle, sold: true, ...m } : { handle, jpTitle, sold: false, method: 'none' });
    }
  }
  return out;
}

// 지금 일본몰 목록에 없는 제품(단종·이름 변경)은 영문명을 알 수 없어 미판매로 단정하지 않는다
const UNKNOWN_NOTE = '판매 여부 확인 필요 — 일본몰에서 내려간 제품이라 이름으로 국내 목록과 대조';

function matchSummary(resolved) {
  return [...resolved.values()]
    .map((r) => {
      if (r.sold) return `- ${r.jpTitle} → ${r.product.name} (no=${r.product.productNo})`;
      if (r.method === 'unknown') return `- ${r.jpTitle} → ${UNKNOWN_NOTE}`;
      return `- ${r.jpTitle} → 국내 미판매`;
    })
    .join('\n');
}

// 원문(간소화된 HTML)의 일본 제품 링크에 국내 판매 여부를 표시해 Claude에게 넘긴다
function annotateJpProductLinks(html, resolved) {
  return html.replace(/<a href="[^"]*yogibo\.jp\/products\/([a-z0-9\-_]+)[^"]*">([\s\S]*?)<\/a>/gi, (m, handle, text) => {
    const r = resolved.get(handle.toLowerCase());
    if (r?.sold) return `<a href="${r.product.url}">${text}</a>[국내 판매: ${r.product.name} · no=${r.product.productNo}]`;
    if (!r || r.method === 'unknown') return `${text}[판매 여부 확인 필요: ${handle}]`;
    return `${text}[국내 미판매: ${r.jpTitle}]`;
  });
}

// Claude 결과에 남은 일본 링크 정리: 제품 링크는 국내 상품으로 바꾸고, 나머지 일본 링크는 텍스트만 남긴다
function replaceJpLinks(html, resolved) {
  let converted = 0;
  let removed = 0;
  const out = html.replace(/<a\b([^>]*?)href=(["'])([^"']*yogibo\.jp[^"']*)\2([^>]*)>([\s\S]*?)<\/a>/gi, (m, pre, q, url, post, text) => {
    const handle = (url.match(/\/products\/([a-z0-9\-_]+)/i) || [])[1];
    const r = handle && resolved.get(handle.toLowerCase());
    if (r?.sold) {
      converted++;
      return `<a${pre}href=${q}${r.product.url}${q}${post}>${text}</a>`;
    }
    removed++;
    return text;
  });
  // 링크가 아닌 본문 텍스트로 적힌 일본 사이트 주소도 지운다
  const cleaned = out.replace(/(?<![="'\w/])https?:\/\/(?:www\.)?yogibo\.jp\/[^\s<"']*/gi, () => {
    removed++;
    return '';
  });
  return { html: cleaned, converted, removed };
}

function withUtm(url, campaign) {
  const u = new URL(url);
  if (u.searchParams.has('utm_source')) return u.toString();
  u.searchParams.set('utm_source', UTM_SOURCE);
  u.searchParams.set('utm_medium', UTM_MEDIUM);
  if (campaign) u.searchParams.set('utm_campaign', campaign);
  return u.toString();
}

// 국내몰 상품 링크에 UTM을 붙인다 (이미 있으면 유지 — 여러 번 저장해도 한 번만 붙음)
function applyUtm(html, campaign) {
  return String(html || '').replace(
    /href=(["'])(https?:\/\/(?:www\.)?yogibo\.kr\/product\/[^"']*)\1/gi,
    (m, q, url) => {
      try {
        return `href=${q}${withUtm(url.replace(/&amp;/g, '&'), campaign).replace(/&/g, '&amp;')}${q}`;
      } catch {
        return m;
      }
    }
  );
}

function renderCard(p) {
  const image = p.image
    ? `<img src="${escapeHtml(p.image)}" alt="${escapeHtml(p.name)}" style="width: 100%; aspect-ratio: 1 / 1; object-fit: cover; display: block; margin: 0; border-radius: 0; background: #f7f9fa;">`
    : '';
  return `<a href="${escapeHtml(p.url)}" style="flex: 1 1 140px; max-width: 200px; background: #ffffff; border-radius: 6px; overflow: hidden; text-decoration: none; display: block;">
${image}
<span style="display: block; padding: 10px 12px 2px; font-size: 13px; font-weight: 700; color: #1e3a47; line-height: 1.4;">${escapeHtml(p.name)}</span>
<span style="display: block; padding: 0 12px 12px; font-size: 11px; font-weight: 600; color: #2c9db6;">제품 보러가기 &rarr;</span>
</a>`;
}

/**
 * {{PRODUCT_CARDS:39,466}} 자리표시자를 Cafe24 대표 이미지·상품명 카드로 바꾼다.
 * 판매 중이 아닌 번호는 빼고 missing으로 알려준다.
 */
async function renderProductCards(html) {
  if (!/\{\{PRODUCT_CARDS:/.test(html)) return { html, missing: [] };
  const kr = await getKrCatalog();
  const byNo = new Map(kr.all.map((p) => [p.productNo, p]));
  const missing = [];
  const out = html.replace(/\{\{PRODUCT_CARDS:([\d,\s]+)\}\}/g, (m, list) => {
    const products = list
      .split(',')
      .map((s) => Number(s.trim()))
      .filter(Boolean)
      .map((no) => byNo.get(no) || (missing.push(no), null))
      .filter(Boolean);
    if (!products.length) return '';
    return `<div style="display: flex; flex-wrap: wrap; gap: 10px; margin-top: 12px;">\n${products.map(renderCard).join('\n')}\n</div>`;
  });
  return { html: out, missing };
}

module.exports = {
  extractJpHandles,
  resolveJpHandles,
  matchSummary,
  annotateJpProductLinks,
  replaceJpLinks,
  applyUtm,
  renderProductCards,
  normalize,
  matchEngName,
  buildEngIndex,
};
