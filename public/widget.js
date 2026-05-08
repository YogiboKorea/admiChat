;(function () {
    // ────────────────────────────────────────────────────────────────
    // 0) 스크립트/설정값
    //    - script.src 의 origin = 이벤트 데이터 서버 (이 앱의 도메인)
    //    - data-api-base       = cafe24 상품/카테고리/트래킹 서버 (ychat)
    //    - data-page-id        = 이벤트 ID (MongoDB _id)
    //    - data-mall-id        = cafe24 mall id
    // ────────────────────────────────────────────────────────────────
    let script = document.currentScript;
    if (!script || !script.dataset.pageId) {
      script = Array.from(document.getElementsByTagName('script')).find(s =>
        /widget\.js/.test(s.src) && s.dataset.pageId
      );
    }
    if (!script || !script.dataset.pageId) {
      console.warn('⚠️ widget.js: data-page-id 누락');
      return;
    }
  
    const SELF_BASE = (() => {
      try { return new URL(script.src).origin; } catch (e) { return ''; }
    })();
    const PRODUCT_API_BASE = script.dataset.apiBase || '';
    const pageId = script.dataset.pageId;
    const mallId = script.dataset.mallId || 'yogibo';
    const couponNos = script.dataset.couponNos || '';
    const couponQSStart = couponNos ? `?coupon_no=${couponNos}` : '';
    const couponQSAppend = couponNos ? `&coupon_no=${couponNos}` : '';
  
    // ────────────────────────────────────────────────────────────────
    // 1) 유틸/트래킹 (ychat 의 /api/{mallId}/track 그대로 사용)
    // ────────────────────────────────────────────────────────────────
    const ua = navigator.userAgent;
    const device = /Android/i.test(ua) ? 'Android' : /iPhone|iPad|iPod/i.test(ua) ? 'iOS' : 'PC';
    const visitorId = (() => {
      const key = 'appVisitorId';
      try {
        let id = localStorage.getItem(key);
        if (!id) {
          id = (crypto && crypto.randomUUID) ? crypto.randomUUID() : String(Date.now()) + Math.random();
          localStorage.setItem(key, id);
        }
        return id;
      } catch (e) {
        return (crypto && crypto.randomUUID) ? crypto.randomUUID() : String(Date.now()) + Math.random();
      }
    })();
  
    const pad = n => String(n).padStart(2, '0');
    function today() {
      const d = new Date();
      return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
    }
    function shouldTrack() {
      try {
        if (/[?&]track=true/.test(location.search)) return true;
        const key = `tracked_${pageId}_${visitorId}_${today()}`;
        if (sessionStorage.getItem(key)) return false;
        sessionStorage.setItem(key, '1');
        return true;
      } catch (e) {
        return true;
      }
    }
    function track(payload) {
      if (!PRODUCT_API_BASE) return;
      fetch(`${PRODUCT_API_BASE}/api/${mallId}/track`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      }).catch(() => {});
    }
    if (shouldTrack()) {
      track({ pageId, pageUrl: location.pathname, visitorId, type: 'view', device, referrer: document.referrer || 'direct', timestamp: new Date().toISOString() });
    } else {
      track({ pageId, pageUrl: location.pathname, visitorId, type: 'revisit', device, referrer: document.referrer || 'direct', timestamp: new Date().toISOString() });
    }
    document.body.addEventListener('click', (e) => {
      const el = e.target.closest('[data-track-click]');
      if (!el) return;
      const elementType = el.dataset.trackClick;
      const payload = { pageId, pageUrl: location.pathname, visitorId, type: 'click', element: elementType, device, referrer: document.referrer || 'direct', timestamp: new Date().toISOString() };
      if (elementType === 'product') {
        const productNo = el.dataset.productNo;
        if (productNo) payload.productNo = productNo;
      }
      track(payload);
    });
  
    // ────────────────────────────────────────────────────────────────
    // 2) 공통 헬퍼
    // ────────────────────────────────────────────────────────────────
    function escapeHtml(s = '') { return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
    function toBool(v) { return v === true || v === 'true' || v === 1 || v === '1' || v === 'on'; }
  
    function fetchWithRetry(url, opts = {}, retries = 3, backoff = 1000) {
      return fetch(url, opts).then(res => {
        if (res.status === 429 && retries > 0) {
          return new Promise(r => setTimeout(r, backoff)).then(() => fetchWithRetry(url, opts, retries - 1, backoff * 2));
        }
        if (!res.ok) throw res;
        return res;
      });
    }
  
    function buildYouTubeSrc(id, autoplay = false, loop = false) {
      const params = new URLSearchParams({ autoplay: autoplay ? '1' : '0', mute: autoplay ? '1' : '0', playsinline: '1', rel: 0, modestbranding: 1, enablejsapi: 1 });
      if (loop) { params.set('loop', '1'); params.set('playlist', id); }
      return `https://www.youtube.com/embed/${id}?${params.toString()}`;
    }
  
    // 카페24 상품 URL 정규화 — iOS Safari 다운로드 다이얼로그 우회
    function normalizeHref(rawHref) {
      if (!rawHref) return null;
      let href = String(rawHref).trim();
      if (!href || href === '#') return null;
      if (/^\d+$/.test(href) || href.length < 4) return null;
  
      let decoded = href;
      try { decoded = decodeURI(href); } catch (e) { decoded = href; }
  
      const m = decoded.match(/\/product\/[^\/]+\/(\d+)(?:\/category\/(\d+))?/);
      if (m) {
        try {
          const baseForOrigin = /^https?:\/\//i.test(href) ? href : `https://${href}`;
          const u = new URL(baseForOrigin);
          const productNo = m[1];
          const cateNo = m[2];
          return `${u.origin}/product/detail.html?product_no=${productNo}` + (cateNo ? `&cate_no=${cateNo}` : '');
        } catch (e) { return null; }
      }
  
      try {
        const finalUrl = /^https?:\/\//i.test(href) ? href : `https://${href}`;
        const u = new URL(finalUrl);
        if (!u.hostname.includes('.')) return null;
        return finalUrl;
      } catch (e) { return null; }
    }
  
    // ────────────────────────────────────────────────────────────────
    // 3) 블록 렌더링
    // ────────────────────────────────────────────────────────────────
    function getRootContainer() {
      let root = document.getElementById('evt-root');
      if (!root) {
        root = document.createElement('div');
        root.id = 'evt-root';
        script.parentNode.insertBefore(root, script);
      }
      root.innerHTML = '';
      return root;
    }
  
    function renderImageBlock(block, root) {
      const wrap = document.createElement('div');
      wrap.style.cssText = 'position:relative; margin:0 auto; width:100%; max-width:800px; font-size:0;';
      const img = document.createElement('img');
      img.src = block.src;
      img.style.cssText = 'max-width:100%; height:auto; display:block; margin:0 auto;';
      wrap.appendChild(img);
      (block.regions || []).forEach(r => {
        const l = (r.xRatio * 100).toFixed(2), t = (r.yRatio * 100).toFixed(2), w = (r.wRatio * 100).toFixed(2), h = (r.hRatio * 100).toFixed(2);
        if (r.coupon) {
          const btn = document.createElement('button');
          btn.dataset.couponNo = r.coupon;
          btn.onclick = () => window.downloadCoupon(r.coupon);
          btn.style.cssText = `position:absolute; left:${l}%; top:${t}%; width:${w}%; height:${h}%; border:none; cursor:pointer; background:transparent;`;
          wrap.appendChild(btn);
        } else if (r.href) {
          const safeHref = normalizeHref(r.href);
          if (!safeHref) return;
          const a = document.createElement('a');
          a.href = safeHref;
          a.target = '_blank';
          a.rel = 'noopener noreferrer';
          a.style.cssText = `position:absolute; left:${l}%; top:${t}%; width:${w}%; height:${h}%; display:block;`;
          wrap.appendChild(a);
        }
      });
      root.appendChild(wrap);
    }
  
    function renderTextBlock(block, root) {
      const st = block.style || {};
      const wrapper = document.createElement('div');
      wrapper.style.textAlign = st.align || 'center';
      wrapper.style.marginTop = `${st.mt ?? 16}px`;
      wrapper.style.marginBottom = `${st.mb ?? 16}px`;
      const inner = document.createElement('div');
      inner.style.fontSize = `${st.fontSize || 18}px`;
      inner.style.fontWeight = st.fontWeight || 'normal';
      inner.style.color = st.color || '#333';
      inner.innerHTML = escapeHtml(block.text || '').replace(/\n/g, '<br/>');
      wrapper.appendChild(inner);
      root.appendChild(wrapper);
    }
  
    function renderVideoBlock(block, root) {
      const ratio = block.ratio || { w: 16, h: 9 };
      if (!block.youtubeId) return;
      const src = buildYouTubeSrc(block.youtubeId, toBool(block.autoplay), toBool(block.loop));
      const wrap = document.createElement('div');
      wrap.style.cssText = `position:relative; width:100%; max-width:800px; margin:16px auto; aspect-ratio:${ratio.w}/${ratio.h};`;
      const iframe = document.createElement('iframe');
      iframe.src = src;
      iframe.title = `youtube-${block.youtubeId}`;
      iframe.style.cssText = 'position:absolute; inset:0; width:100%; height:100%; border:0;';
      iframe.allow = 'accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share';
      iframe.setAttribute('allowfullscreen', '');
      wrap.appendChild(iframe);
      root.appendChild(wrap);
    }
  
    function renderProductBlock(block, root) {
      const groupWrapper = document.createElement('div');
      groupWrapper.className = 'product-group-wrapper';
  
      if (block.layoutType === 'tabs') {
        const activeColor = block.activeColor || '#1890ff';
        const tabsContainer = document.createElement('div');
        tabsContainer.className = `tabs_${pageId}`;
        (block.tabs || []).forEach((t, i) => {
          const btn = document.createElement('button');
          if (i === 0) {
            btn.className = 'active';
            btn.style.backgroundColor = activeColor;
            btn.style.color = '#fff';
            btn.style.borderColor = activeColor;
          }
          btn.onclick = () => window.showTab(`${block.id || pageId}-tab-${i}`, btn, activeColor);
          btn.textContent = t.title || `탭 ${i+1}`;
          tabsContainer.appendChild(btn);
        });
        groupWrapper.appendChild(tabsContainer);
  
        (block.tabs || []).forEach((t, i) => {
          const panel = document.createElement('div');
          panel.id = `${block.id || pageId}-tab-${i}`;
          panel.className = `tab-content_${pageId}`;
          panel.style.display = i === 0 ? 'block' : 'none';
          const ul = document.createElement('ul');
          ul.className = `main_Grid_${pageId}`;
          ul.dataset.gridSize = block.gridSize;
          if (block.registerMode === 'direct') {
            const tabDirect = (block.tabDirectProducts?.[i] || []);
            const directNos = tabDirect.map(p => p.product_no).join(',');
            ul.dataset.directNos = directNos;
            try { ul.dataset.savedProducts = JSON.stringify(tabDirect); } catch (e) { /* skip */ }
          } else { ul.dataset.cate = t.sub || t.root; }
          panel.appendChild(ul);
          groupWrapper.appendChild(panel);
        });
      } else {
        const widgetDiv = document.createElement('div');
        widgetDiv.className = 'product_list_widget';
        const ul = document.createElement('ul');
        ul.className = `main_Grid_${pageId}`;
        ul.dataset.gridSize = block.gridSize;
        if (block.registerMode === 'direct') {
          const directNos = (block.directProducts || []).map(p => p.product_no).join(',');
          ul.dataset.directNos = directNos;
          // 저장된 상품 데이터(영문/요약 등) 를 보존 — runtime ychat 응답에 누락된 필드의 fallback 으로 사용.
          try { ul.dataset.savedProducts = JSON.stringify(block.directProducts || []); } catch (e) { /* skip */ }
        } else { ul.dataset.cate = block.sub || block.root; }
        widgetDiv.appendChild(ul);
        groupWrapper.appendChild(widgetDiv);
      }
      root.appendChild(groupWrapper);
    }
  
    // ────────────────────────────────────────────────────────────────
    // 4) 상품 데이터 로드 (cafe24 ychat 서버에서 fetch)
    // ────────────────────────────────────────────────────────────────
    async function fetchProducts(directNosAttr, category, limit = 300) {
      if (!PRODUCT_API_BASE) return [];
      const fetchOpts = { cache: 'no-store', headers: { 'Cache-Control': 'no-cache' } };
  
      const mapProductData = p => ({
        product_no: p.product_no,
        product_name: p.product_name,
        // cafe24 의 영문 상품명 / 요약 설명 / 브리핑 설명 — #goods_name 영역에 사용.
        // 가장 적합한 값을 우선순위로 fallback.
        eng_product_name: p.eng_product_name || '',
        summary_description: p.summary_description || '',
        simple_description: p.simple_description || '',
        price: p.price,
        list_image: p.list_image,
        image_medium: p.image_medium,
        image_small: p.image_small,
        image_thumbnail: p.tiny_image,
        sale_price: p.sale_price || null,
        benefit_price: p.benefit_price || null,
        benefit_percentage: p.benefit_percentage || null,
        decoration_icon_url: p.decoration_icon_url || null,
        icons: p.icons || null,
        additional_icons: p.additional_icons || [],
        product_tags: p.product_tags || ''
      });
  
      if (directNosAttr) {
        const ids = directNosAttr.split(',').map(s => s.trim()).filter(Boolean);
        if (ids.length === 0) return [];
        const results = await Promise.all(ids.map(no =>
          fetchWithRetry(`${PRODUCT_API_BASE}/api/${mallId}/products/${no}${couponQSStart}`, fetchOpts).then(r => r.json())
        ));
        return results.map(p => (p && p.product_no) ? p : {}).map(mapProductData);
      } else if (category) {
        const prodUrl = `${PRODUCT_API_BASE}/api/${mallId}/categories/${category}/products?limit=${limit}${couponQSAppend}`;
        const rawProducts = await fetchWithRetry(prodUrl, fetchOpts).then(r => r.json()).then(json => Array.isArray(json) ? json : (json.products || []));
        return rawProducts.map(p => (typeof p === 'object' ? p : {})).map(mapProductData);
      }
      return [];
    }
  
    async function loadPanel(ul) {
      const cols = parseInt(ul.dataset.gridSize, 10) || 2;
      let spinner = null;
  
      const spinnerTimer = setTimeout(() => {
        spinner = document.createElement('div');
        spinner.className = 'grid-spinner';
        if (ul.parentNode) ul.parentNode.insertBefore(spinner, ul);
      }, 2000);
  
      try {
        const products = await fetchProducts(ul.dataset.directNos, ul.dataset.cate, ul.dataset.count);
        renderProducts(ul, products, cols);
      } catch (err) {
        console.error('상품 로드 실패:', err);
        if (ul.parentNode) {
          const errDiv = document.createElement('div');
          errDiv.style.textAlign = 'center';
          errDiv.style.padding = '50px 0';
          errDiv.innerHTML = `<p style="color:#666; font-size:14px; margin: 0;">상품 정보를 불러올 수 없습니다.</p>`;
          ul.parentNode.insertBefore(errDiv, ul);
        }
      } finally {
        clearTimeout(spinnerTimer);
        if (spinner) spinner.remove();
      }
    }
  
    // 자체 클래스로 상품 카드 렌더 — 자사몰 /css/goodsData.html 의존성 제거.
    // 디자인은 사이트 라이브 카드(쿠폰 적용 시: % 뱃지 + 정가(취소선) + 최종가)와 일치시킴.
    //   .prd_link    : 이미지+영문/요약+상품명 까지 묶는 a 태그
    //   .prd_thumb   : 이미지 영역 (정사각형)
    //   .prd_desc    : 영문/요약 (작은 회색)
    //   .prd_name    : 상품명 (굵게)
    //   .prd_price   : 가격 영역 (10% 뱃지 + 정가 취소선 + 최종가)
    function renderProducts(ul, products, cols) {
      ul.style.cssText = `display:grid; grid-template-columns:repeat(${cols},1fr); gap:24px; max-width:800px; margin:24px auto; list-style:none; padding:0;`;
  
      const formatKRW = val => `${(Number(val) || 0).toLocaleString('ko-KR')}원`;
      const parseNumber = v => {
        if (v == null) return null;
        if (typeof v === 'number' && isFinite(v)) return v;
        const n = parseFloat(String(v).replace(/[^\d.-]/g, ''));
        return isFinite(n) ? n : null;
      };
  
      ul.innerHTML = products.map(p => {
        const origPrice = parseNumber(p.price) || 0;
        const salePrice = parseNumber(p.sale_price);
        const benefitPrice = parseNumber(p.benefit_price);
        const isSale = salePrice != null && salePrice < origPrice;
        const isCoupon = benefitPrice != null && benefitPrice < (isSale ? salePrice : origPrice);
  
        let displayPercent = null;
        if (isCoupon) {
          const base = isSale ? salePrice : origPrice;
          if (base > 0 && benefitPrice >= 0) {
            displayPercent = Math.round((base - benefitPrice) / base * 100);
          }
        } else if (isSale && origPrice > 0) {
          displayPercent = Math.round((origPrice - salePrice) / origPrice * 100);
        }
  
        const priceText = formatKRW(origPrice);
        const finalPrice = isCoupon ? formatKRW(benefitPrice) : isSale ? formatKRW(salePrice) : priceText;
        // 정가가 표시될 조건: 할인 또는 쿠폰이 적용된 경우
        const showOriginal = isSale || isCoupon;
  
        const initialImg = p.image_medium || p.list_image;
        const hoverImg = p.image_thumbnail || p.image_small;
        const productLink = `/product/detail.html?product_no=${p.product_no}`;
        const hoverAttrs = hoverImg && initialImg && hoverImg !== initialImg
          ? `onmouseover="this.src='${hoverImg}'" onmouseout="this.src='${initialImg}'"` : '';
  
        // 영문 상품명 → 요약설명 → 브리핑설명 순으로 fallback (이름 위 작은 회색)
        const subText = p.eng_product_name || p.summary_description || p.simple_description || '';
  
        return `
          <li>
            <a href="${productLink}" class="prd_link" data-track-click="product" data-product-no="${p.product_no}" target="_blank" rel="noopener noreferrer">
              <div class="prd_thumb">
                ${initialImg ? `<img src="${initialImg}" ${hoverAttrs} alt="${escapeHtml(p.product_name || '')}" />` : ''}
              </div>
              ${subText ? `<div class="prd_desc">${escapeHtml(subText)}</div>` : ''}
              <div class="prd_name">${escapeHtml(p.product_name || '')}</div>
            </a>
            <div class="prd_price">
              ${displayPercent && displayPercent > 0 ? `<span class="prd_percent">${displayPercent}%</span>` : ''}
              ${showOriginal ? `<span class="prd_original">${priceText}</span>` : ''}
              <span class="prd_final">${finalPrice}</span>
            </div>
          </li>`;
      }).join('');
    }
  
    // 스피너/탭만 위젯 자체에서 책임. 상품 카드 디자인은 자사몰 /css/goodsData.html 에 위임.
    // 단, 가격 영역(.goods_price)이 goodsData.html 에 없을 가능성에 대비해 최소 fallback 스타일만 인라인 제공.
    const style = document.createElement('style');
    style.textContent = `
      .grid-spinner { width: 40px; height: 40px; border: 4px solid #f3f3f3; border-top: 4px solid #1890ff; border-radius: 50%; animation: spin_${pageId} 1s linear infinite; margin: 20px auto; }
      @keyframes spin_${pageId} { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg);} }
      .tabs_${pageId} { display: flex; gap: 8px; max-width: 800px; margin: 16px auto; }
      .tabs_${pageId} button { flex: 1; padding: 8px; font-size: 16px; border: 1px solid #d9d9d9; background: #f5f5f5; color: #333; cursor: pointer; border-radius: 4px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
      .tabs_${pageId} button.active { font-weight: 600; }
  
      /* === 상품 카드 — 사이트 라이브 디자인 따라 자체 CSS 로 렌더 === */
      .main_Grid_${pageId} li { list-style: none; }
      .main_Grid_${pageId} .prd_link { display: block; text-decoration: none; color: inherit; }
      .main_Grid_${pageId} .prd_thumb { aspect-ratio: 1 / 1; background: #f8f9fa; overflow: hidden; }
      .main_Grid_${pageId} .prd_thumb img { width: 100%; height: 100%; object-fit: cover; display: block; }
      /* 영문 상품명(요약) — 작은 회색 텍스트, 이름 위 */
      .main_Grid_${pageId} .prd_desc {
        font-size: 13px;
        color: #888;
        line-height: 1.2;
        font-weight: 400;
        margin: 10px 0 2px;
      }
      /* 상품명 — 굵게 */
      .main_Grid_${pageId} .prd_name {
        font-size: 16px;
        font-weight: 600;
        color: #222;
        line-height: 1.3;
        margin-top: 2px;
        display: -webkit-box;
        -webkit-line-clamp: 2;
        -webkit-box-orient: vertical;
        overflow: hidden;
        text-overflow: ellipsis;
      }
      /* 가격 영역 — 10% 뱃지 + 정가(취소선) + 최종가 */
      .main_Grid_${pageId} .prd_price { display: flex; align-items: center; gap: 8px; margin-top: 10px; flex-wrap: wrap; }
      .main_Grid_${pageId} .prd_percent { background: #4FC3D7; color: #fff; padding: 3px 10px; border-radius: 14px; font-size: 12px; font-weight: 600; line-height: 1; }
      .main_Grid_${pageId} .prd_original { color: #999; text-decoration: line-through; font-size: 13px; }
      .main_Grid_${pageId} .prd_final { font-size: 18px; font-weight: 700; color: #111; margin-left: auto; }
  
      @media (max-width: 400px) {
        .main_Grid_${pageId} { width: 96%; margin: 0 auto; }
        .main_Grid_${pageId} .prd_desc { font-size: 12px; }
        .main_Grid_${pageId} .prd_name { font-size: 15px; }
        .main_Grid_${pageId} .prd_final { font-size: 16px; }
      }
    `;
    document.head.appendChild(style);
  
    // ────────────────────────────────────────────────────────────────
    // 5) 이벤트 데이터 로드 (이 앱 서버: /api/events/{pageId})
    // ────────────────────────────────────────────────────────────────
    async function initializePage() {
      try {
        if (!SELF_BASE) {
          console.error('widget.js: SELF_BASE 추출 실패');
          return;
        }
        const response = await fetch(`${SELF_BASE}/api/events/${pageId}`);
        if (!response.ok) throw new Error('Event data fetch failed');
        const json = await response.json();
        // 우리 서버 응답 형태: { success, data }
        const ev = json && json.data ? json.data : json;
  
        const root = getRootContainer();
  
        // sections 우선, 없으면 legacy content.blocks/images 로 fallback
        const blocks = Array.isArray(ev.sections) && ev.sections.length > 0
          ? ev.sections
          : (ev.content && Array.isArray(ev.content.blocks))
            ? ev.content.blocks
            : (ev.images || []).map(img => ({ type: 'image', ...img }));
  
        blocks.forEach(block => {
          switch (block.type) {
            case 'image': renderImageBlock(block, root); break;
            case 'video': renderVideoBlock(block, root); break;
            case 'text': renderTextBlock(block, root); break;
            case 'product_group': renderProductBlock(block, root); break;
            default: break;
          }
        });
  
        document.querySelectorAll(`ul.main_Grid_${pageId}`).forEach(ul => loadPanel(ul));
      } catch (err) {
        console.error('EVENT LOAD ERROR', err);
        const root = getRootContainer();
        root.innerHTML = `<div style="text-align:center; padding:60px 20px; color:#999; font-size:14px;">이벤트를 불러올 수 없습니다.</div>`;
      }
    }
  
    window.showTab = (id, btn, activeColor = '#1890ff') => {
      const parent = btn.closest('.tabs_' + pageId);
      if (!parent) return;
      parent.querySelectorAll('button').forEach(b => {
        b.classList.remove('active');
        b.style.backgroundColor = '#f5f5f5';
        b.style.color = '#333';
        b.style.borderColor = '#d9d9d9';
      });
      btn.classList.add('active');
      btn.style.backgroundColor = activeColor;
      btn.style.color = '#fff';
      btn.style.borderColor = activeColor;
      const contentParent = parent.parentElement;
      contentParent.querySelectorAll('.tab-content_' + pageId).forEach(el => {
        if (el.id === id) { el.style.display = 'block'; }
        else { el.style.display = 'none'; }
      });
    };
  
    // 원본 widget.js 의 쿠폰 다운로드 로직: 쿠폰별로 window.open 반복.
    // - 콤마 구분 문자열 또는 배열 모두 허용
    // - 다중 쿠폰은 각각 별도 창으로 IssueDownload 호출 (cafe24 표준)
    window.downloadCoupon = (coupons) => {
      const list = Array.isArray(coupons)
        ? coupons.map(s => String(s).trim()).filter(Boolean)
        : String(coupons || '').split(',').map(s => s.trim()).filter(Boolean);
      if (list.length === 0) { alert('쿠폰 정보가 없습니다.'); return; }
      list.forEach(cpn => {
        const url = `/exec/front/newcoupon/IssueDownload?coupon_no=${encodeURIComponent(cpn)}&opener_url=${encodeURIComponent(location.href)}`;
        window.open(url, '_blank');
      });
    };
  
    initializePage();
  })();
  