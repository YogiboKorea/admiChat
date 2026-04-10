/**
 * 카트 이벤트 - 3,000원 적립금 1회 지급
 * 카트 페이지(.prdBox 에 상품 1개 이상) 접근 시 팝업 노출
 * 팝업 이미지 클릭 → 서버에 포인트 지급 요청 (1회 한정)
 */
(function () {
  var CAFE24_APP_KEY = 'R4as6neTNyXXjVfpfUj9lE';
  var API_BASE_URL = 'https://port-0-ychat-lzgmwhc4d9883c97.sel4.cloudtype.app';
  var POP_IMG = 'http://yogibo.kr/web/img/cart_pop.png';

  var _memberId = null;
  var _popupEl = null;
  var _isRequesting = false; // 중복 클릭 방지

  /* ─────────────────────────────────────────
   * 1. CAFE24API 폴링
   * ───────────────────────────────────────── */
  function waitForCafe24API(callback, n) {
    n = n || 0;
    if (n > 10) return;
    if (typeof CAFE24API !== 'undefined') { callback(); }
    else { setTimeout(function () { waitForCafe24API(callback, n + 1); }, 500); }
  }

  /* ─────────────────────────────────────────
   * 2. 장바구니 상품 유무 확인
   * ───────────────────────────────────────── */
  function hasCartItems() {
    return document.querySelectorAll('.prdBox').length > 0;
  }

  /* ─────────────────────────────────────────
   * 3. 팝업 렌더링
   * ───────────────────────────────────────── */
  function showPopup() {
    if (_popupEl) return;

    // ── dim ──
    var dim = document.createElement('div');
    dim.id = '__cart_event_dim__';
    dim.style.cssText = [
      'position:fixed', 'inset:0',
      'background:rgba(0,0,0,0.55)',
      'z-index:2147483640',
      'display:flex', 'align-items:center', 'justify-content:center',
      'animation:__fadeIn__ 0.25s ease'
    ].join(';');

    // ── 팝업 래퍼 ──
    var box = document.createElement('div');
    box.style.cssText = [
      'position:relative',
      'max-width:360px', 'width:90%',
      'border-radius:16px',
      'overflow:hidden',
      'box-shadow:0 8px 40px rgba(0,0,0,0.35)',
      'animation:__slideUp__ 0.3s ease'
    ].join(';');

    // ── 닫기 버튼 ──
    var closeBtn = document.createElement('button');
    closeBtn.textContent = '✕';
    closeBtn.style.cssText = [
      'position:absolute', 'top:10px', 'right:12px',
      'background:rgba(0,0,0,0.45)',
      'color:#fff', 'border:none',
      'width:30px', 'height:30px',
      'border-radius:50%',
      'font-size:15px', 'font-weight:bold',
      'cursor:pointer', 'z-index:10',
      'display:flex', 'align-items:center', 'justify-content:center',
      'line-height:1'
    ].join(';');
    closeBtn.onclick = function (e) { e.stopPropagation(); removePopup(); };

    // ── 팝업 이미지 (클릭 = 적립금 지급) ──
    var img = document.createElement('img');
    img.src = POP_IMG;
    img.alt = '3,000원 적립금 이벤트';
    img.style.cssText = [
      'display:block', 'width:100%',
      'cursor:pointer',
      'transition:filter 0.15s'
    ].join(';');
    img.addEventListener('mouseover', function () { img.style.filter = 'brightness(0.93)'; });
    img.addEventListener('mouseout', function () { img.style.filter = ''; });
    img.onclick = function () { handleRewardClick(); };

    box.appendChild(closeBtn);
    box.appendChild(img);
    dim.appendChild(box);

    // ── dim 자체 클릭(배경)으로도 닫기 ──
    dim.addEventListener('click', function (e) {
      if (e.target === dim) removePopup();
    });

    // ── 키프레임 ──
    if (!document.getElementById('__cart_event_style__')) {
      var style = document.createElement('style');
      style.id = '__cart_event_style__';
      style.textContent = [
        '@keyframes __fadeIn__{from{opacity:0}to{opacity:1}}',
        '@keyframes __slideUp__{from{transform:translateY(24px);opacity:0}to{transform:translateY(0);opacity:1}}'
      ].join('');
      document.head.appendChild(style);
    }

    document.body.appendChild(dim);
    _popupEl = dim;
  }

  function removePopup() {
    if (_popupEl) { _popupEl.remove(); _popupEl = null; }
  }

  /* ─────────────────────────────────────────
   * 4. 팝업 이미지 클릭 → 적립금 지급
   * ───────────────────────────────────────── */
  function handleRewardClick() {
    if (_isRequesting) return;

    if (!_memberId) {
      showToast('로그인 후 이용하실 수 있습니다.');
      return;
    }

    _isRequesting = true;

    fetch(API_BASE_URL + '/api/event/cart-reward', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ memberId: _memberId })
    })
      .then(function (res) { return res.json(); })
      .then(function (data) {
        _isRequesting = false;
        removePopup();

        if (data.success) {
          showToast('🎉 3,000원 적립금이 지급되었습니다!');
        } else if (data.alreadyDone) {
          showToast('이미 적립 혜택을 받으셨습니다.');
        } else {
          showToast(data.message || '잠시 후 다시 시도해주세요.');
        }
      })
      .catch(function () {
        _isRequesting = false;
        showToast('서버 연결에 실패했습니다. 잠시 후 다시 시도해주세요.');
      });
  }

  /* ─────────────────────────────────────────
   * 5. 토스트 알림
   * ───────────────────────────────────────── */
  function showToast(message) {
    var toast = document.createElement('div');
    toast.textContent = message;
    toast.style.cssText = [
      'position:fixed', 'bottom:80px', 'left:50%',
      'transform:translateX(-50%)',
      'background:rgba(0,0,0,0.78)', 'color:#fff',
      'padding:12px 22px', 'border-radius:30px',
      'font-size:14px', 'font-weight:bold',
      'z-index:2147483647', 'white-space:nowrap',
      'box-shadow:0 4px 15px rgba(0,0,0,0.25)',
      'transition:opacity 0.5s'
    ].join(';');
    document.body.appendChild(toast);
    setTimeout(function () {
      toast.style.opacity = '0';
      setTimeout(function () { toast.remove(); }, 500);
    }, 3500);
  }

  /* ─────────────────────────────────────────
   * 6. 메인 실행
   * ───────────────────────────────────────── */
  function init() {
    if (!hasCartItems()) {
      console.log('[카트이벤트] 장바구니 상품 없음 - 미실행');
      return;
    }

    waitForCafe24API(function () {
      try {
        CAFE24API.init(CAFE24_APP_KEY);
        CAFE24API.getMemberID(function (memberId) {
          if (!memberId || memberId === 'GUEST' || String(memberId).startsWith('guest_')) {
            console.log('[카트이벤트] 비회원 - 미실행');
            return;
          }
          _memberId = memberId;
          console.log('[카트이벤트] 회원 확인:', memberId);
          showPopup();
        });
      } catch (e) {
        console.error('[카트이벤트] CAFE24API 오류:', e);
      }
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
