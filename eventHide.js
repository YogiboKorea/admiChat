// ================================================================
// 이벤트 영역 숨김 — IP 기록·조회 (2026.11)
//
//  쇼핑몰의 이벤트 영역(.event_block)을 숨길 회원 아이디는 스킨 코드(page.html 의 HIDE_MEMBER_IDS)에 적는다.
//  이 모듈은 IP 만 다룬다.
//   ① 페이지가 대상 회원을 알아보면 ?mark=1 로 부른다 → 그 접속의 IP 를 기록한다
//   ② 그 밖의 접속 → 기록된 IP 인지 답한다 (로그인 여부와 상관없이 숨김)
//
//  · IP 는 다른 사람과 공유되거나 다른 사람에게 넘어간다(회사·가정 공유기, 통신사 모바일망).
//    그래서 기록은 마지막 접속 후 IP_KEEP_DAYS 일이 지나면 자동으로 지운다. 대상 회원이 다시 오면 기한이 늘어난다.
//  · mark 요청은 누구나 보낼 수 있다(아이디 확인을 페이지가 하므로). 잘못 쌓인 기록도 기한이 지나면 사라진다.
//
//  라우트는 server.js 에서 mount(app, deps) 로 붙인다.
// ================================================================
'use strict';

const net = require('net');

const COLLECTION = 'eventHideIp';
const IP_KEEP_DAYS = Math.max(1, Number(process.env.EVENT_HIDE_IP_DAYS) || 30);

function clientIp(req) {
  const raw = String(req.headers['x-forwarded-for'] || req.ip || '').split(',')[0].trim();
  const ip = raw.replace(/^::ffff:/, '');      // IPv4 를 IPv6 형태로 받은 경우 같은 IP 로 맞춘다
  return net.isIP(ip) ? ip : '';               // IP 형식이 아니면 기록·조회하지 않는다
}

function mount(app, deps) {
  const col = () => deps.getDb().collection(COLLECTION);

  app.get('/api/event-hide/check', async (req, res) => {
    // 답이 접속자마다 다르다 — 브라우저·중간 서버가 저장해 두면 안 된다
    res.set('Cache-Control', 'no-store');
    const ip = clientIp(req);

    if (String(req.query.mark) === '1') {
      // 기록이 실패해도 대상 회원에게는 숨김으로 답한다
      if (ip) {
        const now = new Date();
        col().updateOne(
          { ip },
          { $set: { lastAt: now, expiresAt: new Date(now.getTime() + IP_KEEP_DAYS * 86400000) }, $setOnInsert: { firstAt: now } },
          { upsert: true }
        ).catch(e => { if (e.code !== 11000) console.warn('[이벤트숨김] IP 기록 실패:', e.message); });   // 11000: 동시 접속으로 이미 기록됨
      }
      return res.json({ ok: true, hide: true });
    }

    if (!ip) return res.json({ ok: true, hide: false });
    try {
      const found = await col().findOne({ ip }, { projection: { _id: 1 } });
      return res.json({ ok: true, hide: !!found });
    } catch (err) {
      console.error('[이벤트숨김] 조회 오류:', err.message);
      return res.status(500).json({ ok: false });
    }
  });

  console.log('✅ [이벤트숨김] 라우트 등록 완료 · IP 보관', IP_KEEP_DAYS + '일');
}

module.exports = { mount, COLLECTION };
