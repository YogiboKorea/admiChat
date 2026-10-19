/**
 * Cafe24 토큰 선제 갱신 (2026-10, yChat config/tokenKeepalive.js 방식을 옮겨 옴)
 *
 * · 매시 30분(KST)에 tokens 문서의 updatedAt을 보고, 50분 넘게 안 바뀌었으면 refreshAccessToken()으로 미리 갱신한다.
 * · 이 서버는 원래 401이 날 때만 갱신했다. 그러면 토큰을 읽기만 하는 곳(광고 대시보드·onlineData·MCP·매거진)이
 *   만료된 토큰을 보는 시간이 길어지고, 2주 동안 아무도 갱신하지 않으면 리프레시 토큰까지 만료된다.
 * · yChat도 매시 정각에 같은 일을 한다. 둘 다 켜져 있는 동안엔 30분 엇갈려 돌고, 50분 안에 갱신됐으면 건너뛴다.
 * · ENABLE_TOKEN_KEEPALIVE=0 이면 끈다.
 */
'use strict';

const cron = require('node-cron');

const FRESH_MS = 50 * 60 * 1000;   // 이보다 최근에 갱신됐으면 건너뛴다
const CRON_EXPR = '30 * * * *';    // yChat(정각)과 겹치지 않게 30분

function start(deps) {
  const { getDb, refreshAccessToken } = deps;
  const collection = deps.collection || 'tokens';

  if (process.env.ENABLE_TOKEN_KEEPALIVE === '0') {
    console.log('[token-keepalive] ENABLE_TOKEN_KEEPALIVE=0 → 선제 토큰 갱신 꺼짐');
    return;
  }

  let running = false;
  async function tick(reason) {
    if (running) return;
    running = true;
    try {
      const doc = await getDb().collection(collection).findOne({}, { projection: { updatedAt: 1 } });
      const age = doc && doc.updatedAt ? Date.now() - new Date(doc.updatedAt).getTime() : Infinity;
      if (age < FRESH_MS) {
        console.log(`[token-keepalive] ${Math.round(age / 60000)}분 전에 갱신됨 — 건너뜀 (${reason})`);
        return;
      }
      await refreshAccessToken();
      console.log(`[token-keepalive] ✅ 선제 갱신 완료 (${reason})`);
    } catch (err) {
      console.error(`[token-keepalive] ❌ 선제 갱신 실패 (${reason}):`, err.response ? err.response.data : err.message);
    } finally {
      running = false;
    }
  }

  cron.schedule(CRON_EXPR, () => tick('매시 30분'), { timezone: 'Asia/Seoul' });
  setTimeout(() => tick('서버 시작'), 30 * 1000);   // DB 연결이 안정된 뒤 한 번
  console.log(`[token-keepalive] Cafe24 토큰 선제 갱신 예약됨 (${CRON_EXPR}, KST)`);

  return { tick };
}

module.exports = { start, FRESH_MS };
