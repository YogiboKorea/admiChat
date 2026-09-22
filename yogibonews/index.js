/**
 * 요기보 매거진(일본 블로그 → 국내 매거진) 모듈.
 * adminChat 서버에 붙어 /yogibonews 아래로 전부 등록된다.
 *   - 어드민(Vercel Next)  : /yogibonews/api/yogibo-jp-news, /api/pipeline, /api/brand-knowledge, /api/translate-news
 *   - Cafe24 매거진 공개 API: /yogibonews/api/magazine
 * DB는 adminChat과 별도(YOGIBONEWS_MONGODB_URI)이고, 연결에 실패해도 adminChat의 다른 기능은 그대로 돈다(이 경로만 503).
 */
const express = require('express');
const cron = require('node-cron');

const config = require('./config');
const { connectDB, isConnected } = require('./db');
const cafe24 = require('./lib/cafe24');
const newsRouter = require('./routes/news');
const brandKnowledgeRouter = require('./routes/brandKnowledge');
const pipelineRouter = require('./routes/pipeline');
const magazineRouter = require('./routes/magazine');
const { runNewsPipeline } = require('./pipeline/runPipeline');

const BASE_PATH = '/yogibonews';

function buildRouter() {
  const router = express.Router();

  router.get('/api/ping', (req, res) => res.json({ ok: true, db: isConnected() }));

  router.use((req, res, next) => {
    if (isConnected()) return next();
    res.status(503).json({ success: false, message: '매거진 서버가 아직 준비되지 않았습니다. (DB 미연결)' });
  });

  router.use('/api/yogibo-jp-news', newsRouter);
  router.use('/api/brand-knowledge', brandKnowledgeRouter);
  router.use('/api/pipeline', pipelineRouter);
  router.use('/api/magazine', magazineRouter);
  router.post('/api/translate-news', newsRouter.translateHandler);

  router.use((err, req, res, next) => {
    console.error('[yogibonews] Unhandled Error', err);
    res.status(500).json({ success: false, message: err.message || 'Server Error' });
  });

  return router;
}

async function start() {
  try {
    await connectDB();
  } catch (err) {
    console.error(`❌ [yogibonews] 시작 실패 — ${BASE_PATH} 요청은 503으로 응답합니다:`, err.message);
    return;
  }

  const cronExpr = config.fetchCron;
  if (cronExpr === 'off') {
    console.log('⏸️ [yogibonews] 자동 수집 비활성화 (YOGIBONEWS_FETCH_CRON=off)');
    return;
  }
  const task = cron.schedule(
    cronExpr,
    () => {
      runNewsPipeline({ trigger: 'cron' }).catch((err) => console.error('❌ [yogibonews] 자동 수집 실패:', err.message));
    },
    { timezone: config.cronTimezone }
  );
  const next = task.getNextRun?.();
  console.log(
    `⏰ [yogibonews] 자동 수집 스케줄 등록: ${cronExpr} (${config.cronTimezone})` +
      (next ? ` · 다음 실행 ${next.toLocaleString('ko-KR', { timeZone: config.cronTimezone })}` : '')
  );
}

/**
 * @param app      express 앱
 * @param getDb    Cafe24 토큰(tokens 컬렉션)이 있는 DB를 돌려주는 함수 — adminChat의 연결을 같이 쓴다 (읽기 전용)
 */
function mount(app, { getDb } = {}) {
  if (getDb) cafe24.useTokenDb(getDb);
  app.use(BASE_PATH, buildRouter());
  start();
}

module.exports = { mount, BASE_PATH };
