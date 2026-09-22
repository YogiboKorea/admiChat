const express = require('express');
const { getDB } = require('../db');
const { hasKey } = require('../lib/claude');
const { getKrCatalog } = require('../lib/krProducts');
const { runNewsPipeline } = require('../pipeline/runPipeline');
const config = require('../config');

const router = express.Router();

function describeSchedule(expr) {
  if (expr === 'off') return '자동 수집 꺼짐 (로컬 개발 모드)';
  const everyHours = expr.match(/^0 \*\/(\d+) \* \* \*$/);
  if (everyHours) return `${everyHours[1]}시간마다 자동 수집`;
  return `자동 수집 일정: ${expr}`;
}

// 어드민 상단 안내 배너: 어디서, 얼마나 자주, 어떤 기준으로 불러오는지 + 마지막 수집 결과
router.get('/status', async (req, res) => {
  try {
    const db = getDB();
    const schedule = config.fetchCron;
    const [lastRun, waiting, kr] = await Promise.all([
      db.collection('pipelineRuns').find({}).sort({ startedAt: -1 }).limit(1).next(),
      db.collection('yogiboJPnews').countDocuments({ source: { $ne: 'manual' }, status: 'draft', 'pipeline.skipped': true }),
      getKrCatalog(),
    ]);

    res.json({
      success: true,
      data: {
        feedUrl: config.feedUrl,
        schedule,
        scheduleText: describeSchedule(schedule),
        claudeEnabled: hasKey(),
        excludeRules: ['일본 지역 행사·협찬', '일본 매장·배송 공지', '국내 미판매 제품', '일본 한정 콜라보', '기간 한정 행사·캠페인'],
        krCatalog: { source: kr.source, count: kr.all.length },
        waiting,
        lastRun,
      },
    });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// 어드민 '지금 수집' 버튼 — 유료 Claude 호출이 날 수 있어 GET이 아닌 POST로만 받는다
router.post('/run', async (req, res) => {
  try {
    const result = await runNewsPipeline();
    res.json({ success: true, message: '동기화가 완료되었습니다.', result });
  } catch (error) {
    console.error('파이프라인 실행 에러:', error);
    res.status(error.status || 500).json({ success: false, message: error.message });
  }
});

module.exports = router;
