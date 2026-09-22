/**
 * 로컬 개발용 실행기 — adminChat 전체(토큰 로드·각종 크론)를 띄우지 않고 매거진 모듈만 붙여서 실행한다.
 *   npm run dev:yogibonews  →  http://localhost:6103/yogibonews
 * adminChat/server/.env 를 그대로 읽는다. 로컬에서는 YOGIBONEWS_CLAUDE_ENABLED=false, YOGIBONEWS_FETCH_CRON=off 권장.
 */
require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });
const express = require('express');
const cors = require('cors');
const yogibonews = require('.');

const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

yogibonews.mount(app);

const port = Number(process.env.YOGIBONEWS_DEV_PORT) || 6103;
app.listen(port, () => {
  console.log(`🚀 [yogibonews] dev server http://localhost:${port}${yogibonews.BASE_PATH}`);
});
