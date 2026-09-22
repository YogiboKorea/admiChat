/**
 * 기존 adminChat 뉴스레터(yogiboJPnews, brandKnowledge)를 yogibonews DB로 이관·재동기화한다. (전환 시점에 한 번 더 실행)
 * - 원본은 adminChat DB(MONGODB_URI/DB_NAME), 대상은 YOGIBONEWS_MONGODB_URI/YOGIBONEWS_DB_NAME — 둘 다 adminChat/server/.env에서 읽는다.
 * - 원본(adminChat) DB는 읽기만 한다.
 * - 여러 번 실행해도 안전하다:
 *   · 새 DB에 없는 글 → 추가. 그중 기존 수집기가 새로 가져온 일본어 RSS 원본은 새 파이프라인 판별 대기로 넣는다.
 *   · 이미 있는 글 → 원본 쪽이 더 최근에 수정된 경우만 갱신하고, 새 DB에서 붙인 정보(pipeline·보관·원문)는 유지한다.
 *
 * 사용법 (adminChat/server에서): npm run yogibonews:migrate
 */
require('dotenv').config({ path: require('path').join(__dirname, '..', '..', '.env') });
const { MongoClient } = require('mongodb');
const config = require('../config');

const KANA = /[぀-ヿ]/;
const PRESERVE = ['pipeline', 'archived', 'archivedAt', 'originalTitle', 'originalContent', 'publishedAt'];

function stamp(doc) {
  return new Date(doc.updatedAt || doc.createdAt || 0).getTime();
}

function isNewRssOriginal(doc) {
  return doc.source !== 'manual' && doc.status === 'draft' && KANA.test(doc.title || '');
}

async function syncCollection(sourceDb, targetDb, name, { queueNewRss = false } = {}) {
  const source = sourceDb.collection(name);
  const target = targetDb.collection(name);
  const existing = new Map((await target.find({}).toArray()).map((d) => [String(d._id), d]));
  const stats = { total: 0, inserted: 0, queued: 0, updated: 0, unchanged: 0 };

  for await (const doc of source.find({})) {
    stats.total++;
    const current = existing.get(String(doc._id));

    if (!current) {
      const toInsert = { ...doc };
      if (queueNewRss && isNewRssOriginal(doc)) {
        toInsert.originalTitle = doc.title;
        toInsert.originalContent = doc.content;
        toInsert.pipeline = { skipped: true, queuedAt: new Date() };
        stats.queued++;
      }
      await target.insertOne(toInsert);
      stats.inserted++;
      continue;
    }

    if (stamp(doc) > stamp(current)) {
      const { _id, ...fields } = doc;
      for (const key of PRESERVE) delete fields[key];
      await target.updateOne({ _id }, { $set: fields });
      stats.updated++;
    } else {
      stats.unchanged++;
    }
  }
  return stats;
}

async function main() {
  const { MONGODB_URI, DB_NAME } = process.env;
  if (!MONGODB_URI || !DB_NAME) throw new Error('adminChat MONGODB_URI / DB_NAME 환경변수를 설정하세요.');
  if (!config.mongoUri) throw new Error('YOGIBONEWS_MONGODB_URI 환경변수를 설정하세요.');
  if (MONGODB_URI === config.mongoUri && DB_NAME === config.dbName) throw new Error('원본과 대상이 같은 DB입니다.');

  console.log(`원본: ${DB_NAME}  →  대상: ${config.dbName}`);
  const legacy = new MongoClient(MONGODB_URI);
  const target = new MongoClient(config.mongoUri);
  try {
    await legacy.connect();
    await target.connect();
    const news = await syncCollection(legacy.db(DB_NAME), target.db(config.dbName), 'yogiboJPnews', { queueNewRss: true });
    console.log(`✅ yogiboJPnews: 총 ${news.total} · 추가 ${news.inserted}(판별 대기 ${news.queued}) · 갱신 ${news.updated} · 변화없음 ${news.unchanged}`);
    const brand = await syncCollection(legacy.db(DB_NAME), target.db(config.dbName), 'brandKnowledge');
    console.log(`✅ brandKnowledge: 총 ${brand.total} · 추가 ${brand.inserted} · 갱신 ${brand.updated} · 변화없음 ${brand.unchanged}`);
    console.log('🎉 완료. 원본(adminChat) DB는 변경하지 않았습니다.');
  } finally {
    await legacy.close();
    await target.close();
  }
}

main().catch((err) => {
  console.error('❌ 마이그레이션 실패:', err.message);
  process.exit(1);
});
