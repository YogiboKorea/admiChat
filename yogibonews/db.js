const { MongoClient } = require('mongodb');
const config = require('./config');

let client;
let db;

async function connectDB() {
  if (db) return db;

  const uri = config.mongoUri;
  const dbName = config.dbName;
  if (!uri) throw new Error('YOGIBONEWS_MONGODB_URI 환경변수가 설정되지 않았습니다.');

  client = new MongoClient(uri);
  await client.connect();
  db = client.db(dbName);
  console.log(`✅ [yogibonews] MongoDB 연결 완료 (db: ${dbName})`);
  return db;
}

function isConnected() {
  return Boolean(db);
}

function getDB() {
  if (!db) throw new Error('[yogibonews] DB가 아직 연결되지 않았습니다. connectDB()를 먼저 호출하세요.');
  return db;
}

module.exports = { connectDB, getDB, isConnected };
