const { MongoClient } = require('mongodb');

let client;
let db;

async function connectDB() {
  if (db) return db;

  const uri = process.env.MONGODB_URI;
  const dbName = process.env.DB_NAME;
  if (!uri) throw new Error('MONGODB_URI 환경변수가 설정되지 않았습니다.');
  if (!dbName) throw new Error('DB_NAME 환경변수가 설정되지 않았습니다.');

  client = new MongoClient(uri);
  await client.connect();
  db = client.db(dbName);
  console.log(`✅ MongoDB 연결 완료 (db: ${dbName})`);
  return db;
}

function getDB() {
  if (!db) throw new Error('DB가 아직 연결되지 않았습니다. connectDB()를 먼저 호출하세요.');
  return db;
}

module.exports = { connectDB, getDB };
