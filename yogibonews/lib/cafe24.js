const { MongoClient } = require('mongodb');
const config = require('../config');

/**
 * Cafe24 Admin API 읽기 전용 클라이언트.
 * 토큰은 yogiChat이 공유 MongoDB(tokens)에 저장·갱신하는 것을 읽기만 한다.
 * 여기서 refresh 하면 1회용 refresh_token이 회전돼 다른 서버 토큰이 깨지므로 절대 갱신하지 않는다.
 * 401이면 DB에서 최신 토큰을 한 번 다시 읽어 재시도한다.
 */

let client;
let injectedDb = null; // adminChat 안에서는 이미 연결된 DB(tokens가 있는 곳)를 넘겨받아 연결을 하나 더 만들지 않는다
let cache = { token: null, at: 0 };

function adminBase() {
  return `https://${config.cafe24.mallId}.cafe24api.com/api/v2/admin`;
}

function useTokenDb(getDb) {
  injectedDb = getDb;
}

function enabled() {
  return Boolean(injectedDb || config.cafe24.tokenUri);
}

async function tokenDb() {
  if (injectedDb) return injectedDb();
  if (!client) {
    client = new MongoClient(config.cafe24.tokenUri, { serverSelectionTimeoutMS: 8000 });
    await client.connect();
  }
  return client.db(config.cafe24.tokenDb);
}

async function readToken(force) {
  if (!force && cache.token && Date.now() - cache.at < 60000) return cache.token;
  const doc = await (await tokenDb()).collection(config.cafe24.tokenCollection).findOne({});
  if (!doc?.accessToken) throw new Error('Cafe24 토큰이 없습니다 (공유 DB tokens 컬렉션 확인)');
  cache = { token: doc.accessToken, at: Date.now() };
  return doc.accessToken;
}

async function adminGet(endpoint, params = {}, retried = false, rateLimitTries = 0) {
  const token = await readToken(retried);
  const url = new URL(adminBase() + endpoint);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== '') url.searchParams.set(k, String(v));
  }
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}`, 'X-Cafe24-Api-Version': config.cafe24.apiVersion },
  });

  if (res.status === 401 && !retried) return adminGet(endpoint, params, true, rateLimitTries);
  if (res.status === 429 && rateLimitTries < 5) {
    const wait = (Number(res.headers.get('Retry-After')) || 0) * 1000 || 400 * 2 ** rateLimitTries;
    await new Promise((r) => setTimeout(r, wait));
    return adminGet(endpoint, params, retried, rateLimitTries + 1);
  }
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`Cafe24 HTTP ${res.status}: ${body?.error?.message || JSON.stringify(body).slice(0, 160)}`);
  return body;
}

async function adminPaginate(endpoint, params, key, { limit = 100, maxPages = 50 } = {}) {
  const out = [];
  for (let page = 0; page < maxPages; page++) {
    const body = await adminGet(endpoint, { ...params, limit, offset: page * limit });
    const rows = body[key] || [];
    out.push(...rows);
    if (rows.length < limit) break;
  }
  return out;
}

module.exports = { enabled, adminPaginate, useTokenDb };
