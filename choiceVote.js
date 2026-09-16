/**
 * 「이미지 인기투표」 — 직원들이 폴더별 이미지에 표를 주는 간단한 투표 (2026-09)
 *
 * · 로그인 없음. 첫 화면에서 이름만 적으면 그 이름으로 표가 쌓인다(같은 이름 = 같은 사람으로 본다).
 * · 한 사람이 한 이미지에 한 표. 다시 누르면(페이지에서는 더블클릭) 표가 빠진다.
 * · 이미지는 Cafe24 웹호스팅에 올라가 있고(폴더 목록은 페이지가 들고 있다), 여기서는 표만 센다.
 * · 컬렉션 choiceVote : { imageId, voter, at } — (imageId, voter) 하나만 남게 유니크 인덱스.
 */
'use strict';

const COLLECTION = 'choiceVote';
const MAX_NAME = 20;
const ID_RE = /^[0-9A-Za-z가-힣._\-]{1,80}\/[0-9A-Za-z가-힣._\-]{1,120}$/;   // "폴더명/파일명"

function cleanVoter(v) {
  const s = String(v == null ? '' : v).trim().replace(/\s+/g, ' ');
  if (!s || s.length > MAX_NAME) return null;
  return s;
}
function cleanId(v) {
  const s = String(v == null ? '' : v).trim();
  return ID_RE.test(s) ? s : null;
}

function mount(app, deps) {
  const getDb = deps.getDb;
  const col = () => getDb().collection(COLLECTION);

  /** 전체 표 + 내 표 — 페이지가 3초마다 새로 받아 간다 */
  app.get('/api/choice/state', async (req, res) => {
    try {
      const voter = cleanVoter(req.query.voter);
      const rows = await col().find({}, { projection: { imageId: 1, voter: 1 } }).toArray();
      const totals = {};
      const mine = [];
      const voters = {};
      rows.forEach(r => {
        if (!r || !r.imageId) return;
        totals[r.imageId] = (totals[r.imageId] || 0) + 1;
        if (r.voter) voters[r.voter] = true;
        if (voter && r.voter === voter) mine.push(r.imageId);
      });
      return res.json({ ok: true, totals, mine, voterCount: Object.keys(voters).length, voteCount: rows.length });
    } catch (err) {
      console.error('[투표] 상태 오류:', err.message);
      return res.status(500).json({ ok: false, message: '잠시 후 다시 시도해주세요.' });
    }
  });

  /** 표 주기 / 표 빼기 — { voter, id, on } */
  app.post('/api/choice/vote', async (req, res) => {
    try {
      const body = req.body || {};
      const voter = cleanVoter(body.voter);
      const imageId = cleanId(body.id);
      if (!voter) return res.status(400).json({ ok: false, message: '이름을 먼저 적어주세요.' });
      if (!imageId) return res.status(400).json({ ok: false, message: '잘못된 요청입니다.' });
      const on = !(body.on === false || body.on === 'false' || body.on === 0 || body.on === '0');

      if (on) {
        await col().updateOne(
          { imageId, voter },
          { $setOnInsert: { imageId, voter, at: new Date() } },
          { upsert: true },
        );
      } else {
        await col().deleteOne({ imageId, voter });
      }
      const count = await col().countDocuments({ imageId });
      return res.json({ ok: true, id: imageId, on, count });
    } catch (err) {
      // 같은 사람이 아주 빠르게 두 번 눌러 유니크 인덱스에 걸린 경우도 성공으로 본다
      if (err && err.code === 11000) {
        const imageId = cleanId((req.body || {}).id);
        const count = imageId ? await col().countDocuments({ imageId }).catch(() => null) : null;
        return res.json({ ok: true, id: imageId, on: true, count });
      }
      console.error('[투표] 저장 오류:', err.message);
      return res.status(500).json({ ok: false, message: '잠시 후 다시 시도해주세요.' });
    }
  });

  /** 많이 받은 순 — 결과 정리할 때 본다 (표를 준 사람 이름도 같이) */
  app.get('/api/choice/best', async (req, res) => {
    try {
      const limit = Math.min(200, Math.max(1, Number(req.query.limit) || 30));
      const rows = await col().aggregate([
        { $group: { _id: '$imageId', count: { $sum: 1 }, voters: { $addToSet: '$voter' } } },
        { $sort: { count: -1, _id: 1 } },
        { $limit: limit },
      ]).toArray();
      return res.json({ ok: true, items: rows.map(r => ({ id: r._id, count: r.count, voters: r.voters })) });
    } catch (err) {
      console.error('[투표] 집계 오류:', err.message);
      return res.status(500).json({ ok: false, message: '잠시 후 다시 시도해주세요.' });
    }
  });

  console.log('🗳️  이미지 인기투표 API 준비됨 (/api/choice)');
}

module.exports = { mount, COLLECTION, __internals: { cleanVoter, cleanId } };
