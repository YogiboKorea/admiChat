const express = require('express');
const { ObjectId } = require('mongodb');
const { getDB } = require('../db');
const { sanitizeContent } = require('../lib/sanitize');

// yogibo.kr(Cafe24) 매거진 페이지가 부르는 공개 API. 발행(published)된 글만, 필요한 필드만 내보낸다.
const router = express.Router();

const PUBLISHED = { status: 'published' };
const LIST_SORT = { position: 1, pubDate: -1 };

function toId(id) {
  if (typeof id !== 'string' || !id) return null;
  if (ObjectId.isValid(id) && new ObjectId(id).toHexString() === id) return new ObjectId(id);
  return id;
}

function firstImage(html) {
  return (String(html || '').match(/<img[^>]+src=["']([^"']+)["']/i) || [])[1] || null;
}

// v5 기사 맨 앞 분류 라벨(소식/이벤트/공지사항)을 우선 쓰고, 없으면 제목으로 추정한다
function categoryOf(doc) {
  const label = String(doc.content || '').match(/<p[^>]*color:\s*#fff[^>]*>\s*(소식|이벤트|공지사항)\s*<\/p>/i);
  if (label) return label[1];
  const title = doc.title || '';
  if (/이벤트|캠페인|프로모션|할인|세일|특가/.test(title)) return '이벤트';
  if (/공지|안내|변경|운영/.test(title)) return '공지사항';
  return '소식';
}

function summary(doc) {
  return {
    id: String(doc._id),
    title: doc.title || '',
    thumbnail: doc.thumbnail || firstImage(doc.content),
    category: categoryOf(doc),
    publishedAt: doc.publishedAt || doc.pubDate || doc.createdAt || null,
  };
}

// 목록: ?limit=4&offset=0 → { items, total }
router.get('/', async (req, res) => {
  try {
    const limit = Math.min(Math.max(Number(req.query.limit) || 12, 1), 60);
    const offset = Math.max(Number(req.query.offset) || 0, 0);
    const col = getDB().collection('yogiboJPnews');
    const [docs, total] = await Promise.all([
      col.find(PUBLISHED, { projection: { title: 1, thumbnail: 1, content: 1, position: 1, pubDate: 1, publishedAt: 1, createdAt: 1 } })
        .sort(LIST_SORT)
        .skip(offset)
        .limit(limit)
        .toArray(),
      col.countDocuments(PUBLISHED),
    ]);
    res.set('Cache-Control', 'public, max-age=60');
    res.json({ success: true, items: docs.map(summary), total });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Server Error' });
  }
});

// 글 보기: 본문은 한 번 더 정리해서 내보낸다(예전에 저장된 글 대비)
router.get('/:id', async (req, res) => {
  try {
    const _id = toId(req.params.id);
    if (!_id) return res.status(400).json({ success: false, message: '잘못된 ID' });
    const doc = await getDB().collection('yogiboJPnews').findOne({ _id, ...PUBLISHED });
    if (!doc) return res.status(404).json({ success: false, message: '글을 찾을 수 없습니다.' });
    res.set('Cache-Control', 'public, max-age=60');
    res.json({ success: true, item: { ...summary(doc), content: sanitizeContent(doc.content) } });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Server Error' });
  }
});

router.post('/:id/view', async (req, res) => {
  try {
    const _id = toId(req.params.id);
    if (!_id) return res.status(400).json({ success: false });
    await getDB().collection('yogiboJPnews').updateOne({ _id, ...PUBLISHED }, { $inc: { views: 1 } });
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ success: false });
  }
});

module.exports = router;
