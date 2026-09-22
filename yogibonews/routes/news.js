const express = require('express');
const multer = require('multer');
const os = require('os');
const fs = require('fs');
const crypto = require('crypto');
const sharp = require('sharp');
const { ObjectId } = require('mongodb');

const { getDB } = require('../db');
const { uploadBuffer } = require('../lib/ftp');
const { askClaude, hasKey } = require('../lib/claude');
const { getKrCatalog, formatProductList } = require('../lib/krProducts');
const { applyUtm, renderProductCards } = require('../lib/productLinks');
const { sanitizeContent } = require('../lib/sanitize');
const { wrapWithFont, designRules } = require('../lib/v5');
const { convertDoc } = require('../pipeline/runPipeline');
const { translateJaToKo } = require('../pipeline/translate');

const router = express.Router();

const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, os.tmpdir()),
    filename: (req, file, cb) => cb(null, `${file.fieldname}-${Date.now()}-${Math.round(Math.random() * 1e9)}`),
  }),
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const allowed = ['image/jpeg', 'image/png', 'image/webp', 'image/gif'];
    cb(allowed.includes(file.mimetype) ? null : new Error('허용되지 않는 파일 형식입니다.'), allowed.includes(file.mimetype));
  },
});

// 이관 데이터에 문자열 _id('hardcoded_recovery') 게시글이 있어 ObjectId가 아닌 id도 그대로 조회한다
function toId(id) {
  if (typeof id !== 'string' || !id) return null;
  if (ObjectId.isValid(id) && new ObjectId(id).toHexString() === id) return new ObjectId(id);
  return id;
}

// 매거진에 이미 발행된 글의 순서를 지키기 위한 정렬 (position 오름차순 → 최신 글)
const LIST_SORT = { position: 1, pubDate: -1 };
const NOT_ARCHIVED = { archived: { $ne: true } };

// 목록 조회 — status: draft|pending|published|excluded|all, source: rss|manual, archived=1이면 보관된 예전 원본만
router.get('/', async (req, res) => {
  try {
    const { status, source, limit, offset, archived } = req.query;
    const collection = getDB().collection('yogiboJPnews');
    const query = archived === '1' ? { archived: true } : { ...NOT_ARCHIVED };
    if (status && status !== 'all') query.status = status;
    if (source === 'rss') query.source = { $ne: 'manual' };
    if (source === 'manual') query.source = 'manual';

    const totalCount = await collection.countDocuments(query);
    let cursor = collection.find(query).sort(LIST_SORT);
    if (offset) cursor = cursor.skip(Number(offset));
    if (limit) cursor = cursor.limit(Number(limit));
    const data = await cursor.toArray();

    const counts = {
      all: await collection.countDocuments(NOT_ARCHIVED),
      draft: await collection.countDocuments({ ...NOT_ARCHIVED, status: 'draft' }),
      pending: await collection.countDocuments({ status: 'pending' }),
      published: await collection.countDocuments({ status: 'published' }),
      excluded: await collection.countDocuments({ status: 'excluded' }),
      archived: await collection.countDocuments({ archived: true }),
      rss: await collection.countDocuments({ source: { $ne: 'manual' } }),
      manual: await collection.countDocuments({ source: 'manual' }),
    };

    res.json({ success: true, data, totalCount, counts });
  } catch (error) {
    console.error('뉴스레터 조회 에러:', error);
    res.status(500).json({ success: false, message: 'Server Error' });
  }
});

// 단건 조회
router.get('/:id', async (req, res) => {
  try {
    const queryId = toId(req.params.id);
    if (!queryId) return res.status(400).json({ success: false, message: '잘못된 ID 형식입니다.' });
    const doc = await getDB().collection('yogiboJPnews').findOne({ _id: queryId });
    if (!doc) return res.status(404).json({ success: false, message: '게시글 없음' });
    res.json({ success: true, data: doc });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Server Error' });
  }
});

// 순서 변경 (드래그앤드롭)
router.put('/order', async (req, res) => {
  try {
    const { order } = req.body;
    if (!order || !Array.isArray(order)) {
      return res.status(400).json({ success: false, message: '잘못된 데이터 형식입니다.' });
    }
    const collection = getDB().collection('yogiboJPnews');
    const bulkOps = order
      .map((item) => {
        const queryId = toId(item.id);
        if (!queryId) return null;
        return { updateOne: { filter: { _id: queryId }, update: { $set: { position: item.position } } } };
      })
      .filter(Boolean);
    if (bulkOps.length > 0) await collection.bulkWrite(bulkOps);
    res.json({ success: true, message: '순서가 성공적으로 업데이트되었습니다.' });
  } catch (error) {
    console.error('순서 업데이트 에러:', error);
    res.status(500).json({ success: false, message: '서버 오류가 발생했습니다.' });
  }
});

// 내용 수정 및 상태 변경 (draft/pending/published/excluded)
router.put('/:id', async (req, res) => {
  try {
    const queryId = toId(req.params.id);
    if (!queryId) return res.status(400).json({ success: false, message: '잘못된 게시글 ID 형식입니다.' });

    const { title, content, status, excludeReason } = req.body;
    const updateData = { updatedAt: new Date() };
    // 검수대기에서 검수자가 '발행 안 함'을 고른 경우 — 자동 제외와 구분해 사유를 남긴다
    if (status === 'excluded' && excludeReason !== undefined) {
      Object.assign(updateData, {
        'pipeline.exclude': true,
        'pipeline.excludeCategory': 'reviewer',
        'pipeline.excludeReason': excludeReason || '검수자가 발행하지 않기로 했습니다.',
        'pipeline.reviewedAt': new Date(),
      });
    }
    if (title !== undefined) updateData.title = title;
    // 저장할 때마다 위험한 코드를 걷어내고, 직접 넣은 국내 상품 링크에도 기사별 UTM을 붙인다(이미 붙은 링크는 그대로)
    if (content !== undefined) updateData.content = applyUtm(sanitizeContent(content), `news_${req.params.id}`);
    if (status !== undefined) {
      updateData.status = status;
      updateData.archived = false;
    }

    // 매거진은 position 오름차순 정렬 — 기존 어드민과 같이 발행 시점의 -timestamp로 최신 발행글을 맨 위에 둔다
    if (status === 'published') {
      const current = await getDB().collection('yogiboJPnews').findOne({ _id: queryId }, { projection: { status: 1, position: 1 } });
      if (current && current.status !== 'published' && typeof current.position !== 'number') {
        updateData.position = -Date.now();
        updateData.publishedAt = new Date();
      }
    }

    await getDB().collection('yogiboJPnews').updateOne({ _id: queryId }, { $set: updateData });
    res.json({ success: true, message: '게시글이 성공적으로 업데이트되었습니다.' });
  } catch (error) {
    console.error('뉴스레터 업데이트 에러:', error);
    res.status(500).json({ success: false, message: 'Server Error' });
  }
});

// 신규 게시글 생성 (직접 작성 / AI 작성 공용)
router.post('/', async (req, res) => {
  try {
    const { title, content, status, thumbnail } = req.body;
    if (!title || !content) {
      return res.status(400).json({ success: false, message: '제목과 내용은 필수입니다.' });
    }
    const newPost = {
      title,
      content: sanitizeContent(content),
      status: status || 'draft',
      thumbnail: thumbnail || null,
      pubDate: new Date(),
      createdAt: new Date(),
      updatedAt: new Date(),
      views: 0,
      source: 'manual',
    };
    const collection = getDB().collection('yogiboJPnews');
    const result = await collection.insertOne(newPost);
    const withUtm = applyUtm(newPost.content, `news_${result.insertedId}`);
    if (withUtm !== newPost.content) {
      await collection.updateOne({ _id: result.insertedId }, { $set: { content: withUtm } });
      newPost.content = withUtm;
    }
    res.json({ success: true, message: '새 게시글이 생성되었습니다.', data: { _id: result.insertedId, ...newPost } });
  } catch (error) {
    console.error('게시글 생성 에러:', error);
    res.status(500).json({ success: false, message: '서버 오류가 발생했습니다.' });
  }
});

// 삭제
router.delete('/:id', async (req, res) => {
  try {
    const queryId = toId(req.params.id);
    if (!queryId) return res.status(400).json({ success: false, message: '잘못된 ID 형식입니다.' });
    const result = await getDB().collection('yogiboJPnews').deleteOne({ _id: queryId });
    if (result.deletedCount === 0) return res.status(404).json({ success: false, message: '게시글을 찾을 수 없습니다.' });
    res.json({ success: true, message: '게시글이 삭제되었습니다.' });
  } catch (error) {
    console.error('게시글 삭제 에러:', error);
    res.status(500).json({ success: false, message: '서버 오류가 발생했습니다.' });
  }
});

// v5 매거진 기사로 AI 재편집 → 검수대기 (Claude 호출 — 과금 발생)
router.post('/:id/convert', async (req, res) => {
  try {
    const queryId = toId(req.params.id);
    if (!queryId) return res.status(400).json({ success: false, message: '잘못된 ID 형식입니다.' });
    if (!hasKey()) {
      return res.status(400).json({ success: false, message: 'Claude 호출이 꺼져 있습니다 (로컬 CLAUDE_ENABLED=false). 라이브 서버에서 실행하세요.' });
    }
    const doc = await getDB().collection('yogiboJPnews').findOne({ _id: queryId });
    if (!doc) return res.status(404).json({ success: false, message: '게시글 없음' });

    await convertDoc(doc);
    const updated = await getDB().collection('yogiboJPnews').findOne({ _id: queryId });
    res.json({ success: true, data: updated });
  } catch (error) {
    console.error('❌ AI 재편집 에러:', error.message);
    res.status(500).json({ success: false, message: error.message || 'AI 재편집 중 오류' });
  }
});

// 조회수 증가
router.post('/:id/view', async (req, res) => {
  try {
    const queryId = toId(req.params.id);
    if (!queryId) return res.status(400).json({ success: false, message: '잘못된 ID 형식입니다.' });
    await getDB().collection('yogiboJPnews').updateOne({ _id: queryId }, { $inc: { views: 1 } });
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Server Error' });
  }
});

// 썸네일 업로드
router.post('/:id/thumbnail-upload', upload.single('file'), async (req, res) => {
  const queryId = toId(req.params.id);
  if (!queryId) return res.status(400).json({ success: false, message: '잘못된 게시글 ID 형식입니다.' });
  try {
    if (!req.file) return res.status(400).json({ success: false, message: '파일 없음' });
    const processedBuffer = await sharp(req.file.path).resize(800, 500, { fit: 'cover', position: 'center' }).webp({ quality: 82 }).toBuffer();
    fs.unlinkSync(req.file.path);

    const filename = `news-${req.params.id}-${Date.now()}-${crypto.randomBytes(6).toString('hex')}.webp`;
    const publicUrl = await uploadBuffer(processedBuffer, filename);

    const result = await getDB().collection('yogiboJPnews').updateOne(
      { _id: queryId },
      { $set: { thumbnail: publicUrl, thumbnailUpdatedAt: new Date() } }
    );
    if (result.matchedCount === 0) return res.status(404).json({ success: false, message: '게시글 없음' });
    res.json({ success: true, url: publicUrl, filename });
  } catch (err) {
    console.error('[Thumbnail Upload Error]', err);
    res.status(500).json({ success: false, message: err.message || '서버 오류' });
  }
});

// 본문 이미지 업로드
router.post('/upload-image', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ success: false, message: '파일 없음' });
    const processedBuffer = await sharp(req.file.path).resize(800, null, { fit: 'inside', withoutEnlargement: true }).webp({ quality: 82 }).toBuffer();
    fs.unlinkSync(req.file.path);

    const filename = `newsletter-${Date.now()}-${crypto.randomBytes(6).toString('hex')}.webp`;
    const publicUrl = await uploadBuffer(processedBuffer, filename);
    res.json({ success: true, url: publicUrl });
  } catch (err) {
    console.error('[Image Upload Error]', err);
    res.status(500).json({ success: false, message: err.message || '이미지 업로드 실패' });
  }
});

const STYLE_GUIDES = {
  'product-launch': '신제품 출시 소식. 기대감과 혜택을 강조하고 구매로 이어지는 CTA를 넣습니다.',
  'event-promo': '이벤트/프로모션 안내. 혜택과 기간, 참여 방법을 명확히 안내합니다.',
  'brand-story': '브랜드 스토리텔링. 감성적으로 공감을 이끌고 요기보의 라이프스타일 가치를 전합니다.',
  collab: '콜라보/한정판 소식. 희소성과 특별함을 강조합니다.',
  'tips-guide': '생활 팁/가이드. 실용적이고 친근하게, 요기보 제품 활용법을 자연스럽게 연결합니다.',
};

const GENERATE_SCHEMA = {
  type: 'object',
  properties: { title: { type: 'string' }, content: { type: 'string' } },
  required: ['title', 'content'],
  additionalProperties: false,
};

// AI 작성: 프롬프트 + 참고 이미지 + 브랜드자료로 v5 매거진 기사 초안 생성 (Claude 호출 — 과금 발생)
router.post('/generate', async (req, res) => {
  try {
    const { prompt, images, imageUrls, style } = req.body;
    if (!prompt) return res.status(400).json({ success: false, message: '프롬프트를 입력해주세요.' });
    if (!hasKey()) {
      return res.status(400).json({ success: false, message: 'Claude 호출이 꺼져 있습니다 (로컬 CLAUDE_ENABLED=false). 라이브 서버에서 실행하세요.' });
    }

    const [kr, knowledge] = await Promise.all([
      getKrCatalog(),
      getDB().collection('brandKnowledge').find({}).sort({ updatedAt: -1 }).toArray(),
    ]);
    const knowledgeText = knowledge.length
      ? knowledge.map((k) => `### [${k.category}] ${k.title}\n${k.content}`).join('\n\n')
      : '(등록된 브랜드자료 없음)';
    const urls = Array.isArray(imageUrls) ? imageUrls : [];

    const system = `당신은 요기보 코리아(yogibo.kr) 매거진 에디터입니다. 요청받은 주제로 매거진 기사 초안을 씁니다.
결과물은 사람이 검토한 뒤 최소한의 수정만 거쳐 발행되므로, 그대로 게시할 수 있는 완성본으로 씁니다.
사실(제품 사양, 가격, 기간 등)은 요청 내용과 브랜드자료에 있는 것만 쓰고 만들지 않습니다.

## 이미지
첨부 이미지는 순서대로 1번부터 번호가 붙습니다. 이미지를 넣을 자리에는
<img src="{{IMAGE_n}}" alt="한국어 설명" style="width: 100%; display: block; border-radius: 6px; margin: 20px 0;"> 형식만 씁니다.

${designRules(new Date().getFullYear())}

## 제품 링크와 추천 제품
- 제품을 처음 언급하는 곳에 https://yogibo.kr/product/detail.html?product_no=번호 링크를 겁니다. 아래 목록 밖 URL은 쓰지 않습니다.
- 추천 제품 박스 안에는 {{PRODUCT_CARDS:번호,번호}} 한 줄만 씁니다(2~5개). 시스템이 상품 이미지 카드로 바꿉니다.

## 국내 판매 제품 (대표 모델, 이름 (영문명) | 번호)
${formatProductList(kr.base)}

## 브랜드자료 (참고용)
${knowledgeText}

## 출력
- title: 목록 카드에 쓰일 한국어 제목(HTML 없이 한 줄)
- content: v5 형식 HTML 전체`;

    const content = [];
    if (Array.isArray(images)) {
      for (const img of images) {
        if (img.data) {
          content.push({ type: 'image', source: { type: 'base64', media_type: img.mediaType || 'image/jpeg', data: img.data } });
        }
      }
    }
    content.push({ type: 'text', text: `톤: ${STYLE_GUIDES[style] || '요기보 브랜드 톤의 따뜻하고 친근한 기사.'}\n\n요청:\n${prompt}` });

    const result = await askClaude({ system, content, schema: GENERATE_SCHEMA, effort: 'medium', maxTokens: 32000, stream: true });

    let html = result.content.replace(/\{\{IMAGE_(\d+)\}\}/g, (m, n) => urls[Number(n) - 1] || '');
    html = html.replace(/<img\b[^>]*src=["']\s*["'][^>]*>/gi, '');
    html = (await renderProductCards(html)).html;
    // UTM은 게시글이 저장될 때 글 번호로 붙는다 (POST /api/yogibo-jp-news)
    res.json({ success: true, title: result.title.trim(), content: wrapWithFont(html) });
  } catch (error) {
    console.error('❌ AI 생성 에러:', error.message);
    res.status(500).json({ success: false, message: error.message || 'AI 생성 중 오류' });
  }
});

module.exports = router;

// 수동 JP→KR 번역 (직접 작성 글이나 재번역이 필요한 글용) — index.js에서 /api/translate-news로 마운트
module.exports.translateHandler = async (req, res) => {
  try {
    const { title, content } = req.body;
    const { translatedTitle, translatedContent } = await translateJaToKo({ title, content });
    res.json({ success: true, translatedTitle, translatedContent });
  } catch (error) {
    console.error('❌ 번역 에러:', error);
    res.status(500).json({ success: false, message: '번역 중 오류가 발생했습니다. 본문이 너무 길 수 있습니다.' });
  }
};
