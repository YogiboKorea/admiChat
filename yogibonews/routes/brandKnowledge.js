const express = require('express');
const multer = require('multer');
const os = require('os');
const fs = require('fs');
const { ObjectId } = require('mongodb');

const { getDB } = require('../db');
const { askClaude, hasKey } = require('../lib/claude');

const router = express.Router();

const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, os.tmpdir()),
    filename: (req, file, cb) => cb(null, `${file.fieldname}-${Date.now()}-${Math.round(Math.random() * 1e9)}`),
  }),
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const allowed = ['image/jpeg', 'image/png', 'image/webp', 'image/gif', 'application/pdf'];
    cb(allowed.includes(file.mimetype) ? null : new Error('허용되지 않는 파일 형식입니다. (jpg/png/webp/gif/pdf만 가능)'), allowed.includes(file.mimetype));
  },
});

// 카테고리: product-spec(제품), brand-story(브랜드), promotion(프로모션)

router.get('/', async (req, res) => {
  try {
    const { category } = req.query;
    const query = category ? { category } : {};
    const docs = await getDB().collection('brandKnowledge').find(query).sort({ updatedAt: -1 }).toArray();
    res.json({ success: true, data: docs });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

router.post('/', async (req, res) => {
  try {
    const { title, category, content } = req.body;
    if (!title || !category || !content) {
      return res.status(400).json({ success: false, message: '제목, 카테고리, 내용은 필수입니다.' });
    }
    const doc = { title, category, content, createdAt: new Date(), updatedAt: new Date() };
    const result = await getDB().collection('brandKnowledge').insertOne(doc);
    res.json({ success: true, data: { _id: result.insertedId, ...doc } });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

router.put('/:id', async (req, res) => {
  try {
    if (!ObjectId.isValid(req.params.id)) return res.status(400).json({ success: false, message: '잘못된 ID' });
    const { title, category, content } = req.body;
    await getDB().collection('brandKnowledge').updateOne(
      { _id: new ObjectId(req.params.id) },
      { $set: { title, category, content, updatedAt: new Date() } }
    );
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

router.delete('/:id', async (req, res) => {
  try {
    if (!ObjectId.isValid(req.params.id)) return res.status(400).json({ success: false, message: '잘못된 ID' });
    await getDB().collection('brandKnowledge').deleteOne({ _id: new ObjectId(req.params.id) });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

const EXTRACT_SYSTEM = `당신은 문서 분석 전문가입니다. 첨부된 문서/이미지의 내용을 빠짐없이 한국어 텍스트로 정리합니다.
제품명, 가격, 특징, 소재, 사이즈 등 핵심 정보를 모두 포함하고, 마크다운 없이 일반 텍스트로 정리합니다.`;

// 이미지/PDF에서 텍스트 추출 → 브랜드 자료 본문으로 사용
router.post('/extract', upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ success: false, message: '파일 없음' });

  const { path: filePath, mimetype } = req.file;
  try {
    if (!hasKey()) {
      return res.status(500).json({ success: false, message: 'ANTHROPIC_API_KEY 환경변수가 설정되지 않았습니다.' });
    }

    const data = fs.readFileSync(filePath).toString('base64');
    const fileBlock =
      mimetype === 'application/pdf'
        ? { type: 'document', source: { type: 'base64', media_type: 'application/pdf', data } }
        : { type: 'image', source: { type: 'base64', media_type: mimetype, data } };

    const text = await askClaude({
      system: EXTRACT_SYSTEM,
      content: [fileBlock, { type: 'text', text: '이 문서의 내용을 텍스트로 정리해주세요.' }],
      effort: 'low',
      maxTokens: 32000,
      stream: true,
    });

    res.json({ success: true, text });
  } catch (err) {
    console.error('[브랜드자료 추출 오류]', err);
    res.status(500).json({ success: false, message: err.message || '텍스트 추출 실패' });
  } finally {
    fs.unlink(filePath, () => {});
  }
});

module.exports = router;
