const sharp = require('sharp');
const crypto = require('crypto');
const { uploadBuffer } = require('./ftp');

function absoluteUrl(src, base = 'https://yogibo.jp') {
  if (!src) return null;
  if (src.startsWith('//')) return `https:${src}`;
  try {
    return new URL(src, base).href;
  } catch {
    return null;
  }
}

async function download(url) {
  const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 yogibonews' } });
  if (!res.ok) throw new Error(`이미지 다운로드 실패 HTTP ${res.status}`);
  return Buffer.from(await res.arrayBuffer());
}

function randomName(prefix) {
  return `${prefix}-${Date.now()}-${crypto.randomBytes(6).toString('hex')}.webp`;
}

// 원본(일본 CDN) 이미지를 받아 웹용 webp로 줄인 뒤 Cafe24 FTP에 올리고 공개 URL을 돌려준다
async function rehostImage(url) {
  const buffer = await download(url);
  const out = await sharp(buffer).resize(800, null, { fit: 'inside', withoutEnlargement: true }).webp({ quality: 82 }).toBuffer();
  return uploadBuffer(out, randomName('newsletter'));
}

async function makeThumbnail(url) {
  const buffer = await download(url);
  const out = await sharp(buffer).resize(800, 500, { fit: 'cover', position: 'center' }).webp({ quality: 82 }).toBuffer();
  return uploadBuffer(out, randomName('news-thumb'));
}

module.exports = { absoluteUrl, rehostImage, makeThumbnail };
