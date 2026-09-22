const { getDB } = require('../db');
const { hasKey } = require('../lib/claude');
const { fetchJapanBlogFeed } = require('./fetchFeed');
const { classifyItem } = require('./classify');
const { convertToKoreanMagazine } = require('./convert');
const { sanitizeContent } = require('../lib/sanitize');

// 판별 대기 글을 한 번에 몇 건까지 처리할지 (과금 상한). 남은 건 다음 실행에서 이어서 처리한다.
const REPROCESS_LIMIT = Number(process.env.REPROCESS_LIMIT) || 30;

function news() {
  return getDB().collection('yogiboJPnews');
}

function sourceOf(doc) {
  return {
    title: doc.originalTitle || doc.title,
    content: doc.originalContent || doc.content,
    link: doc.link,
  };
}

/**
 * 게시글 1건을 v5 매거진 기사로 재편집해 '검수대기(pending)'로 올린다. (Claude 호출 — 과금 발생)
 * 원문(originalTitle/originalContent)이 없던 글은 덮어쓰기 전에 현재 제목·본문을 원문으로 보존한다.
 */
async function convertDoc(doc) {
  const source = sourceOf(doc);
  const converted = await convertToKoreanMagazine({ ...source, campaign: `news_${doc._id}` });

  const set = {
    title: converted.title,
    content: sanitizeContent(converted.content),
    status: 'pending',
    archived: false,
    updatedAt: new Date(),
    'pipeline.convertedAt': new Date(),
    'pipeline.reviewNotes': converted.reviewNotes,
    'pipeline.error': null,
  };
  if (!doc.originalContent) {
    set.originalTitle = source.title;
    set.originalContent = source.content;
  }
  if (converted.thumbnail && !doc.thumbnail) set.thumbnail = converted.thumbnail;

  await news().updateOne({ _id: doc._id }, { $set: set });
  return converted;
}

/**
 * RSS 원문 1건: 1차 분류(지역·장소·국내 미판매·일본 한정 콜라보 제외) → 통과 시 v5 재편집 → 검수대기
 * convert=false(기존 원본 백로그)면 분류까지만 하고 원본 탭에 '라이브 후보'로 남긴다.
 * 반환: 'excluded' | 'pending' | 'candidate' | 'needsReview'
 */
async function processRssDoc(doc, log, { convert = true } = {}) {
  const source = sourceOf(doc);
  const classification = await classifyItem(source);
  const pipeline = { ...(doc.pipeline || {}), classifiedAt: new Date(), ...classification, error: null };

  if (classification.skipped) {
    await news().updateOne({ _id: doc._id }, { $set: { pipeline, updatedAt: new Date() } });
    return 'needsReview';
  }

  if (classification.exclude) {
    await news().updateOne({ _id: doc._id }, { $set: { pipeline, status: 'excluded', updatedAt: new Date() } });
    log(`🚫 제외(${classification.excludeCategory}): ${source.title} — ${classification.excludeReason}`);
    return 'excluded';
  }

  await news().updateOne({ _id: doc._id }, { $set: { pipeline, updatedAt: new Date() } });

  if (!classification.translatable) {
    log(`✋ 게시할 본문 부족, 수동 확인: ${source.title}`);
    return 'needsReview';
  }
  if (!convert) return 'candidate';

  try {
    const converted = await convertDoc({ ...doc, pipeline });
    log(`✅ 재편집 완료, 검수대기: ${converted.title}`);
    return 'pending';
  } catch (err) {
    await news().updateOne({ _id: doc._id }, { $set: { 'pipeline.error': err.message, updatedAt: new Date() } });
    log(`❌ 재편집 실패, 수동 확인으로 남김 (${source.title}): ${err.message}`);
    return 'needsReview';
  }
}

/**
 * 일본 블로그 RSS 수집 → 신규 글 저장(원본) → 분류 → 재편집 → 검수대기.
 * 이어서 판별 대기 글(로컬 수집분·기존 원본 백로그)을 처리한다. 발행은 항상 사람이 검수 후 직접 한다.
 */
async function runNewsPipeline({ log = console.log, trigger = 'manual' } = {}) {
  const startedAt = new Date();
  const col = news();
  const result = {
    fetched: 0, inserted: 0, excluded: 0, pending: 0, candidate: 0, needsReview: 0,
    reprocessed: 0, backlogRemaining: 0, errors: [],
  };
  const tally = (outcome) => {
    result[outcome]++;
  };

  try {
    const items = await fetchJapanBlogFeed();
    result.fetched = items.length;

    for (const item of items) {
      try {
        if (await col.findOne({ guid: item.guid }, { projection: { _id: 1 } })) continue;
        if (await col.findOne({ title: item.title }, { projection: { _id: 1 } })) continue;

        const doc = {
          guid: item.guid,
          link: item.link,
          source: 'rss',
          originalTitle: item.title,
          originalContent: item.content,
          title: item.title,
          content: item.content,
          status: 'draft',
          pubDate: item.pubDate,
          thumbnail: null,
          views: 0,
          createdAt: new Date(),
          updatedAt: new Date(),
          pipeline: {},
        };
        const { insertedId } = await col.insertOne(doc);
        result.inserted++;
        tally(await processRssDoc({ ...doc, _id: insertedId }, log));
      } catch (err) {
        result.errors.push({ title: item.title, message: err.message });
        log(`❌ 파이프라인 처리 실패 (${item.title}): ${err.message}`);
      }
    }

    if (hasKey()) {
      const waitingQuery = { source: { $ne: 'manual' }, status: 'draft', 'pipeline.skipped': true };
      const waiting = await col.find(waitingQuery).sort({ pubDate: -1 }).limit(REPROCESS_LIMIT).toArray();
      for (const doc of waiting) {
        try {
          tally(await processRssDoc(doc, log, { convert: !doc.pipeline?.backlog }));
          result.reprocessed++;
        } catch (err) {
          result.errors.push({ title: sourceOf(doc).title, message: err.message });
        }
      }
      result.backlogRemaining = await col.countDocuments(waitingQuery);
      if (result.backlogRemaining) log(`ℹ️ 판별 대기 ${result.backlogRemaining}건은 다음 실행에서 이어서 처리합니다.`);
    }
  } finally {
    await getDB()
      .collection('pipelineRuns')
      .insertOne({ trigger, startedAt, finishedAt: new Date(), claudeEnabled: hasKey(), result })
      .catch((err) => console.warn('⚠️ 수집 이력 저장 실패:', err.message));
  }

  return result;
}

module.exports = { runNewsPipeline, convertDoc, processRssDoc };
