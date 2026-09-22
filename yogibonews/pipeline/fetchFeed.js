const Parser = require('rss-parser');

const parser = new Parser();

function cleanLink(rawLink) {
  let link = rawLink || '';
  if (link.includes('?')) link = link.split('?')[0];
  if (link.endsWith('/')) link = link.slice(0, -1);
  return link;
}

/**
 * 요기보 JP 블로그 Atom 피드를 읽어 정규화된 아이템 배열을 반환한다.
 * (adminChat/server.js의 fetchAndSaveYogiboJPNews 피드 파싱 로직 이식)
 */
async function fetchJapanBlogFeed() {
  const feedUrl = process.env.JP_BLOG_FEED_URL || 'https://yogibo.jp/blogs/life.atom';
  const feed = await parser.parseURL(feedUrl);

  return (feed.items || []).map((item) => ({
    guid: cleanLink(item.link),
    link: cleanLink(item.link),
    title: item.title || '',
    content: item.content || '',
    pubDate: item.pubDate ? new Date(item.pubDate) : new Date(),
  }));
}

module.exports = { fetchJapanBlogFeed };
