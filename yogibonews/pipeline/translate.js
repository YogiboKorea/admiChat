/**
 * 일→한 번역. adminChat/server.js의 /api/translate-news 로직을 그대로 이식했다.
 * <style>/<script>/<img> 태그를 마스킹 처리해서 번역기가 HTML 구조를 깨뜨리지 않도록 보호한다.
 */
async function translateJaToKo({ title, content }) {
  const { translate: googleTranslate } = await import('@vitalets/google-translate-api');

  let translatedTitle = title;
  if (title) {
    const titleResult = await googleTranslate(title, { from: 'ja', to: 'ko' });
    translatedTitle = titleResult.text;
  }

  let translatedContent = content || '';
  if (content) {
    const protectedTags = [];
    let masked = content;

    masked = masked.replace(/<style[^>]*>[\s\S]*?<\/style>/gi, (m) => {
      protectedTags.push(m);
      return `__TG${protectedTags.length - 1}__`;
    });
    masked = masked.replace(/<script[^>]*>[\s\S]*?<\/script>/gi, (m) => {
      protectedTags.push(m);
      return `__TG${protectedTags.length - 1}__`;
    });
    masked = masked.replace(/<img[^>]*>/gi, (m) => {
      protectedTags.push(m);
      return `__TG${protectedTags.length - 1}__`;
    });

    const contentResult = await googleTranslate(masked, { from: 'ja', to: 'ko' });
    if (!contentResult || !contentResult.text) {
      throw new Error('번역 API에서 빈 값을 반환했습니다.');
    }

    translatedContent = contentResult.text.replace(/__\s*TG\s*(\d+)\s*__/gi, (match, idx) => {
      return protectedTags[parseInt(idx, 10)] || match;
    });
  }

  return { translatedTitle, translatedContent };
}

module.exports = { translateJaToKo };
