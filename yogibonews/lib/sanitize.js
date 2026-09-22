const sanitizeHtml = require('sanitize-html');

// 기사 HTML은 yogibo.kr 페이지에 그대로 들어가므로 스크립트·이벤트 속성·위험한 링크를 걷어낸다.
// v5 기사의 인라인 스타일과, 예전 발행글이 쓰던 <style> 블록·class는 유지한다.
const OPTIONS = {
  allowedTags: [
    'div', 'p', 'span', 'a', 'img', 'br', 'hr', 'strong', 'b', 'em', 'i', 'u', 's', 'small', 'sub', 'sup', 'mark',
    'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'li', 'dl', 'dt', 'dd', 'blockquote', 'figure', 'figcaption',
    'table', 'thead', 'tbody', 'tfoot', 'tr', 'th', 'td', 'caption', 'colgroup', 'col',
    'section', 'article', 'header', 'footer', 'nav', 'style', 'details', 'summary',
  ],
  allowedAttributes: {
    '*': ['style', 'class', 'id', 'title', 'align', 'width', 'height', 'colspan', 'rowspan'],
    a: ['href', 'target', 'rel', 'name'],
    img: ['src', 'alt', 'loading', 'decoding'],
  },
  allowedSchemes: ['http', 'https', 'mailto', 'tel'],
  allowedSchemesAppliedToAttributes: ['href', 'src'],
  allowProtocolRelative: true,
  allowVulnerableTags: true, // <style> 블록 허용 (예전 발행글 호환)
  transformTags: {
    a: (tagName, attribs) => (attribs.target === '_blank' ? { tagName, attribs: { ...attribs, rel: 'noopener' } } : { tagName, attribs }),
  },
};

function sanitizeContent(html) {
  return sanitizeHtml(String(html || ''), OPTIONS);
}

module.exports = { sanitizeContent };
