const fs = require('fs');
const path = require('path');

const V5_EXAMPLE = fs.readFileSync(path.join(__dirname, '..', 'pipeline', 'templates', 'v5-example.html'), 'utf8');

// 브랜드 규칙: 모든 글꼴은 프리텐다드. 본문 전체를 감싸서 사이트 기본 폰트와 무관하게 적용한다
const FONT_WRAPPER_STYLE = "font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, sans-serif;";

function wrapWithFont(html) {
  return `<div style="${FONT_WRAPPER_STYLE}">\n${html}\n</div>`;
}

function designRules(year) {
  return `## 디자인 — Yogibo Korea 뉴스레터 v5
아래 예시의 구조, 인라인 스타일, 컬러(#00BDD4 / #1E3A47 / #2C9DB6 / #58B5CA / #E5F8FA)를 그대로 따릅니다.
- 맨 앞 v5 주석, 분류 라벨(소식/이벤트/공지사항 중 하나), 제목(h1, 길면 <br>로 두 줄), 본문 래퍼(padding: 0 32px 28px)
- 소제목은 세로 그라데이션 바 + 한국어 소제목 + 영문 대문자 부제
- 통계 카드는 근거 수치가 있을 때만, 인용 박스는 실제 인용이 있을 때만 씁니다.
- 끝부분에 추천 제품 박스(국내 제품 칩 링크)를 넣고, CTA 버튼은 가장 관련 있는 국내 제품 페이지로 연결합니다.
- 푸터는 예시와 같은 형식으로 쓰고 연도는 © ${year} 로 씁니다.
- 인라인 스타일만 씁니다. <style>, <script>, class는 쓰지 않습니다. font-family는 시스템이 전체에 프리텐다드를 적용하므로 따로 쓰지 않습니다.

<example>
${V5_EXAMPLE}
</example>`;
}

module.exports = { V5_EXAMPLE, wrapWithFont, designRules };
