# yogibonews — 요기보 매거진 모듈

일본 요기보 블로그(RSS) → 1차 분류(지역 행사·매장 공지·국내 미판매·일본 한정 콜라보·기간 한정 제외) → 한국 독자용 재편집(Claude) → **검수대기** → 사람이 발행 → yogibo.kr 매거진.

- adminChat `server.js`의 `initialize()`에서 `yogibonews.mount(app, { getDb })`로 붙는다. 모든 경로는 `/yogibonews` 아래.
  - 어드민(Vercel, `Desktop/yogibonews/web`): `/yogibonews/api/yogibo-jp-news`, `/api/pipeline`, `/api/brand-knowledge`, `/api/translate-news`
  - Cafe24 공개 API(`Desktop/yogibonews/cafe24/*.html`): `/yogibonews/api/magazine`
- 글은 별도 DB(`YOGIBONEWS_MONGODB_URI`)에 둔다. 연결 실패 시 이 경로만 503이고 adminChat 다른 기능은 영향 없음.
- Cafe24 상품 목록은 adminChat DB의 `tokens`를 **읽기만** 한다 (토큰 갱신은 yogiChat 전담).

## 환경변수 (CloudType adminChat 서비스)

| 변수 | 필수 | 기본값 / 설명 |
|---|---|---|
| `YOGIBONEWS_MONGODB_URI` | ✅ | 매거진 DB (yogico 클러스터) |
| `YOGIBONEWS_DB_NAME` | | `yogibonews` |
| `YOGIBONEWS_ANTHROPIC_API_KEY` | ✅ | 없으면 `ANTHROPIC_API_KEY` 사용 |
| `YOGIBONEWS_CLAUDE_ENABLED` | | `false`면 Claude 호출 안 함(분류·재편집 멈춤). 운영은 비워두거나 `true` |
| `YOGIBONEWS_CLAUDE_MODEL` | | `claude-opus-5` |
| `YOGIBONEWS_FETCH_CRON` | | `0 */6 * * *` (6시간마다, 00·06·12·18시). `off`면 자동 수집 안 함 |
| `YOGIBONEWS_CRON_TZ` | | `Asia/Seoul` — 서버 시계와 무관하게 이 시간대로 돈다 |
| `YOGIBONEWS_REPROCESS_LIMIT` | | `30` — 한 번에 처리할 판별 대기 글 수(과금 상한) |
| `YOGIBONEWS_FEED_URL` | | `https://yogibo.jp/blogs/life.atom` |
| `YOGIBONEWS_FTP_USER` / `_PASS` / `_HOST` / `_PORT` | | 없으면 adminChat `FTP_*` 사용 (같은 Cafe24 FTP 계정) |
| `YOGIBONEWS_FTP_REMOTE_DIR` | | `/web/news` |
| `YOGIBONEWS_FTP_PUBLIC_BASE` | | `https://yogibo.openhost.cafe24.com/web/news` |

## 명령 (adminChat/server에서)

- `npm run dev:yogibonews` — 이 모듈만 단독 실행 (http://localhost:6103/yogibonews). 로컬 `.env`는 `YOGIBONEWS_CLAUDE_ENABLED=false`, `YOGIBONEWS_FETCH_CRON=off`
- `npm run yogibonews:migrate` — 기존 adminChat `yogiboJPnews`·`brandKnowledge` → 매거진 DB 재동기화 (원본은 읽기만, 여러 번 실행해도 안전)
