/**
 * yogibonews 설정.
 * adminChat 서버 안에 붙어 돌기 때문에 adminChat의 MONGODB_URI·DB_NAME 등과 겹치지 않도록 YOGIBONEWS_ 접두사 변수를 쓴다.
 * 같은 계정을 쓰는 값(Cafe24 FTP 로그인, Anthropic 키, 몰 ID)만 adminChat 변수로 대체한다.
 * 값은 호출 시점에 읽는다 (dotenv 로드 순서와 무관하게 동작).
 */
function env(name) {
  const value = process.env[`YOGIBONEWS_${name}`];
  return value === undefined || value === '' ? undefined : value;
}

module.exports = {
  get mongoUri() {
    return env('MONGODB_URI');
  },
  get dbName() {
    return env('DB_NAME') || 'yogibonews';
  },

  get anthropicKey() {
    return env('ANTHROPIC_API_KEY') || process.env.ANTHROPIC_API_KEY;
  },
  // false면 키가 있어도 Claude를 호출하지 않는다 (로컬 개발 시 토큰 과금 방지)
  get claudeEnabled() {
    return env('CLAUDE_ENABLED') !== 'false';
  },
  get claudeModel() {
    return env('CLAUDE_MODEL') || 'claude-opus-5';
  },

  get feedUrl() {
    return env('FEED_URL') || 'https://yogibo.jp/blogs/life.atom';
  },
  // 'off'면 자동 수집을 등록하지 않는다 (로컬 개발 서버가 운영 DB에 대고 중복 수집하지 않도록)
  get fetchCron() {
    return env('FETCH_CRON') || '0 */6 * * *';
  },
  // 판별 대기 글을 한 번에 몇 건까지 처리할지 (과금 상한)
  get reprocessLimit() {
    return Number(env('REPROCESS_LIMIT')) || 30;
  },

  get ftp() {
    return {
      host: env('FTP_HOST') || process.env.FTP_HOST || 'yogibo.ftp.cafe24.com',
      port: Number(env('FTP_PORT') || process.env.FTP_PORT) || 21,
      user: env('FTP_USER') || process.env.FTP_USER,
      password: env('FTP_PASS') || process.env.FTP_PASS,
      remoteDir: env('FTP_REMOTE_DIR') || '/web/news',
      publicBase: env('FTP_PUBLIC_BASE') || 'https://yogibo.openhost.cafe24.com/web/news',
    };
  },

  get cafe24() {
    return {
      mallId: env('CAFE24_MALL_ID') || process.env.CAFE24_MALLID || 'yogibo',
      apiVersion: env('CAFE24_API_VERSION') || process.env.CAFE24_API_VERSION || '2025-12-01',
      // adminChat 안에서는 mount 시 넘겨받은 DB를 쓰고, 단독 실행(로컬 개발)일 때만 이 URI로 직접 붙는다
      tokenUri: env('CAFE24_TOKEN_URI'),
      tokenDb: env('CAFE24_TOKEN_DB') || 'yogibo',
      tokenCollection: env('CAFE24_TOKEN_COLLECTION') || 'tokens',
    };
  },
};
