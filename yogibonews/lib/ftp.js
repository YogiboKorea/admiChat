const ftp = require('basic-ftp');
const { Readable } = require('stream');
const config = require('../config');

/**
 * buffer를 Cafe24 FTP의 지정 디렉토리에 업로드하고 공개 URL을 반환한다.
 * 로그인은 adminChat과 같은 Cafe24 FTP 계정을 쓰고, 디렉토리/공개 URL은 YOGIBONEWS_FTP_REMOTE_DIR / YOGIBONEWS_FTP_PUBLIC_BASE 전용 경로를 쓴다.
 */
async function uploadBuffer(buffer, filename, dir) {
  const cfg = config.ftp;
  if (!cfg.user || !cfg.password) {
    throw new Error('FTP 계정 환경변수(FTP_USER/FTP_PASS)가 설정되지 않았습니다.');
  }

  const remoteDir = (dir || cfg.remoteDir).replace(/^\/+/, '');
  const publicBase = cfg.publicBase.replace(/\/+$/, '');

  const client = new ftp.Client();
  client.ftp.verbose = false;
  try {
    await client.access({
      host: cfg.host,
      port: cfg.port,
      user: cfg.user,
      password: cfg.password,
      secure: 'explicit',
    });
    await client.ensureDir(remoteDir);
    const stream = Readable.from(buffer);
    await client.uploadFrom(stream, filename);
  } finally {
    client.close();
  }

  return `${publicBase}/${filename}`;
}

module.exports = { uploadBuffer };
