const ftp = require('basic-ftp');
const { Readable } = require('stream');

/**
 * buffer를 Cafe24 FTP의 지정 디렉토리에 업로드하고 공개 URL을 반환한다.
 * 디렉토리/공개 URL prefix는 FTP_REMOTE_DIR / FTP_PUBLIC_BASE 환경변수로 이 프로젝트 전용 경로를 쓴다.
 */
async function uploadBuffer(buffer, filename, dir) {
  if (!process.env.FTP_USER || !process.env.FTP_PASS) {
    throw new Error('FTP_USER/FTP_PASS 환경변수가 설정되지 않았습니다.');
  }

  const remoteDir = (dir || process.env.FTP_REMOTE_DIR || 'web/news').replace(/^\/+/, '');
  const publicBase = (process.env.FTP_PUBLIC_BASE || 'https://yogibo.cafe24.com/web/news').replace(/\/+$/, '');

  const client = new ftp.Client();
  client.ftp.verbose = false;
  try {
    await client.access({
      host: process.env.FTP_HOST || 'yogibo.ftp.cafe24.com',
      port: process.env.FTP_PORT ? Number(process.env.FTP_PORT) : 21,
      user: process.env.FTP_USER,
      password: process.env.FTP_PASS,
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
