const express = require("express");
const bodyParser = require("body-parser");
const fs = require("fs");
const path = require("path");
const cors = require("cors");
const compression = require("compression");
const axios = require("axios");
const { MongoClient, ObjectId } = require("mongodb");
require("dotenv").config();
const ExcelJS = require('exceljs');
const moment = require('moment-timezone');
//const { translate: googleTranslate } = await import('@vitalets/google-translate-api');
// ========== [추가] 일본 요기보 뉴스레터 연동 (RSS/Atom) ==========
const Parser = require('rss-parser');
const cheerio = require('cheerio');
const multer = require('multer');
const sharp = require('sharp');
const ftp = require('basic-ftp');
const { Readable } = require('stream');
const os = require('os');
const PDFExtract = require('pdf.js-extract').PDFExtract;
const pdfExtract = new PDFExtract();
const crypto = require('crypto');


// multer 설정 (임시 디스크에 저장하여 메모리 폭발 방지)
const upload = multer({
  storage: multer.diskStorage({
    destination: function (req, file, cb) {
      cb(null, os.tmpdir()); // OS 기본 임시 폴더 사용
    },
    filename: function (req, file, cb) {
      const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
      cb(null, file.fieldname + '-' + uniqueSuffix);
    }
  }),
  limits: { fileSize: 10 * 1024 * 1024 }, // 10MB로 확대
  fileFilter: (req, file, cb) => {
    const allowed = ['image/jpeg', 'image/png', 'image/webp', 'image/gif', 'application/pdf'];
    if (allowed.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error('허용되지 않는 파일 형식입니다. (jpg/png/webp/gif/pdf만 가능)'));
    }
  }
});


const cron = require('node-cron');
const parser = new Parser();


// ========== [1] 환경변수 및 기본 설정 ==========
// 초기값은 비워두거나 안전하게 처리 (DB에서 로드됨)
let accessToken = process.env.ACCESS_TOKEN || '';
let refreshToken = process.env.REFRESH_TOKEN || '';

const CAFE24_CLIENT_ID = process.env.CAFE24_CLIENT_ID;
const CAFE24_CLIENT_SECRET = process.env.CAFE24_CLIENT_SECRET;
const DB_NAME = process.env.DB_NAME;
const MONGODB_URI = process.env.MONGODB_URI;
const CAFE24_MALLID = process.env.CAFE24_MALLID;
const CAFE24_API_VERSION = process.env.CAFE24_API_VERSION || '2025-12-01';

// ★ [핵심] 전역 DB 변수 선언 (모든 API가 공유)
let db;

// ========== [2] Express 앱 기본 설정 ==========
const app = express();
app.use(cors());
app.use(compression());
app.use(express.json({ limit: '50mb' }));
app.use(bodyParser.urlencoded({ limit: '50mb', extended: true }));
app.use(express.static(path.join(__dirname, "public")));
const corsOptions = {
  origin: [
    'https://yogibo.kr',
    'https://www.yogibo.kr',
    'http://yogibo.kr',
    'http://www.yogibo.kr',
    'https://skin-skin123.yogibo.cafe24.com' // 사용 중인 스킨 도메인
  ],
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS']
};
app.use(cors(corsOptions)); // 명시적 CORS 적용



// MongoDB 컬렉션명 정의
const tokenCollectionName = "tokens";
const COLLECTION_COUPON_MAP = "coupon_map";
// ========== [3] MongoDB 토큰 관리 함수 (전역 db 사용) ==========
async function getTokensFromDB() {
  try {
    const collection = db.collection(tokenCollectionName);
    const tokensDoc = await collection.findOne({});

    if (tokensDoc) {
      accessToken = tokensDoc.accessToken;
      refreshToken = tokensDoc.refreshToken;
      console.log('✅ MongoDB에서 토큰 로드 성공:', {
        accessToken: accessToken.substring(0, 10) + '...',
        updatedAt: tokensDoc.updatedAt
      });
    } else {
      console.log('⚠️ MongoDB에 저장된 토큰이 없습니다. (첫 실행이거나 데이터 없음)');
      // 초기 토큰이 환경변수에 있다면 저장 시도
      if (accessToken && refreshToken) {
        await saveTokensToDB(accessToken, refreshToken);
      }
    }
  } catch (error) {
    console.error('❌ 토큰 로드 중 오류:', error);
  }
}

async function saveTokensToDB(newAccessToken, newRefreshToken) {
  try {
    const collection = db.collection(tokenCollectionName);
    await collection.updateOne(
      {},
      {
        $set: {
          accessToken: newAccessToken,
          refreshToken: newRefreshToken,
          updatedAt: new Date(),
        },
      },
      { upsert: true }
    );
    console.log('💾 MongoDB에 토큰 저장(업데이트) 완료');
  } catch (error) {
    console.error('❌ 토큰 저장 중 오류:', error);
  }
}
// ========== [4] 토큰 갱신 및 API 요청 로직 ==========

// 💡 [핵심 추가] 동시성 방어(Lock)를 위한 전역 변수
let isRefreshing = false;
let refreshPromise = null;

// 토큰 갱신 함수 (Promise 락 적용)
async function refreshAccessToken() {
  // 1. 이미 다른 API 요청이 토큰을 갱신하고 있다면, 그 작업이 끝날 때까지 기다립니다.
  if (isRefreshing) {
    console.log('⏳ 다른 요청이 토큰을 갱신 중입니다. 기존 갱신 작업 완료를 대기합니다...');
    return await refreshPromise;
  }

  const now = new Date().toLocaleTimeString();
  console.log(`\n[${now}] 🚨 토큰 갱신 프로세스 시작! (원인: 401 에러 또는 강제 만료)`);

  // 2. 내가 갱신을 시작한다고 락(Lock)을 겁니다.
  isRefreshing = true;

  // 3. 갱신 작업을 Promise로 만들어 다른 요청들이 이 Promise를 기다리게 합니다.
  refreshPromise = (async () => {
    try {
      // 💡 [안전장치] 혹시 모르니 DB에서 최신 토큰을 한 번 더 읽어옵니다. (멀티 프로세스 환경 대비)
      await getTokensFromDB();

      const clientId = (process.env.CAFE24_CLIENT_ID || '').trim();
      const clientSecret = (process.env.CAFE24_CLIENT_SECRET || '').trim();
      const mallId = (process.env.CAFE24_MALLID || '').trim();

      const basicAuth = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');

      console.log(`[${now}] 🚀 Cafe24 서버로 새 토큰 요청 전송...`);

      const response = await axios.post(
        `https://${mallId}.cafe24api.com/api/v2/oauth/token`,
        `grant_type=refresh_token&refresh_token=${refreshToken}`,
        {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': `Basic ${basicAuth}`,
          },
        }
      );

      const newAccessToken = response.data.access_token;
      const newRefreshToken = response.data.refresh_token;

      console.log(`[${now}] ✅ Cafe24 토큰 갱신 성공!`);

      // 메모리 변수 갱신
      accessToken = newAccessToken;
      refreshToken = newRefreshToken;

      // DB 저장
      await saveTokensToDB(newAccessToken, newRefreshToken);
      console.log(`[${now}] 갱신 프로세스 정상 종료.\n`);

      return newAccessToken;

    } catch (error) {
      console.error(`[${now}] ❌ 토큰 갱신 실패:`, error.response ? error.response.data : error.message);
      throw error;
    } finally {
      // 4. 갱신 성공하든 실패하든 무조건 락(Lock)을 풀어줍니다.
      isRefreshing = false;
      refreshPromise = null;
    }
  })();

  // 갱신 작업 실행 후 새 토큰 반환
  return await refreshPromise;
}



// 공통 API 요청 함수 (재시도 로직 포함)
async function apiRequest(method, url, data = {}, params = {}) {
  try {
    const response = await axios({
      method, url, data, params,
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
        'X-Cafe24-Api-Version': CAFE24_API_VERSION
      },
    });
    return response.data;
  } catch (error) {
    // 401 에러 발생 시 토큰 갱신 후 재시도
    if (error.response && error.response.status === 401) {
      console.log(`⚠️ [401 에러 감지] 토큰이 만료되었습니다. 갱신을 시도합니다...`);
      await refreshAccessToken();
      console.log(`🔄 갱신된 토큰으로 API 재요청...`);
      return apiRequest(method, url, data, params); // 재귀 호출
    } else {
      const errorDetails = error.response ? JSON.stringify(error.response.data.error) : '상세 에러 없음';
      console.error(`❌ API 요청 오류 [${error.response?.status}]:`, error.message);
      console.error(`📝 카페24 상세 응답:`, errorDetails);
      throw error;
    }
  }
}

// [테스트용] 토큰 강제 만료 API
app.get('/api/test/expire-token', (req, res) => {
  accessToken = "INVALID_TOKEN_TEST";
  console.log(`\n[TEST] 🧪 현재 AccessToken을 강제로 망가뜨렸습니다: ${accessToken}`);
  res.json({ message: '토큰이 강제로 변조되었습니다. 다음 API 호출 시 갱신이 시도됩니다.' });
});

// [임시] DB 토큰 강제 업데이트 (현재 메모리 값으로)
app.get('/force-update-token', async (req, res) => {
  try {
    await saveTokensToDB(accessToken, refreshToken);
    res.send(`
          <h1>DB 업데이트 완료!</h1>
          <p><b>현재 적용된 토큰:</b> ${accessToken.substring(0, 10)}...</p>
      `);
  } catch (e) {
    res.send(`에러 발생: ${e.message}`);
  }
});


// ========== [5] 럭키 드로우 & 고객 정보 API ==========

async function getCustomerDataByMemberId(memberId) {
  // 토큰이 없으면 DB에서 로드 시도 (혹시 모를 상황 대비)
  if (!accessToken) await getTokensFromDB();

  const url = `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/customersprivacy`;
  const params = { member_id: memberId };
  try {
    const data = await apiRequest('GET', url, {}, params);
    // console.log('Customer Data:', JSON.stringify(data, null, 2)); // 로그 너무 길면 주석
    return data;
  } catch (error) {
    console.error(`Error fetching customer data for member_id ${memberId}:`, error);
    throw error;
  }
}

// 럭키 드로우 참여자 수
app.get('/api/entry/count', async (req, res) => {
  try {
    const count = await db.collection('entries').countDocuments();
    res.json({ count });
  } catch (error) {
    console.error('참여자 수 가져오기 오류:', error);
    res.status(500).json({ error: '서버 내부 오류' });
  }
});

// 럭키 드로우 응모
app.post('/api/entry', async (req, res) => {
  const { memberId } = req.body;
  if (!memberId) {
    return res.status(400).json({ error: 'memberId 값이 필요합니다.' });
  }
  try {
    const customerData = await getCustomerDataByMemberId(memberId);
    if (!customerData || !customerData.customersprivacy) {
      return res.status(404).json({ error: '고객 데이터를 찾을 수 없습니다.' });
    }

    let customerPrivacy = customerData.customersprivacy;
    if (Array.isArray(customerPrivacy)) {
      customerPrivacy = customerPrivacy[0];
    }

    const { member_id, name, cellphone, email, address1, address2, sms, gender } = customerPrivacy;

    const existingEntry = await db.collection('entries').findOne({ memberId: member_id });
    if (existingEntry) {
      return res.status(409).json({ message: '이미 응모하셨습니다.' });
    }

    const createdAtKST = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));

    const newEntry = {
      memberId: member_id,
      name,
      cellphone,
      email,
      address1,
      address2,
      sms,
      gender,
      createdAt: createdAtKST
    };

    const result = await db.collection('entries').insertOne(newEntry);
    res.json({
      message: '이벤트 응모 완료 되었습니다.',
      entry: newEntry,
      insertedId: result.insertedId
    });
  } catch (error) {
    console.error('회원 정보 저장 오류:', error);
    res.status(500).json({ error: '서버 내부 오류' });
  }
});

// 럭키 드로우 엑셀 다운로드
app.get('/api/lucky/download', async (req, res) => {
  try {
    const entries = await db.collection('entries').find({}).toArray();
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Entries');
    worksheet.columns = [
      { header: '참여 날짜', key: 'createdAt', width: 30 },
      { header: '회원아이디', key: 'memberId', width: 20 },
      { header: '회원 성함', key: 'name', width: 20 },
      { header: '휴대폰 번호', key: 'cellphone', width: 20 },
      { header: '이메일', key: 'email', width: 30 },
      { header: '주소', key: 'fullAddress', width: 50 },
      { header: 'SNS 수신여부', key: 'sms', width: 15 },
      { header: '성별', key: 'gender', width: 10 },
    ];

    entries.forEach(entry => {
      const fullAddress = (entry.address1 || '') + (entry.address2 ? ' ' + entry.address2 : '');
      worksheet.addRow({
        createdAt: entry.createdAt,
        memberId: entry.memberId,
        name: entry.name,
        cellphone: entry.cellphone,
        email: entry.email,
        fullAddress: fullAddress,
        sms: entry.sms,
        gender: entry.gender,
      });
    });

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename=luckyEvent.xlsx');
    await workbook.xlsx.write(res);
    res.end();
  } catch (error) {
    console.error('Excel 다운로드 오류:', error);
    res.status(500).json({ error: 'Excel 다운로드 중 오류 발생' });
  }
});


// ========== [6] 2월 출석체크 이벤트 API ==========

// 상태 조회
app.get('/api/event/status', async (req, res) => {
  const { memberId } = req.query;
  if (!memberId) return res.status(400).json({ success: false, message: 'memberId required' });

  try {
    const collection = db.collection('event_daily_checkin');

    // 1. 우리 DB 조회
    const eventDoc = await collection.findOne({ memberId });

    let myCount = 0;
    let isTodayDone = false;
    let isMarketingAgreed = 'F';

    if (eventDoc) {
      myCount = eventDoc.count || 0;
      if (eventDoc.lastParticipatedAt) {
        const lastDate = moment(eventDoc.lastParticipatedAt).tz('Asia/Seoul');
        const today = moment().tz('Asia/Seoul');
        if (lastDate.isSame(today, 'day')) isTodayDone = true;
      }
      if (eventDoc.marketingAgreed === true) isMarketingAgreed = 'T';
    }

    // 2. 우리 DB 미동의 상태면 Cafe24 API '조회'
    if (isMarketingAgreed === 'F') {
      try {
        let realConsent = false;

        // A. 마케팅 동의(Privacy) 확인
        try {
          const privacyUrl = `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/privacyconsents`;
          const privacyRes = await apiRequest('GET', privacyUrl, {}, {
            shop_no: 1, member_id: memberId, consent_type: 'marketing', limit: 1, sort: 'issued_date_desc'
          });
          if (privacyRes.privacy_consents?.length > 0 && privacyRes.privacy_consents[0].agree === 'T') {
            realConsent = true;
          }
        } catch (e) { }

        // B. SMS 수신동의 확인 (Fallback)
        if (!realConsent) {
          try {
            const customerUrl = `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/customers`;
            const customerRes = await apiRequest('GET', customerUrl, {}, {
              member_id: memberId, fields: 'sms,news_mail'
            });
            if (customerRes.customers?.length > 0) {
              const { sms, news_mail } = customerRes.customers[0];
              if (sms === 'T' || news_mail === 'T') realConsent = true;
            }
          } catch (e) { }
        }

        // ★ [DB저장] 기존 동의자로 확인됨
        if (realConsent) {
          console.log(`[Sync] ${memberId} 기존 동의 확인 -> DB 업데이트 (EXISTING)`);
          await collection.updateOne(
            { memberId: memberId },
            {
              $set: {
                marketingAgreed: true,
                marketingAgreedAt: new Date(),
                consentType: 'EXISTING'
              },
              $setOnInsert: { count: 0, firstParticipatedAt: new Date() }
            },
            { upsert: true }
          );
          isMarketingAgreed = 'T';
        }
      } catch (e) {
        // API 오류 시 무시
      }
    }

    res.json({ success: true, count: myCount, todayDone: isTodayDone, marketing_consent: isMarketingAgreed });

  } catch (err) {
    console.error('Status Error:', err);
    res.status(500).json({ success: false });
  }
});

// 참여하기
app.post('/api/event/participate', async (req, res) => {
  const { memberId } = req.body;
  if (!memberId) return res.status(400).json({ success: false, message: 'Login required' });

  try {
    const collection = db.collection('event_daily_checkin');

    const eventDoc = await collection.findOne({ memberId });
    const nowKST = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
    const todayMoment = moment(nowKST).tz('Asia/Seoul');

    if (eventDoc) {
      // 3회 이상이면 차단
      if ((eventDoc.count || 0) >= 3) {
        return res.json({ success: false, message: '모든 이벤트 참여가 완료되었습니다!' });
      }
      // 날짜 중복 체크
      if (eventDoc.lastParticipatedAt) {
        const lastDate = moment(eventDoc.lastParticipatedAt).tz('Asia/Seoul');
        if (lastDate.isSame(todayMoment, 'day')) {
          return res.json({ success: false, message: '당일 참여 완료한 이벤트 입니다.' });
        }
      }
    }

    const updateResult = await collection.findOneAndUpdate(
      { memberId: memberId },
      {
        $inc: { count: 1 },
        $set: { lastParticipatedAt: nowKST },
        $push: { history: nowKST },
        $setOnInsert: { firstParticipatedAt: nowKST, marketingAgreed: false }
      },
      { upsert: true, returnDocument: 'after' }
    );

    const updatedDoc = updateResult.value || updateResult;
    const newCount = updatedDoc ? updatedDoc.count : 1;
    const msg = newCount >= 3 ? '모든 이벤트 참여 완료!' : '출석 완료!';

    res.json({ success: true, count: newCount, message: msg });

  } catch (err) {
    console.error('Participate Error:', err);
    res.status(500).json({ success: false });
  }
});

// 마케팅 동의
app.post('/api/event/marketing-consent', async (req, res) => {
  const { memberId } = req.body;
  if (!memberId) return res.status(400).json({ error: 'memberId required' });

  try {
    const collection = db.collection('event_daily_checkin');

    await collection.updateOne(
      { memberId: memberId },
      {
        $set: {
          marketingAgreed: true,
          marketingAgreedAt: new Date(),
          consentType: 'NEW'
        },
        $setOnInsert: { count: 0, firstParticipatedAt: new Date() }
      },
      { upsert: true }
    );

    console.log(`[DB] ${memberId} 신규 마케팅 동의 저장 (NEW)`);
    res.json({ success: true, message: '마케팅 동의 완료' });

  } catch (err) {
    console.error('Consent Error:', err);
    res.status(500).json({ error: 'Error' });
  }
});

// 출석체크 엑셀 다운로드
app.get('/api/event/download', async (req, res) => {
  try {
    const entries = await db.collection('event_daily_checkin').find({ count: { $gt: 0 } }).toArray();

    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Participants');

    worksheet.columns = [
      { header: 'ID', key: 'memberId', width: 20 },
      { header: '참여횟수', key: 'count', width: 10 },
      { header: '마케팅 수신동의 여부', key: 'marketingAgreed', width: 15 },
      { header: '동의 구분', key: 'consentType', width: 25 },
      { header: '마지막 참여날짜', key: 'lastParticipatedAt', width: 15 },
      { header: '처음 참여날짜', key: 'firstParticipatedAt', width: 15 }
    ];

    entries.forEach(entry => {
      const fmt = (d) => d ? moment(d).tz('Asia/Seoul').format('YYYY-MM-DD') : '-';

      let consentLabel = '-';
      if (entry.marketingAgreed) {
        if (entry.consentType === 'NEW') {
          consentLabel = '신규 동의 (이벤트)';
        } else if (entry.consentType === 'EXISTING') {
          consentLabel = '기존 동의 (SMS/마케팅)';
        } else {
          consentLabel = '확인 필요 (기존)';
        }
      }

      worksheet.addRow({
        memberId: entry.memberId,
        count: entry.count || 0,
        marketingAgreed: entry.marketingAgreed ? 'O' : 'X',
        consentType: consentLabel,
        lastParticipatedAt: fmt(entry.lastParticipatedAt),
        firstParticipatedAt: fmt(entry.firstParticipatedAt)
      });
    });

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename=Event2_Participants.xlsx`);
    await workbook.xlsx.write(res);
    res.end();

  } catch (err) {
    console.error(err);
    res.status(500).send('Excel Error');
  }
});


// ========== [7] 로그 수집 및 통계 API (전역 db 사용) ==========

// 로그 수집
app.post('/api/trace/log', async (req, res) => {
  try {
    let userIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '';
    if (userIp.includes(',')) userIp = userIp.split(',')[0].trim();

    const BLOCKED_IPS = ['127.0.0.1', '61.99.75.10'];

    const { isDev } = req.body;

    if (BLOCKED_IPS.includes(userIp) && !isDev) {
      return res.json({ success: true, msg: 'IP Filtered' });
    }

    let { eventTag, visitorId, currentUrl, prevUrl, utmData, deviceType } = req.body;

    console.log('[LOG] 요청:', {
      visitorId,
      currentUrl: currentUrl?.substring(0, 50),
      userIp,
      isDev
    });

    const isRealMember = visitorId && !/guest_/i.test(visitorId) && visitorId !== 'null';

    if (isRealMember) {
      const mergeTimeLimit = new Date(Date.now() - 5 * 60 * 1000);

      const mergeResult = await db.collection('visit_logs1Event').updateMany(
        {
          userIp: userIp,
          visitorId: { $regex: /^guest_/i },
          createdAt: { $gte: mergeTimeLimit }
        },
        { $set: { visitorId: visitorId, isMember: true } }
      );

      if (mergeResult.modifiedCount > 0) {
        console.log(`[MERGE] ${mergeResult.modifiedCount}건 병합 → ${visitorId}`);
      }

      await db.collection('event01ClickData').updateMany(
        {
          ip: userIp,
          visitorId: { $regex: /^guest_/i },
          createdAt: { $gte: mergeTimeLimit }
        },
        { $set: { visitorId: visitorId } }
      );
    }

    if (!isRealMember) {
      const existingGuestLog = await db.collection('visit_logs1Event').findOne(
        {
          userIp: userIp,
          visitorId: { $regex: /^guest_/i },
          createdAt: { $gte: new Date(Date.now() - 30 * 60 * 1000) }
        },
        { sort: { createdAt: -1 } }
      );

      if (existingGuestLog && existingGuestLog.visitorId) {
        visitorId = existingGuestLog.visitorId;
      }
    }

    let isNewSession = true;
    let skipReason = null;
    let isRevisit = false;

    if (visitorId) {
      const lastLog = await db.collection('visit_logs1Event').findOne(
        { visitorId: visitorId },
        { sort: { createdAt: -1 } }
      );

      if (lastLog) {
        const timeDiff = Date.now() - new Date(lastLog.createdAt).getTime();
        const SESSION_TIMEOUT = 30 * 60 * 1000;

        if (timeDiff < 2 * 60 * 1000 && lastLog.currentUrl === currentUrl) {
          skipReason = 'Duplicate (same URL within 2min)';
        }

        if (timeDiff < SESSION_TIMEOUT) {
          isNewSession = false;
          isRevisit = lastLog.isRevisit || false;

        } else {
          isNewSession = true;
          const pastLog = await db.collection('visit_logs1Event').findOne({
            visitorId: visitorId,
            createdAt: { $lt: new Date(Date.now() - 24 * 60 * 60 * 1000) }
          });

          if (pastLog) {
            isRevisit = true;
            console.log(`[REVISIT] 재방문 유저 확인: ${visitorId}`);
          } else {
            isRevisit = false;
          }
        }
      } else {
        isRevisit = false;
      }
    }

    if (skipReason) {
      console.log(`[SKIP] ${skipReason}`);
      return res.json({ success: true, msg: skipReason });
    }

    const hasPromoVisit = await db.collection('visit_logs1Event').findOne({
      $or: [{ visitorId: visitorId }, { userIp: userIp }],
      currentUrl: { $regex: '1_promotion.html' }
    });

    if (isNewSession && !hasPromoVisit) {
      if (currentUrl && !currentUrl.includes('1_promotion.html')) {
        return res.json({ success: true, msg: 'Not entry point' });
      }
    }

    if (currentUrl && currentUrl.includes('skin-skin')) {
      return res.json({ success: true, msg: 'Skin Ignored' });
    }

    const log = {
      visitorId: visitorId,
      isMember: !!isRealMember,
      eventTag: eventTag,
      currentUrl: currentUrl,
      prevUrl: prevUrl,
      utmData: utmData || {},
      userIp: userIp,
      deviceType: deviceType || 'unknown',
      duration: 0,
      isRevisit: isRevisit,
      createdAt: new Date()
    };

    const result = await db.collection('visit_logs1Event').insertOne(log);

    const logStatus = isRevisit ? '[REVISIT]' : '[NEW]';
    console.log(`[SAVE] ${logStatus} ${visitorId} (Session: ${isNewSession ? 'New' : 'Cont'})`);

    res.json({ success: true, logId: result.insertedId });

  } catch (e) {
    console.error('[ERROR]', e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// 체류 시간 업데이트
app.post('/api/trace/log/exit', async (req, res) => {
  let { logId, duration } = req.body;
  if (!logId || duration === undefined) return res.status(400).send('Missing Data');

  try {
    await db.collection('visit_logs1Event').updateOne(
      { _id: new ObjectId(logId) },
      { $set: { duration: parseInt(duration) } }
    );
    res.send('OK');
  } catch (error) {
    console.error(error);
    res.status(500).send('Error');
  }
});

// 관리자 대시보드: 요약
app.get('/api/trace/summary', async (req, res) => {
  try {
    const stats = await db.collection('visit_logs1Event').aggregate([
      {
        $group: {
          _id: "$eventTag",
          totalHits: { $sum: 1 },
          uniqueVisitors: { $addToSet: "$visitorId" },
          lastActive: { $max: "$createdAt" }
        }
      },
      {
        $project: {
          _id: 1,
          totalHits: 1,
          uniqueVisitors: { $size: "$uniqueVisitors" },
          lastActive: 1
        }
      },
      { $sort: { totalHits: -1 } }
    ]).toArray();

    res.json({ success: true, data: stats });
  } catch (err) {
    res.status(500).json({ msg: 'Server Error' });
  }
});

// 방문자 목록 조회
app.get('/api/trace/visitors', async (req, res) => {
  try {
    const { date } = req.query;
    let matchStage = {};

    if (date) {
      matchStage.createdAt = {
        $gte: new Date(date + "T00:00:00.000Z"),
        $lte: new Date(date + "T23:59:59.999Z")
      };
    }

    const visitors = await db.collection('visit_logs1Event').aggregate([
      { $match: matchStage },
      { $sort: { createdAt: -1 } },
      {
        $group: {
          _id: {
            $cond: [
              { $regexMatch: { input: "$visitorId", regex: /^guest_/i } },
              "$userIp",
              "$visitorId"
            ]
          },
          visitorId: { $first: "$visitorId" },
          isMember: { $first: "$isMember" },
          eventTag: { $first: "$eventTag" },
          lastAction: { $first: "$createdAt" },
          count: { $sum: 1 },
          userIp: { $first: "$userIp" },
          hasVisitedEvent: {
            $max: {
              $cond: [
                { $regexMatch: { input: "$currentUrl", regex: "1_promotion.html" } }, 1, 0
              ]
            }
          }
        }
      },
      {
        $addFields: {
          searchId: "$_id"
        }
      },
      { $sort: { lastAction: -1 } },
      { $limit: 150 }
    ], { allowDiskUse: true }).toArray();

    res.json({ success: true, visitors });
  } catch (err) {
    console.error(err);
    res.status(500).json({ msg: 'Server Error' });
  }
});

// Journey
app.get('/api/trace/journey/:visitorId', async (req, res) => {
  const { visitorId } = req.params;
  const { startDate, endDate } = req.query;

  console.log('[Journey] 요청:', { visitorId, startDate, endDate });

  try {
    let dateFilter = null;

    if (startDate) {
      const start = new Date(startDate + 'T00:00:00.000Z');
      const end = endDate
        ? new Date(endDate + 'T23:59:59.999Z')
        : new Date(startDate + 'T23:59:59.999Z');

      dateFilter = { $gte: start, $lte: end };
    }

    const isIpFormat = /^(\d{1,3}\.){3}\d{1,3}$/.test(visitorId) || visitorId.includes(':');
    const isGuestId = visitorId.toLowerCase().startsWith('guest_');
    const isMemberId = !isIpFormat && !isGuestId;

    let baseQuery = {};
    let clickQuery = {};

    if (isMemberId) {
      baseQuery = { visitorId: visitorId };
      clickQuery = { visitorId: visitorId };
    }
    else if (isIpFormat) {
      baseQuery = {
        userIp: visitorId,
        visitorId: { $regex: /^guest_/i }
      };
      clickQuery = {
        ip: visitorId,
        visitorId: { $regex: /^guest_/i }
      };
    }
    else if (isGuestId) {
      const guestLog = await db.collection('visit_logs1Event').findOne(
        { visitorId: visitorId },
        { projection: { userIp: 1 } }
      );

      if (guestLog && guestLog.userIp) {
        baseQuery = {
          userIp: guestLog.userIp,
          visitorId: { $regex: /^guest_/i }
        };
        clickQuery = {
          ip: guestLog.userIp,
          visitorId: { $regex: /^guest_/i }
        };
      } else {
        baseQuery = { visitorId: visitorId };
        clickQuery = { visitorId: visitorId };
      }
    }

    if (dateFilter) {
      baseQuery = { $and: [baseQuery, { createdAt: dateFilter }] };
      clickQuery = { $and: [clickQuery, { createdAt: dateFilter }] };
    }

    const views = await db.collection('visit_logs1Event')
      .find(baseQuery)
      .sort({ createdAt: 1 })
      .project({ currentUrl: 1, createdAt: 1, visitorId: 1, _id: 0 })
      .toArray();

    const formattedViews = views.map(v => ({
      type: 'VIEW',
      title: v.currentUrl,
      url: v.currentUrl,
      timestamp: v.createdAt
    }));

    const clicks = await db.collection('event01ClickData')
      .find(clickQuery)
      .sort({ createdAt: 1 })
      .project({ sectionName: 1, sectionId: 1, createdAt: 1, _id: 0 })
      .toArray();

    const formattedClicks = clicks.map(c => ({
      type: 'CLICK',
      title: `👉 [클릭] ${c.sectionName}`,
      url: '',
      timestamp: c.createdAt
    }));

    const journey = [...formattedViews, ...formattedClicks];
    journey.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

    res.json({ success: true, journey });

  } catch (error) {
    console.error('[Journey Error]', error);
    res.status(500).json({ msg: 'Server Error', error: error.message });
  }
});

// 퍼널 분석
app.get('/api/trace/funnel', async (req, res) => {
  try {
    const { startDate, endDate } = req.query;

    let dateFilter = {};
    if (startDate || endDate) {
      dateFilter = {};
      if (startDate) dateFilter.$gte = new Date(startDate + "T00:00:00.000Z");
      if (endDate) dateFilter.$lte = new Date(endDate + "T23:59:59.999Z");
    }

    const validVisitors = await db.collection('visit_logs1Event').distinct('visitorId', {
      createdAt: dateFilter,
      currentUrl: { $regex: '1_promotion.html|index.html|store.html' }
    });

    if (validVisitors.length === 0) {
      return res.json({ success: true, data: [] });
    }

    const pipeline = [
      {
        $match: {
          createdAt: dateFilter,
          visitorId: { $in: validVisitors }
        }
      },
      {
        $project: {
          visitorId: 1,
          userIp: 1,
          currentUrl: 1,
          uniqueId: {
            $cond: [
              { $regexMatch: { input: "$visitorId", regex: /^guest_/i } },
              "$userIp",
              "$visitorId"
            ]
          },
          channelName: {
            $switch: {
              branches: [
                { case: { $eq: ["$utmData.campaign", "home_main"] }, then: "브검 : 홈페이지 메인" },
                { case: { $eq: ["$utmData.campaign", "naver_main"] }, then: "브검 : 1월 말할 수 없는 편안함(메인)" },
                { case: { $eq: ["$utmData.campaign", "naver_sub1"] }, then: "브검 : 1월 말할 수 없는 편안함(서브1)_10%" },
                { case: { $eq: ["$utmData.campaign", "naver_sub2"] }, then: "브검 : 1월 말할 수 없는 편안함(서브2)_20%" },
                { case: { $eq: ["$utmData.campaign", "naver_sub3"] }, then: "브검 : 1월 말할 수 없는 편안함(서브3)_갓생" },
                { case: { $eq: ["$utmData.campaign", "naver_sub4"] }, then: "브검 : 1월 말할 수 없는 편안함(서브4)_무료배송" },
                { case: { $eq: ["$utmData.campaign", "naver_sub5"] }, then: "브검 : 1월 말할 수 없는 편안함(서브5)_가까운매장" },
                { case: { $eq: ["$utmData.content", "employee_discount"] }, then: "메타 : 1월 말할 수 없는 편안함(직원 할인 찬스)" },
                { case: { $eq: ["$utmData.content", "areading_group1"] }, then: "메타 : 1월 말할 수 없는 편안함(sky독서소파)" },
                { case: { $eq: ["$utmData.content", "areading_group2"] }, then: "메타 : 1월 말할 수 없는 편안함(sky독서소파2)" },
                { case: { $eq: ["$utmData.content", "special_price1"] }, then: "메타 : 1월 말할 수 없는 편안함(신년특가1)" },
                { case: { $eq: ["$utmData.content", "special_price2"] }, then: "메타 : 1월 말할 수 없는 편안함(신년특가2)" },
                { case: { $eq: ["$utmData.content", "horse"] }, then: "메타 : 1월 말할 수 없는 편안함(말 ai아님)" },
                { case: { $eq: ["$utmData.campaign", "message_main"] }, then: "플친 : 1월 말할 수 없는 편안함(메인)" },
                { case: { $eq: ["$utmData.campaign", "message_sub1"] }, then: "플친 : 1월 말할 수 없는 편안함(10%)" },
                { case: { $eq: ["$utmData.campaign", "message_sub2"] }, then: "플친 : 1월 말할 수 없는 편안함(20%)" },
                { case: { $eq: ["$utmData.campaign", "message_sub3"] }, then: "플친 : 1월 말할 수 없는 편안함(지원이벤트)" },
                { case: { $eq: ["$utmData.campaign", "message_sub4"] }, then: "플친 : 1월 말할 수 없는 편안함(무료배송)" }
              ],
              default: "직접/기타 방문"
            }
          }
        }
      },
      {
        $group: {
          _id: "$channelName",
          step1_visitors: { $addToSet: "$uniqueId" },
          step2_visitors: {
            $addToSet: {
              $cond: [{ $regexMatch: { input: "$currentUrl", regex: "product|detail.html" } }, "$uniqueId", "$$REMOVE"]
            }
          },
          step3_visitors: {
            $addToSet: {
              $cond: [{ $regexMatch: { input: "$currentUrl", regex: "basket.html" } }, "$uniqueId", "$$REMOVE"]
            }
          },
          step4_visitors: {
            $addToSet: {
              $cond: [{ $regexMatch: { input: "$currentUrl", regex: "orderform.html" } }, "$uniqueId", "$$REMOVE"]
            }
          },
          step5_visitors: {
            $addToSet: {
              $cond: [{ $regexMatch: { input: "$currentUrl", regex: "order_result.html" } }, "$uniqueId", "$$REMOVE"]
            }
          }
        }
      },
      {
        $project: {
          _id: 0,
          channelName: "$_id",
          count_total: { $size: "$step1_visitors" },
          count_detail: { $size: "$step2_visitors" },
          count_cart: { $size: "$step3_visitors" },
          count_order: { $size: "$step4_visitors" },
          count_purchase: { $size: "$step5_visitors" }
        }
      },
      { $sort: { count_total: -1 } }
    ];

    const funnelData = await db.collection('visit_logs1Event').aggregate(pipeline).toArray();
    res.json({ success: true, data: funnelData });

  } catch (err) {
    console.error(err);
    res.status(500).json({ msg: 'Server Error' });
  }
});

// 섹션 클릭 로그
app.post('/api/trace/click', async (req, res) => {
  try {
    let userIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '';
    if (userIp.includes(',')) {
      userIp = userIp.split(',')[0].trim();
    }

    const BLOCKED_IPS = ['127.0.0.1', '::1'];
    if (BLOCKED_IPS.includes(userIp)) {
      return res.json({ success: true, msg: 'IP Filtered' });
    }

    const { sectionId, sectionName, visitorId } = req.body;

    if (!sectionId || !sectionName) {
      return res.status(400).json({ success: false, msg: 'Missing Data' });
    }

    const clickLog = {
      sectionId,
      sectionName,
      visitorId: visitorId || 'guest',
      ip: userIp,
      createdAt: new Date()
    };

    await db.collection('event01ClickData').insertOne(clickLog);

    res.json({ success: true });

  } catch (e) {
    console.error(e);
    res.status(500).json({ success: false });
  }
});

// 섹션 클릭 통계
app.get('/api/trace/clicks/stats', async (req, res) => {
  try {
    const { startDate, endDate } = req.query;

    let matchStage = {};
    if (startDate || endDate) {
      matchStage.createdAt = {};
      if (startDate) matchStage.createdAt.$gte = new Date(startDate + "T00:00:00.000Z");
      if (endDate) matchStage.createdAt.$lte = new Date(endDate + "T23:59:59.999Z");
    }

    const stats = await db.collection('event01ClickData').aggregate([
      { $match: matchStage },
      {
        $group: {
          _id: "$sectionId",
          name: { $first: "$sectionName" },
          count: { $sum: 1 }
        }
      },
      { $sort: { count: -1 } }
    ]).toArray();

    const formattedData = stats.map(item => ({
      id: item._id,
      name: item.name,
      count: item.count
    }));

    res.json({ success: true, data: formattedData });

  } catch (err) {
    console.error(err);
    res.status(500).json({ msg: 'Server Error' });
  }
});

// 클릭 상세 조회
app.get('/api/trace/visitors/by-click', async (req, res) => {
  try {
    const { sectionId, startDate, endDate } = req.query;

    const start = startDate ? new Date(startDate + 'T00:00:00.000Z') : new Date(0);
    const end = endDate ? new Date(endDate + 'T23:59:59.999Z') : new Date();

    const clickLogs = await db.collection('event01ClickData').find({
      sectionId: sectionId,
      createdAt: { $gte: start, $lte: end }
    }).sort({ createdAt: -1 }).toArray();

    if (clickLogs.length === 0) {
      return res.json({ success: true, visitors: [], msg: '클릭 기록 없음' });
    }

    const uniqueVisitors = {};

    for (const log of clickLogs) {
      const vid = log.visitorId || log.ip || 'Unknown';

      if (!uniqueVisitors[vid]) {
        uniqueVisitors[vid] = {
          _id: vid,
          lastAction: log.createdAt,
          isMember: (vid && !vid.startsWith('guest_') && vid !== 'null' && vid !== 'guest'),
          currentUrl: '',
          userIp: log.ip,
          count: 1
        };
      } else {
        uniqueVisitors[vid].count++;
      }
    }

    const visitors = Object.values(uniqueVisitors);

    res.json({ success: true, visitors: visitors });

  } catch (error) {
    console.error('클릭 방문자 조회 실패:', error);
    res.status(500).json({ success: false, message: '서버 오류' });
  }
});

// 인기 페이지 조회
app.get('/api/trace/stats/pages', async (req, res) => {
  try {
    const { startDate, endDate } = req.query;
    let matchStage = {};

    if (startDate || endDate) {
      matchStage.createdAt = {};
      if (startDate) matchStage.createdAt.$gte = new Date(startDate + "T00:00:00.000Z");
      if (endDate) matchStage.createdAt.$lte = new Date(endDate + "T23:59:59.999Z");
    }

    const pipeline = [
      { $match: matchStage },
      {
        $group: {
          _id: "$currentUrl",
          count: { $sum: 1 },
          visitors: { $addToSet: "$visitorId" }
        }
      },
      {
        $project: {
          url: "$_id",
          count: 1,
          visitors: 1,
          visitorCount: { $size: "$visitors" }
        }
      },
      { $sort: { count: -1 } },
      { $limit: 100 }
    ];

    const data = await db.collection('visit_logs1Event').aggregate(pipeline, { allowDiskUse: true }).toArray();
    res.json({ success: true, data });

  } catch (err) {
    console.error(err);
    res.status(500).json({ msg: 'Server Error' });
  }
});

// 흐름 분석
app.get('/api/trace/stats/flow', async (req, res) => {
  try {
    const { startDate, endDate } = req.query;

    let matchStage = {
      prevUrl: { $regex: 'category|list.html' },
      $and: [
        { currentUrl: { $regex: 'product|detail.html' } },
        { currentUrl: { $not: { $regex: 'list.html' } } }
      ]
    };

    if (startDate || endDate) {
      matchStage.createdAt = {};
      if (startDate) matchStage.createdAt.$gte = new Date(startDate + "T00:00:00.000Z");
      if (endDate) matchStage.createdAt.$lte = new Date(endDate + "T23:59:59.999Z");
    }

    const pipeline = [
      { $match: matchStage },
      {
        $group: {
          _id: { category: "$prevUrl", product: "$currentUrl" },
          count: { $sum: 1 },
          visitors: { $addToSet: "$visitorId" }
        }
      },
      { $sort: { count: -1 } },
      {
        $group: {
          _id: "$_id.category",
          totalCount: { $sum: "$count" },
          topProducts: {
            $push: {
              productUrl: "$_id.product",
              count: "$count",
              visitors: "$visitors"
            }
          }
        }
      },
      { $sort: { totalCount: -1 } },
      { $limit: 30 }
    ];

    const data = await db.collection('visit_logs1Event').aggregate(pipeline, { allowDiskUse: true }).toArray();
    res.json({ success: true, data });

  } catch (err) {
    console.error(err);
    res.status(500).json({ msg: 'Server Error' });
  }
});

// 채널별 방문자
app.get('/api/trace/visitors/by-channel', async (req, res) => {
  try {
    const { channelName, startDate, endDate } = req.query;

    if (!channelName) {
      return res.status(400).json({ success: false, msg: 'Missing channelName' });
    }

    let dateFilter = {};
    if (startDate || endDate) {
      dateFilter = {};
      if (startDate) dateFilter.$gte = new Date(startDate + "T00:00:00.000Z");
      if (endDate) dateFilter.$lte = new Date(endDate + "T23:59:59.999Z");
    }

    const validVisitors = await db.collection('visit_logs1Event').distinct('visitorId', {
      createdAt: dateFilter,
      currentUrl: { $regex: '1_promotion.html|index.html|store.html' }
    });

    if (!validVisitors || validVisitors.length === 0) {
      return res.json({ success: true, visitors: [] });
    }

    const channelNameExpr = {
      $switch: {
        branches: [
          { case: { $eq: ["$utmData.campaign", "home_main"] }, then: "브검 : 홈페이지 메인" },
          { case: { $eq: ["$utmData.campaign", "naver_main"] }, then: "브검 : 1월 말할 수 없는 편안함(메인)" },
          { case: { $eq: ["$utmData.campaign", "naver_sub1"] }, then: "브검 : 1월 말할 수 없는 편안함(서브1)_10%" },
          { case: { $eq: ["$utmData.campaign", "naver_sub2"] }, then: "브검 : 1월 말할 수 없는 편안함(서브2)_20%" },
          { case: { $eq: ["$utmData.campaign", "naver_sub3"] }, then: "브검 : 1월 말할 수 없는 편안함(서브3)_갓생" },
          { case: { $eq: ["$utmData.campaign", "naver_sub4"] }, then: "브검 : 1월 말할 수 없는 편안함(서브4)_무료배송" },
          { case: { $eq: ["$utmData.campaign", "naver_sub5"] }, then: "브검 : 1월 말할 수 없는 편안함(서브5)_가까운매장" },
          { case: { $eq: ["$utmData.content", "employee_discount"] }, then: "메타 : 1월 말할 수 없는 편안함(직원 할인 찬스)" },
          { case: { $eq: ["$utmData.content", "areading_group1"] }, then: "메타 : 1월 말할 수 없는 편안함(sky독서소파)" },
          { case: { $eq: ["$utmData.content", "areading_group2"] }, then: "메타 : 1월 말할 수 없는 편안함(sky독서소파2)" },
          { case: { $eq: ["$utmData.content", "special_price1"] }, then: "메타 : 1월 말할 수 없는 편안함(신년특가1)" },
          { case: { $eq: ["$utmData.content", "special_price2"] }, then: "메타 : 1월 말할 수 없는 편안함(신년특가2)" },
          { case: { $eq: ["$utmData.content", "horse"] }, then: "메타 : 1월 말할 수 없는 편안함(말 ai아님)" },
          { case: { $eq: ["$utmData.campaign", "message_main"] }, then: "플친 : 1월 말할 수 없는 편안함(메인)" },
          { case: { $eq: ["$utmData.campaign", "message_sub1"] }, then: "플친 : 1월 말할 수 없는 편안함(10%)" },
          { case: { $eq: ["$utmData.campaign", "message_sub2"] }, then: "플친 : 1월 말할 수 없는 편안함(20%)" },
          { case: { $eq: ["$utmData.campaign", "message_sub3"] }, then: "플친 : 1월 말할 수 없는 편안함(지원이벤트)" },
          { case: { $eq: ["$utmData.campaign", "message_sub4"] }, then: "플친 : 1월 말할 수 없는 편안함(무료배송)" }
        ],
        default: "직접/기타 방문"
      }
    };

    const pipeline = [
      {
        $match: {
          createdAt: dateFilter,
          visitorId: { $in: validVisitors }
        }
      },
      {
        $project: {
          visitorId: 1,
          userIp: 1,
          isMember: 1,
          isRevisit: 1,
          currentUrl: 1,
          createdAt: 1,
          uniqueId: {
            $cond: [
              { $regexMatch: { input: "$visitorId", regex: /^guest_/i } },
              "$userIp",
              "$visitorId"
            ]
          },
          channelName: channelNameExpr
        }
      },
      { $match: { channelName: channelName } },
      {
        $group: {
          _id: "$uniqueId",
          visitorId: { $last: "$visitorId" },
          userIp: { $last: "$userIp" },
          isMember: { $max: { $cond: ["$isMember", 1, 0] } },
          lastAction: { $max: "$createdAt" },
          count: { $sum: 1 },
          isRevisit: { $max: "$isRevisit" }
        }
      },
      {
        $project: {
          _id: 0,
          searchId: "$_id",
          visitorId: 1,
          isMember: { $toBool: "$isMember" },
          lastAction: 1,
          userIp: 1,
          count: 1,
          isRevisit: 1
        }
      },
      { $sort: { lastAction: -1 } },
      { $limit: 100 }
    ];

    const visitors = await db.collection('visit_logs1Event')
      .aggregate(pipeline, { allowDiskUse: true })
      .toArray();

    return res.json({ success: true, visitors });

  } catch (err) {
    console.error('API 11 Error:', err);
    return res.status(500).json({ msg: 'Server Error', error: err.toString() });
  }
});


// ========== [8] 메타/인스타그램 관련 API ==========

const INSTAGRAM_TOKEN = process.env.INSTAGRAM_TOKEN;
const SALLYFELLTOKEN = process.env.SALLYFELLTOKEN;

// 카테고리 조회
app.get('/api/meta/categories', async (req, res) => {
  const url = `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/categories`;

  try {
    let allCategories = [];
    let offset = 0;
    let hasMore = true;
    const LIMIT = 100;

    console.log(`[Category] 카테고리 전체 데이터 수집 시작...`);

    while (hasMore) {
      const response = await axios.get(url, {
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
          'X-Cafe24-Api-Version': CAFE24_API_VERSION
        },
        params: {
          shop_no: 1,
          limit: LIMIT,
          offset: offset,
          fields: 'category_no,category_name'
        }
      });

      const cats = response.data.categories;

      if (cats && cats.length > 0) {
        allCategories = allCategories.concat(cats);

        if (cats.length < LIMIT) {
          hasMore = false;
        } else {
          offset += LIMIT;
        }
      } else {
        hasMore = false;
      }
    }

    const categoryMap = {};
    allCategories.forEach(cat => {
      categoryMap[cat.category_no] = cat.category_name;
    });

    console.log(`[Category] 총 ${allCategories.length}개의 카테고리 로드 완료`);
    res.json({ success: true, data: categoryMap });

  } catch (error) {
    if (error.response && error.response.status === 401) {
      try {
        console.log('Token expired. Refreshing...');
        await refreshAccessToken();
        return res.redirect(req.originalUrl);
      } catch (e) {
        return res.status(401).json({ error: "Token refresh failed" });
      }
    }
    console.error("카테고리 전체 조회 실패:", error.message);
    res.status(500).json({ success: false, message: 'Server Error' });
  }
});

// 상품 조회
app.get('/api/meta/products', async (req, res) => {
  const url = `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/products`;

  try {
    let allProducts = [];
    let offset = 0;
    let hasMore = true;
    const LIMIT = 100;

    console.log(`[Product] 상품 전체 데이터 수집 시작...`);

    while (hasMore) {
      const response = await axios.get(url, {
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
          'X-Cafe24-Api-Version': CAFE24_API_VERSION
        },
        params: {
          shop_no: 1,
          limit: LIMIT,
          offset: offset,
          fields: 'product_no,product_name'
        }
      });

      const products = response.data.products;

      if (products && products.length > 0) {
        allProducts = allProducts.concat(products);

        if (products.length < LIMIT) {
          hasMore = false;
        } else {
          offset += LIMIT;
        }
      } else {
        hasMore = false;
      }
    }

    const productMap = {};
    allProducts.forEach(prod => {
      productMap[prod.product_no] = prod.product_name;
    });

    console.log(`[Product] 총 ${allProducts.length}개의 상품 정보 로드 완료`);
    res.json({ success: true, data: productMap });

  } catch (error) {
    if (error.response && error.response.status === 401) {
      try {
        await refreshAccessToken();
        return res.redirect(req.originalUrl);
      } catch (e) {
        return res.status(401).json({ error: "Token refresh failed" });
      }
    }
    console.error("상품 전체 조회 실패:", error.message);
    res.status(500).json({ success: false, message: 'Server Error' });
  }
});

// 인스타그램 피드 1
app.get("/api/instagramFeed", async (req, res) => {
  try {
    const pageLimit = 40;
    const url = `https://graph.instagram.com/v22.0/me/media?access_token=${INSTAGRAM_TOKEN}&fields=id,caption,media_url,permalink,media_type,timestamp&limit=${pageLimit}`;
    const response = await axios.get(url);
    const feedData = response.data;

    // DB 저장 (비동기)
    saveInstagramFeedData(feedData);

    res.json(feedData);
  } catch (error) {
    console.error("Error fetching Instagram feed:", error.message);
    res.status(500).json({ error: "Failed to fetch Instagram feed" });
  }
});

// 인스타그램 피드 2 (샐리필)
app.get("/api/instagramSallyFeed", async (req, res) => {
  try {
    const pageLimit = 16;
    const url = `https://graph.instagram.com/v22.0/me/media?access_token=${SALLYFELLTOKEN}&fields=id,caption,media_url,permalink,media_type,timestamp&limit=${pageLimit}`;
    const response = await axios.get(url);
    const feedData = response.data;

    saveInstagramFeedData(feedData);

    res.json(feedData);
  } catch (error) {
    console.error("Error fetching Instagram feed:", error.message);
    res.status(500).json({ error: "Failed to fetch Instagram feed" });
  }
});

// 인스타 토큰 조회 API
app.get('/api/instagramToken', (req, res) => {
  const token = process.env.INSTAGRAM_TOKEN;
  if (token) {
    res.json({ token });
  } else {
    res.status(500).json({ error: 'INSTAGRAM_TOKEN is not set.' });
  }
});

app.get('/api/sallyfeelToken', (req, res) => {
  const token = process.env.SALLYFELLTOKEN;
  if (token) {
    res.json({ token });
  } else {
    res.status(500).json({ error: 'SALLYFELLTOKEN is not set.' });
  }
});

// 인스타 피드 저장 함수
async function saveInstagramFeedData(feedData) {
  try {
    const instagramCollection = db.collection('instagramData');
    const feedItems = feedData.data || [];
    for (const item of feedItems) {
      await instagramCollection.updateOne(
        { id: item.id },
        { $set: item },
        { upsert: true }
      );
    }
    console.log("Instagram feed data saved to DB successfully.");
  } catch (err) {
    console.error("Error saving Instagram feed data to DB:", err);
  }
}

// 인스타 클릭 추적
app.post('/api/trackClick', async (req, res) => {
  const { postId } = req.body;
  if (!postId) {
    return res.status(400).json({ error: 'postId 값이 필요합니다.' });
  }
  try {
    const collection = db.collection('instaClickdata');
    await collection.updateOne(
      { postId: postId },
      { $inc: { counter: 1 } },
      { upsert: true }
    );
    res.status(200).json({ message: 'Click tracked successfully', postId });
  } catch (error) {
    console.error("Error tracking click event:", error);
    res.status(500).json({ error: 'Error tracking click event' });
  }
});

// 인스타 클릭수 조회
app.get('/api/getClickCount', async (req, res) => {
  const postId = req.query.postId;
  if (!postId) {
    return res.status(400).json({ error: 'postId query parameter is required' });
  }
  try {
    const collection = db.collection('instaClickdata');
    const doc = await collection.findOne({ postId: postId });
    const clickCount = doc && doc.counter ? doc.counter : 0;

    res.status(200).json({ clickCount });
  } catch (error) {
    console.error("Error fetching click count:", error);
    res.status(500).json({ error: 'Error fetching click count' });
  }
});







// ==========================================
// [5] Cafe24 API (상품 & 옵션 조회) 온라인팀 매출분석 자료 활용 코드 
// ==========================================
// ==========================================
// [수정] Cafe24 카테고리(분류) 목록 조회 API (전체 가져오기)
// ==========================================
app.get('/api/cafe24/categories', async (req, res) => {
  try {
    let allCategories = [];
    let offset = 0;
    const limit = 100; // 카페24가 허용하는 최대 개수
    let hasMore = true;
    let loopCount = 0;

    // 데이터가 안 끊기고 다 나올 때까지 반복해서 긁어옵니다 (무한루프 방지: 최대 10번 = 1000개)
    while (hasMore && loopCount < 10) {
      const fetchFromCafe24 = async (retry = false) => {
        try {
          return await axios.get(
            `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/categories`,
            {
              params: {
                shop_no: 1,
                limit: limit,
                offset: offset
              },
              headers: {
                Authorization: `Bearer ${accessToken}`,
                'Content-Type': 'application/json',
                'X-Cafe24-Api-Version': CAFE24_API_VERSION
              }
            }
          );
        } catch (err) {
          if (err.response && err.response.status === 401 && !retry) {
            await refreshAccessToken();
            return await fetchFromCafe24(true);
          }
          throw err;
        }
      };

      const response = await fetchFromCafe24();
      const cats = response.data.categories || [];
      allCategories = allCategories.concat(cats);

      // 불러온 개수가 100개보다 적으면 뒤에 더 이상 데이터가 없는 것이므로 종료
      if (cats.length < limit) {
        hasMore = false;
      } else {
        offset += limit; // 다음 100개를 가져오기 위해 오프셋 증가
      }
      loopCount++;
    }

    res.json({ success: true, data: allCategories });
  } catch (error) {
    console.error("🔥 Cafe24 카테고리 목록 조회 에러:", error.message);
    res.status(500).json({ success: false, message: "Cafe24 API Error" });
  }
});
// ==========================================
// [추가] 특정 카테고리의 상품 목록 조회 API
// ==========================================
app.get('/api/cafe24/categories/:categoryNo/products', async (req, res) => {
  try {
    const { categoryNo } = req.params;
    const fetchFromCafe24 = async (retry = false) => {
      try {
        return await axios.get(
          `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/products`,
          {
            params: {
              shop_no: 1,
              category: categoryNo,
              display: 'T',
              selling: 'T',
              embed: 'options,images',
              limit: 100
            },
            headers: {
              Authorization: `Bearer ${accessToken}`,
              'Content-Type': 'application/json',
              'X-Cafe24-Api-Version': CAFE24_API_VERSION
            }
          }
        );
      } catch (err) {
        if (err.response && err.response.status === 401 && !retry) {
          await refreshAccessToken();
          return await fetchFromCafe24(true);
        }
        throw err;
      }
    };

    const response = await fetchFromCafe24();
    res.json({ success: true, data: response.data.products });
  } catch (error) {
    console.error("🔥 Cafe24 카테고리별 상품 조회 에러:", error.message);
    res.status(500).json({ success: false, message: "Cafe24 API Error" });
  }
});

app.get('/api/cafe24/products', async (req, res) => {
  try {
    const { keyword } = req.query;
    if (!keyword) return res.json({ success: true, count: 0, data: [] });

    const fetchFromCafe24 = async (retry = false) => {
      try {
        return await axios.get(
          `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/products`,
          {
            params: { shop_no: 1, product_name: keyword, display: 'T', selling: 'T', embed: 'options,images', limit: 100, sort: 'created_date', order: 'asc' },
            headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json', 'X-Cafe24-Api-Version': CAFE24_API_VERSION }
          }
        );
      } catch (err) {
        if (err.response && err.response.status === 401 && !retry) {
          await refreshAccessToken();
          return await fetchFromCafe24(true);
        }
        throw err;
      }
    };

    const response = await fetchFromCafe24();
    const products = response.data.products || [];
    const cleanData = products.map(item => {
      let myOptions = [];
      let rawOptionList = item.options ? (Array.isArray(item.options) ? item.options : item.options.options) : [];

      if (rawOptionList.length > 0) {
        let targetOption = rawOptionList.find(opt => {
          const name = (opt.option_name || opt.name || "").toLowerCase();
          return name.includes('색상') || name.includes('color');
        }) || rawOptionList[0];
        if (targetOption && targetOption.option_value) {
          myOptions = targetOption.option_value.map(val => ({
            option_code: val.value_no || val.value_code || val.value,
            option_name: val.value_name || val.option_text || val.name
          }));
        }
      }
      let img = item.detail_image || item.list_image || item.small_image || (item.images && item.images[0] && item.images[0].big);
      return {
        product_no: item.product_no, product_name: item.product_name,
        price: Math.floor(Number(item.price)), options: myOptions,
        detail_image: img
      };
    });
    res.json({ success: true, count: cleanData.length, data: cleanData });
  } catch (error) { res.status(500).json({ success: false, message: "Cafe24 API Error" }); }
});

app.get('/api/cafe24/products/:productNo/options', async (req, res) => {
  try {
    const { productNo } = req.params;
    const fetchFromCafe24 = async (retry = false) => {
      try {
        return await axios.get(
          `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/products/${productNo}`,
          { params: { shop_no: 1, embed: 'options' }, headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json', 'X-Cafe24-Api-Version': CAFE24_API_VERSION } }
        );
      } catch (err) {
        if (err.response && err.response.status === 401 && !retry) { await refreshAccessToken(); return await fetchFromCafe24(true); }
        throw err;
      }
    };
    const response = await fetchFromCafe24();
    const product = response.data.product;
    let myOptions = [];
    let rawOptionList = Array.isArray(product.options) ? product.options : (product.options && product.options.options ? product.options.options : []);

    if (rawOptionList.length > 0) {
      let targetOption = rawOptionList.find(opt => {
        const name = (opt.option_name || opt.name || "").toLowerCase();
        return name.includes('색상') || name.includes('color');
      }) || rawOptionList[0];
      if (targetOption && targetOption.option_value) {
        myOptions = targetOption.option_value.map(val => ({
          option_code: val.value_no || val.value_code || val.value,
          option_name: val.value_name || val.option_text || val.name
        }));
      }
    }
    res.json({ success: true, product_no: product.product_no, product_name: product.product_name, options: myOptions });
  } catch (error) { res.status(500).json({ success: false, message: "Cafe24 API Error" }); }
});

// ==========================================
// [5-2] Cafe24 쿠폰 및 자동 매핑 API
// ==========================================
app.get('/api/cafe24/coupons', async (req, res) => {
  try {
    const fetchFromCafe24 = async (url, params, retry = false) => {
      try {
        return await axios.get(url, {
          params,
          headers: {
            Authorization: `Bearer ${accessToken}`,
            'Content-Type': 'application/json',
            'X-Cafe24-Api-Version': CAFE24_API_VERSION
          }
        });
      } catch (err) {
        if (err.response && err.response.status === 401 && !retry) {
          await refreshAccessToken();
          return await fetchFromCafe24(url, params, true);
        }
        throw err;
      }
    };

    const listRes = await fetchFromCafe24(
      `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/coupons`,
      { shop_no: 1, limit: 100, issue_type: 'D' }
    );
    const coupons = listRes.data.coupons || [];

    const now = new Date();
    const activeCoupons = coupons.filter(c => {
      if (c.deleted === 'T') return false;
      if (c.is_stopped_issued_coupon === 'T') return false;
      if (c.issue_type !== 'D') return false;
      if (c.issue_start_date && new Date(c.issue_start_date) > now) return false;
      if (c.issue_end_date && new Date(c.issue_end_date) < now) return false;
      if (c.available_period_type === 'F') {
        if (c.available_start_datetime && new Date(c.available_start_datetime) > now) return false;
        if (c.available_end_datetime && new Date(c.available_end_datetime) < now) return false;
      }
      return true;
    });

    const detailResults = await Promise.allSettled(
      activeCoupons.map(c => fetchFromCafe24(`https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/coupons/${c.coupon_no}`, { shop_no: 1 }))
    );

    const enriched = activeCoupons.map((c, idx) => {
      let availableProducts = [];
      let availableProductType = c.available_product_type || 'A';
      const detail = detailResults[idx];

      if (detail.status === 'fulfilled' && detail.value.data.coupon) {
        const dc = detail.value.data.coupon;
        availableProductType = dc.available_product_type || availableProductType;
        const raw = dc.available_product;
        if (Array.isArray(raw)) {
          availableProducts = raw.map(p => (typeof p === 'object' && p !== null && p.product_no) ? Number(p.product_no) : Number(p)).filter(n => !isNaN(n));
        } else if (typeof raw === 'number') {
          availableProducts = [raw];
        } else if (typeof raw === 'string' && raw) {
          availableProducts = raw.split(',').map(s => Number(s.trim())).filter(n => !isNaN(n));
        }
      }

      return {
        coupon_no: c.coupon_no,
        coupon_name: c.coupon_name,
        benefit_type: c.benefit_type,
        benefit_percentage: c.benefit_percentage ? parseFloat(c.benefit_percentage) : null,
        benefit_price: c.benefit_price ? Math.floor(parseFloat(c.benefit_price)) : null,
        available_date: c.available_date || '',
        available_product_type: availableProductType,
        available_product: availableProducts
      };
    });

    res.json({ success: true, count: enriched.length, data: enriched });
  } catch (error) { res.status(500).json({ success: false, message: 'Cafe24 Coupon API Error' }); }
});

app.get('/api/cafe24/coupons/:couponNo', async (req, res) => {
  try {
    const { couponNo } = req.params;
    const fetchFromCafe24 = async (url, params, retry = false) => {
      try {
        return await axios.get(url, {
          params,
          headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json', 'X-Cafe24-Api-Version': CAFE24_API_VERSION }
        });
      } catch (err) {
        if (err.response && err.response.status === 401 && !retry) {
          await refreshAccessToken(); return await fetchFromCafe24(url, params, true);
        }
        throw err;
      }
    };

    const couponRes = await fetchFromCafe24(`https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/coupons`, { shop_no: 1, coupon_no: couponNo });
    const coupon = (couponRes.data.coupons || [])[0];
    if (!coupon) return res.status(404).json({ success: false, message: '쿠폰 없음' });

    const productNos = coupon.available_product_list || [];
    let productDetails = [];

    if (productNos.length > 0) {
      try {
        const chunkSize = 100;
        for (let i = 0; i < productNos.length; i += chunkSize) {
          const chunk = productNos.slice(i, i + chunkSize);
          const productRes = await fetchFromCafe24(
            `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/products`,
            { shop_no: 1, product_no: chunk.join(','), fields: 'product_no,product_name,price,detail_image,list_image,small_image', limit: 100 }
          );
          const chunkDetails = (productRes.data.products || []).map(p => ({
            product_no: p.product_no, product_name: p.product_name, price: Math.floor(Number(p.price)), image: p.detail_image || p.list_image || p.small_image || ''
          }));
          productDetails = productDetails.concat(chunkDetails);
        }
      } catch (e) {
        productDetails = productNos.map(no => ({ product_no: no, product_name: `상품 #${no}`, price: 0, image: '' }));
      }
    }

    res.json({
      success: true,
      data: {
        coupon_no: coupon.coupon_no, coupon_name: coupon.coupon_name, benefit_type: coupon.benefit_type,
        benefit_percentage: coupon.benefit_percentage ? parseFloat(coupon.benefit_percentage) : null,
        benefit_price: coupon.benefit_price ? Math.floor(parseFloat(coupon.benefit_price)) : null,
        available_product_type: coupon.available_product || 'A', available_product_list: productNos, products: productDetails
      }
    });
  } catch (error) { res.status(500).json({ success: false, message: 'Cafe24 Coupon API Error' }); }
});

app.post('/api/coupon-map', async (req, res) => {
  try {
    const { coupon_no, coupon_name, benefit_type, benefit_percentage, benefit_price, start_date, end_date, products } = req.body;
    if (!coupon_no) return res.status(400).json({ success: false });

    await db.collection(COLLECTION_COUPON_MAP).updateOne(
      { coupon_no: String(coupon_no) },
      { $set: { coupon_no: String(coupon_no), coupon_name, benefit_type, benefit_percentage, benefit_price, start_date, end_date, products, updated_at: new Date() } },
      { upsert: true }
    );
    res.json({ success: true });
  } catch (e) { res.status(500).json({ success: false }); }
});

app.get('/api/coupon-map', async (req, res) => {
  try {
    const mappings = await db.collection(COLLECTION_COUPON_MAP).find({}).toArray();
    const today = new Date().toISOString().slice(0, 10);
    const active = mappings.filter(m => !m.end_date || m.end_date >= today);
    res.json({ success: true, data: active });
  } catch (e) { res.status(500).json({ success: false }); }
});

app.delete('/api/coupon-map/:couponNo', async (req, res) => {
  try {
    await db.collection(COLLECTION_COUPON_MAP).deleteOne({ coupon_no: String(req.params.couponNo) });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ success: false }); }
});



// ==========온라인 목표 매출 통계관련 데이터 ==========

// ==========================================
// [업데이트] 자사몰 통계 - 일별 데이터 + 합계
// ==========================================
app.get('/api/online/homepage-stats', async (req, res) => {
  try {
    const { startDate, endDate } = req.query;

    if (!startDate || !endDate) {
      return res.status(400).json({ success: false, message: '시작일과 종료일 정보가 필요합니다.' });
    }

    const fetchFromCafe24 = async (url, params, retry = false) => {
      try {
        return await axios.get(url, {
          params,
          headers: {
            Authorization: `Bearer ${accessToken}`,
            'Content-Type': 'application/json',
            'X-Cafe24-Api-Version': CAFE24_API_VERSION
          }
        });
      } catch (err) {
        if (err.response && err.response.status === 401 && !retry) {
          await refreshAccessToken();
          return await fetchFromCafe24(url, params, true);
        }
        throw err;
      }
    };

    // 날짜 범위 생성
    const getDatesInRange = (start, end) => {
      const dates = [];
      let current = new Date(start);
      const endDate = new Date(end);
      while (current <= endDate) {
        dates.push(current.toISOString().split('T')[0]);
        current.setDate(current.getDate() + 1);
      }
      return dates;
    };

    const dateRange = getDatesInRange(startDate, endDate);

    // ── 1. 방문자 데이터 (일별) ──
    const getVisitorsByDate = async (sDate, eDate) => {
      const visitorMap = {};

      try {
        const visitorRes = await axios.get(
          `https://ca-api.cafe24data.com/visitors/view`,
          {
            params: {
              mall_id: 'yogibo',
              shop_no: 1,
              start_date: sDate,
              end_date: eDate
            },
            headers: {
              Authorization: `Bearer ${accessToken}`,
              'Content-Type': 'application/json'
            }
          }
        );

        const data = visitorRes.data;
        if (data.view && Array.isArray(data.view)) {
          data.view.forEach(v => {
            const date = v.date.split('T')[0];
            visitorMap[date] = {
              visitors: Number(v.visit_count || 0),
              firstVisit: Number(v.first_visit_count || 0),
              reVisit: Number(v.re_visit_count || 0)
            };
          });
        }
      } catch (err) {
        console.log('⚠️ 방문자 데이터 조회 실패:', err.message);
      }

      return visitorMap;
    };

    // ── 2. 주문 데이터 (일별 집계) ──
    const getOrdersByDate = async (sDate, eDate) => {
      const orderMap = {};
      const detailMap = {
        newMemberList: [],
        existMemberList: [],
        guestList: []
      };

      try {
        let orderHasMore = true;
        let orderOffset = 0;

        while (orderHasMore && orderOffset < 5000) {
          const orderRes = await fetchFromCafe24(
            `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/orders`,
            {
              shop_no: 1,
              start_date: sDate,
              end_date: eDate,
              date_type: 'pay_date',
              limit: 100,
              offset: orderOffset,
              embed: 'items'
            }
          );

          const orders = orderRes.data.orders || [];

          orders
            .filter(o => !['C', 'R', 'E'].includes(o.order_status))
            .forEach(o => {
              const date = o.payment_date ? o.payment_date.split('T')[0] : o.order_date.split('T')[0];

              if (!orderMap[date]) {
                orderMap[date] = {
                  totalAmt: 0,
                  ordCount: 0,
                  newMemberBuy: 0,
                  existMemberBuy: 0,
                  guestBuy: 0
                };
              }

              const amt = Number(o.payment_amount) || 0;
              orderMap[date].totalAmt += amt;
              orderMap[date].ordCount++;

              // 상세 정보
              const orderDetail = {
                orderNo: o.order_id,
                orderDate: o.order_date,
                buyerName: o.billing_name || '비공개',
                buyerEmail: o.member_email || '-',
                memberId: o.member_id || null,
                paymentAmount: amt,
                items: (o.items || []).map(item => ({
                  productName: item.product_name,
                  quantity: item.quantity,
                  price: Number(item.product_price) || 0,
                  optionValue: item.option_value || ''
                }))
              };

              if (o.member_id) {
                if (o.first_order === 'T') {
                  orderMap[date].newMemberBuy++;
                  detailMap.newMemberList.push({ ...orderDetail, date });
                } else {
                  orderMap[date].existMemberBuy++;
                  detailMap.existMemberList.push({ ...orderDetail, date });
                }
              } else {
                orderMap[date].guestBuy++;
                detailMap.guestList.push({ ...orderDetail, date });
              }
            });

          if (orders.length < 100) orderHasMore = false;
          else orderOffset += 100;
        }

      } catch (err) {
        console.log(`⚠️ 주문 정보 가져오기 실패:`, err.message);
      }

      return { orderMap, detailMap };
    };

    // 병렬 조회
    const [visitorMap, orderData] = await Promise.all([
      getVisitorsByDate(startDate, endDate),
      getOrdersByDate(startDate, endDate)
    ]);

    const { orderMap, detailMap } = orderData;

    // 일별 데이터 조합
    const dailyData = dateRange.map(date => {
      const visitor = visitorMap[date] || { visitors: 0, firstVisit: 0, reVisit: 0 };
      const order = orderMap[date] || { totalAmt: 0, ordCount: 0, newMemberBuy: 0, existMemberBuy: 0, guestBuy: 0 };

      const purchaseRate = visitor.visitors > 0
        ? ((order.ordCount / visitor.visitors) * 100).toFixed(2)
        : '0.00';
      const visitAvg = visitor.visitors > 0 ? Math.floor(order.totalAmt / visitor.visitors) : 0;
      const orderAvg = order.ordCount > 0 ? Math.floor(order.totalAmt / order.ordCount) : 0;

      return {
        date,
        ...visitor,
        ...order,
        purchaseRate,
        visitAvg,
        orderAvg
      };
    });

    // 합계 계산
    const totals = dailyData.reduce((acc, d) => {
      acc.visitors += d.visitors;
      acc.firstVisit += d.firstVisit;
      acc.reVisit += d.reVisit;
      acc.totalAmt += d.totalAmt;
      acc.ordCount += d.ordCount;
      acc.newMemberBuy += d.newMemberBuy;
      acc.existMemberBuy += d.existMemberBuy;
      acc.guestBuy += d.guestBuy;
      return acc;
    }, {
      visitors: 0, firstVisit: 0, reVisit: 0,
      totalAmt: 0, ordCount: 0,
      newMemberBuy: 0, existMemberBuy: 0, guestBuy: 0
    });

    totals.purchaseRate = totals.visitors > 0
      ? ((totals.ordCount / totals.visitors) * 100).toFixed(2)
      : '0.00';
    totals.visitAvg = totals.visitors > 0 ? Math.floor(totals.totalAmt / totals.visitors) : 0;
    totals.orderAvg = totals.ordCount > 0 ? Math.floor(totals.totalAmt / totals.ordCount) : 0;

    console.log(`✅ ${startDate}~${endDate} 일별 데이터 ${dailyData.length}일, 총 주문 ${totals.ordCount}건`);

    res.json({
      success: true,
      startDate,
      endDate,
      daily: dailyData,
      totals,
      details: detailMap
    });

  } catch (error) {
    console.error("🔥 홈페이지 통계 API 에러:", error);
    res.status(500).json({ success: false, error: error.message });
  }
});




// ==========================================
// [추가] 요기보 나에게 꼭 맞는 찾기 TEST API
// ==========================================

// 1. 테스트 결과 저장 (프론트엔드에서 '결과 확인하기' 클릭 시 호출)
app.post('/api/yogibo/test-result', async (req, res) => {
  try {
    const { visitorId, answers, journey, recommendedName, recommendedDesc } = req.body;

    // 한국 시간(KST)으로 저장
    const createdAtKST = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));

    const newResult = {
      visitorId: visitorId || 'guest_unknown',
      answers: answers || {},
      journey: journey || [],
      recommendedName: recommendedName || '',
      recommendedDesc: recommendedDesc || '',
      createdAt: createdAtKST
    };

    // 'yogibo_test_results' 컬렉션에 데이터 저장
    const result = await db.collection('yogibo_test_results').insertOne(newResult);

    console.log(`[Yogibo Test] 테스트 결과 저장 완료: ${visitorId} -> 추천: ${recommendedName}`);
    res.json({ success: true, insertedId: result.insertedId });
  } catch (error) {
    console.error('[Yogibo Test] 결과 저장 오류:', error);
    res.status(500).json({ success: false, message: 'Server Internal Error' });
  }
});




// 2. 관리자용: 테스트 결과 엑셀 다운로드
app.get('/api/yogibo/test-result/download', async (req, res) => {
  try {
    const results = await db.collection('yogibo_test_results')
      .find({})
      .sort({ createdAt: -1 })
      .toArray();

    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Test Results');

    // 엑셀 컬럼 정의
    worksheet.columns = [
      { header: '참여 일시', key: 'createdAt', width: 25 },
      { header: '방문자 ID', key: 'visitorId', width: 25 },
      { header: '최종 추천 제품', key: 'recommendedName', width: 35 },
      { header: '추천 상세/충전재', key: 'recommendedDesc', width: 30 },
      { header: 'Q1 (관심제품)', key: 'q1', width: 15 },
      { header: 'Q2 (사용자)', key: 'q2', width: 15 },
      { header: 'Q3 (사용장소)', key: 'q3', width: 15 },
      { header: 'Q4 (중요가치)', key: 'q4', width: 15 },
      { header: 'Q5 (사용용도)', key: 'q5', width: 15 },
      { header: '여정 (클릭 순서)', key: 'journey', width: 30 }
    ];

    results.forEach(item => {
      // 시간 포맷팅
      const fmtDate = item.createdAt
        ? moment(item.createdAt).tz('Asia/Seoul').format('YYYY-MM-DD HH:mm:ss')
        : '-';

      worksheet.addRow({
        createdAt: fmtDate,
        visitorId: item.visitorId,
        recommendedName: item.recommendedName,
        recommendedDesc: item.recommendedDesc,
        q1: item.answers?.q1 || '-',
        q2: item.answers?.q2 || '-',
        q3: item.answers?.q3 || '-',
        q4: item.answers?.q4 || '-',
        q5: item.answers?.q5 || '-',
        journey: (item.journey || []).join(' > ') // 예: q1 > q5 > result
      });
    });

    // 헤더 및 파일명 설정 후 응답
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename=yogibo_test_results.xlsx');

    await workbook.xlsx.write(res);
    res.end();
  } catch (error) {
    console.error('[Yogibo Test] 엑셀 다운로드 오류:', error);
    res.status(500).json({ error: 'Excel 다운로드 중 오류 발생' });
  }
});


// =================================================================
// 🇯🇵 요기보 일본 뉴스레터 자동화 모듈 (스케줄러 & API)
// =================================================================

// ========== [추가] 서버 수면(Sleep) 방지용 Ping 라우터 ==========
app.get('/api/ping', (req, res) => {
  res.status(200).send('pong');
});

// ========== [추가] 노드 서버 메모리 캐시 변수 ==========
let newsCache = null;
let newsCacheTime = 0;
const NEWS_CACHE_TTL = 5 * 60 * 1000; // 5분 유지
// 3. API - 뉴스레터 목록 불러오기 (페이징 및 서버 필터링 적용)
app.get('/api/yogibo-jp-news', async (req, res) => {
  try {
    const { status, source, limit, offset } = req.query;
    const query = {};

    // 탭 필터링 조건 설정
    if (status && status !== 'all') query.status = status;
    if (source === 'rss') query.source = { $ne: 'manual' };
    if (source === 'manual') query.source = 'manual';

    const collection = db.collection('yogiboJPnews');

    // 현재 탭에 해당하는 전체 게시물 수
    const totalCount = await collection.countDocuments(query);

    // 데이터 조회 (limit과 offset으로 잘라서 가져오기)
    let cursor = collection.find(query).sort({ position: 1, pubDate: -1 });
    if (offset) cursor = cursor.skip(Number(offset));
    if (limit) cursor = cursor.limit(Number(limit));
    const newsList = await cursor.toArray();

    // 관리자 화면 좌측 뱃지를 위한 전체 요약 카운트
    const counts = {
      all: await collection.countDocuments(),
      original: await collection.countDocuments({ status: 'draft' }),     // 원본 (draft)
      pending: await collection.countDocuments({ status: 'pending' }),    // 게시물대기 (pending)
      published: await collection.countDocuments({ status: 'published' }),// 라이브 (published)
      rss: await collection.countDocuments({ source: { $ne: 'manual' } }),
      manual: await collection.countDocuments({ source: 'manual' })
    };

    res.json({ success: true, data: newsList, totalCount, counts });
  } catch (error) {
    console.error('뉴스레터 조회 에러:', error);
    res.status(500).json({ success: false, message: 'Server Error' });
  }
});

// 1. 피드 파싱 및 DB 자동 저장 로직 (Upsert 방식 - 중복 방지 완벽 적용)
async function fetchAndSaveYogiboJPNews() {
  if (!db) {
    console.log('⚠️ DB 연결이 안 되어있어 뉴스레터 동기화를 보류합니다.');
    return;
  }

  try {
    console.log('🇯🇵 [Yogibo JP] 일본 뉴스레터 정기 업데이트 확인 중...');

    const feed = await parser.parseURL('https://yogibo.jp/blogs/life.atom');
    const collection = db.collection('yogiboJPnews');
    let newCount = 0;

    for (const item of feed.items) {
      // URL 뒤에 붙는 ? 쿼리파라미터나 / 슬래시를 제거하여 순수 주소만 추출 (중복 수집 1차 방어)
      let cleanLink = item.link || '';
      if (cleanLink.includes('?')) cleanLink = cleanLink.split('?')[0];
      if (cleanLink.endsWith('/')) cleanLink = cleanLink.slice(0, -1);

      // 제목이 완전히 똑같은 글이 있는지 먼저 확인 (중복 수집 2차 방어)
      const existingPost = await collection.findOne({ title: item.title });

      if (!existingPost) {
        // DB에 같은 제목이 없을 때만 새로 추가
        const result = await collection.updateOne(
          { guid: cleanLink }, // 기준 키
          {
            $setOnInsert: {
              guid: cleanLink,
              title: item.title,
              content: item.content,
              link: cleanLink,
              pubDate: item.pubDate ? new Date(item.pubDate) : new Date(),
              status: 'draft', // 무조건 '임시저장' 상태로 대기
              createdAt: new Date()
            }
          },
          { upsert: true }
        );

        if (result.upsertedId) {
          newCount++;
          console.log(`🆕 새 뉴스레터 발견 및 임시저장: ${item.title}`);
        }
      }
    }

    if (newCount === 0) {
      console.log('✅ [Yogibo JP] 새로운 뉴스레터가 없습니다. (기존 데이터 유지)');
    } else {
      console.log(`✅ [Yogibo JP] 총 ${newCount}개의 새 뉴스레터 동기화 완료`);
    }

  } catch (error) {
    console.error('❌ [Yogibo JP] 뉴스레터 가져오기 실패:', error.message);
  }
}

// 2. 스케줄러 등록 (매일 자정 12시에 1번만 실행되도록 설정)
// 크론 표현식 '0 0 * * *' = 매일 00:00 (24시간마다 1번)
cron.schedule('0 */6 * * *', () => {
  fetchAndSaveYogiboJPNews();
});

// [추가] 순서 변경 저장 API (드래그 앤 드롭)
app.put('/api/yogibo-jp-news/order', async (req, res) => {
  try {
    const { order } = req.body;
    // order 형태 예시: [{ id: '...', position: 100 }, { id: '...', position: 200 }]

    if (!order || !Array.isArray(order)) {
      return res.status(400).json({ success: false, message: '잘못된 데이터 형식입니다.' });
    }

    const collection = db.collection('yogiboJPnews');

    // bulkWrite를 사용할 때 하드코딩된 ID 예외 처리
    const bulkOps = order.map(item => {
      let queryId;

      if (item.id === 'hardcoded_recovery') {
        queryId = item.id; // 하드코딩된 일반 문자열은 그대로 통과
      } else if (ObjectId.isValid(item.id)) {
        queryId = new ObjectId(item.id); // 정상적인 24자리 Hex는 변환
      } else {
        return null; // 알 수 없는 쓰레기값이면 건너뛰기
      }

      return {
        updateOne: {
          filter: { _id: queryId },
          update: { $set: { position: item.position } }
        }
      };
    }).filter(op => op !== null); // 위에서 null로 반환된 쓰레기값들은 배열에서 제거

    if (bulkOps.length > 0) {
      await collection.bulkWrite(bulkOps);
    }

    res.json({ success: true, message: '순서가 성공적으로 업데이트되었습니다.' });
  } catch (error) {
    console.error('순서 업데이트 에러:', error);
    res.status(500).json({ success: false, message: '서버 오류가 발생했습니다.' });
  }
});

// 4. API - 뉴스레터 내용 수정 및 라이브 상태 변경 (관리자 페이지용)
app.put('/api/yogibo-jp-news/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { title, content, status } = req.body;

    // ★ [수정] 하드코딩 ID 예외 처리 및 ObjectId 변환
    let queryId;
    if (id === 'hardcoded_recovery') {
      queryId = id; // 일반 문자열 그대로 사용
    } else if (ObjectId.isValid(id)) {
      queryId = new ObjectId(id);
    } else {
      console.error(`[뉴스레터 업데이트] 유효하지 않은 ID 값 수신: ${id}`);
      return res.status(400).json({ success: false, message: '잘못된 게시글 ID 형식입니다.' });
    }

    const updateData = {};
    if (title) updateData.title = title;
    if (content) updateData.content = content;
    if (status) updateData.status = status;
    updateData.updatedAt = new Date();

    const result = await db.collection('yogiboJPnews').updateOne(
      { _id: queryId },
      { $set: updateData },
      { upsert: id === 'hardcoded_recovery' } // 하드코딩 데이터가 DB에 아예 없었다면 새로 생성(upsert)
    );

    res.json({ success: true, message: '게시글이 성공적으로 업데이트되었습니다.' });
  } catch (error) {
    console.error('뉴스레터 업데이트 에러:', error);
    res.status(500).json({ success: false, message: 'Server Error' });
  }
});


// 5. [수동 테스트용] 즉시 동기화 API (스케줄러 기다리기 답답할 때 호출)
app.get('/api/test/fetch-jp-news', async (req, res) => {
  await fetchAndSaveYogiboJPNews();
  res.json({ success: true, message: '수동 피드 동기화가 실행되었습니다. 서버 로그를 확인하세요.' });
});


// 6. [1회성 청소용] 중복 데이터 자동 삭제 API
// 동일한 제목이 2개 이상이면 제일 처음 저장된 1개만 남기고 나머지는 자동 삭제합니다.
app.get('/api/test/cleanup-duplicates', async (req, res) => {
  try {
    const collection = db.collection('yogiboJPnews');

    const duplicates = await collection.aggregate([
      { $group: { _id: "$title", count: { $sum: 1 }, docs: { $push: "$_id" } } },
      { $match: { count: { $gt: 1 } } }
    ]).toArray();

    let deletedCount = 0;

    for (const dup of duplicates) {
      const toDeleteIds = dup.docs.slice(1);
      const result = await collection.deleteMany({ _id: { $in: toDeleteIds } });
      deletedCount += result.deletedCount;
    }

    res.json({ success: true, message: `청소 완료! 총 ${deletedCount}개의 중복 게시글이 삭제되었습니다.` });
  } catch (error) {
    console.error('청소 에러:', error);
    res.status(500).json({ success: false, message: error.message });
  }
});


// 7. API - 구글 무료 번역 (이미지 및 CSS 스타일 태그 파괴 방지, 증발 버그 수정)
app.post('/api/translate-news', async (req, res) => {
  const { title, content } = req.body;

  try {
    const { translate: googleTranslate } = await import('@vitalets/google-translate-api');
    console.log('🔄 구글 번역기로 한글 초벌 번역 중...');

    // 제목 번역
    let translatedTitle = title;
    if (title) {
      const titleResult = await googleTranslate(title, { from: 'ja', to: 'ko' });
      translatedTitle = titleResult.text;
    }

    let translatedHtml = content || '';

    // 본문이 있을 경우에만 번역 진행
    if (content) {
      const protectedTags = [];
      let maskedContent = content;

      // PROTECTED_TAG 같은 영단어 대신, 번역기가 무시하는 단순 기호(__TG번호__) 사용
      maskedContent = maskedContent.replace(/<style[^>]*>[\s\S]*?<\/style>/gi, (match) => {
        protectedTags.push(match);
        return `__TG${protectedTags.length - 1}__`;
      });

      maskedContent = maskedContent.replace(/<script[^>]*>[\s\S]*?<\/script>/gi, (match) => {
        protectedTags.push(match);
        return `__TG${protectedTags.length - 1}__`;
      });

      maskedContent = maskedContent.replace(/<img[^>]*>/gi, (match) => {
        protectedTags.push(match);
        return `__TG${protectedTags.length - 1}__`;
      });

      // 텍스트 번역 수행
      const contentResult = await googleTranslate(maskedContent, { from: 'ja', to: 'ko' });

      // 번역 API가 알 수 없는 이유로 빈 값을 반환했을 경우 에러 처리 (화면 하얘짐 방지)
      if (!contentResult || !contentResult.text) {
        throw new Error("번역 API에서 빈 값을 반환했습니다.");
      }

      translatedHtml = contentResult.text;

      // 보호했던 태그 원상 복구
      translatedHtml = translatedHtml.replace(/__\s*TG\s*(\d+)\s*__/gi, (match, index) => {
        return protectedTags[parseInt(index)] || match;
      });
    }

    console.log('✅ 번역 완료 (보호 성공)!');

    res.json({
      success: true,
      translatedTitle: translatedTitle,
      translatedContent: translatedHtml
    });
  } catch (error) {
    console.error('❌ 번역 에러:', error);
    res.status(500).json({ success: false, message: '구글 번역 중 오류가 발생했습니다. 본문이 너무 길 수 있습니다.' });
  }
});
// 8. API - 썸네일 FTP 업로드 (경로 에러 수정 완료 + 하드코딩 ID 예외 처리)

app.post('/api/yogibo-jp-news/:id/thumbnail-upload', upload.single('file'), async (req, res) => {
  const postId = req.params.id;

  try {
    if (!req.file) return res.status(400).json({ success: false, message: '파일 없음' });

    // buffer 대신 path 사용
    const processedBuffer = await sharp(req.file.path)
      .resize(800, 500, { fit: 'cover', position: 'center' })
      .webp({ quality: 82 })
      .toBuffer();

    // 임시 파일 삭제 (매우 중요: 안 지우면 디스크 꽉 참)
    fs.unlinkSync(req.file.path);

    const randomHex = crypto.randomBytes(6).toString('hex');
    const filename = `news-${postId}-${Date.now()}-${randomHex}.webp`;

    const client = new ftp.Client();
    client.ftp.verbose = true;
    try {
      await client.access({
        host: process.env.FTP_HOST || 'yogibo.ftp.cafe24.com',
        port: process.env.FTP_PORT ? Number(process.env.FTP_PORT) : 21,
        user: process.env.FTP_USER,
        password: process.env.FTP_PASS,
        secure: 'explicit',
      });

      await client.ensureDir('web/img/news');
      console.log('디렉토리 생성 또는 확인 성공');

      const stream = Readable.from(processedBuffer);
      await client.uploadFrom(stream, filename);
      console.log('파일 업로드 성공:', filename);

    } finally {
      client.close();
    }

    const publicUrl = `https://yogibo.cafe24.com/web/img/news/${filename}`;

    // ★ [수정됨] 하드코딩 ID 판별 로직 추가
    let queryId;
    if (postId === 'hardcoded_recovery') {
      queryId = postId;
    } else if (ObjectId.isValid(postId)) {
      queryId = new ObjectId(postId);
    } else {
      return res.status(400).json({ success: false, message: '잘못된 게시글 ID 형식입니다.' });
    }

    // DB 업데이트
    const result = await db.collection('yogiboJPnews').updateOne(
      { _id: queryId },
      { $set: { thumbnail: publicUrl, thumbnailUpdatedAt: new Date() } },
      { upsert: postId === 'hardcoded_recovery' } // 하드코딩 데이터면 없을 때 생성
    );

    // upsert가 아닌 일반 수정일 때 매칭된 게 없으면 에러 처리
    if (result.matchedCount === 0 && postId !== 'hardcoded_recovery') {
      return res.status(404).json({ success: false, message: '게시글 없음' });
    }

    return res.json({ success: true, url: publicUrl, filename });

  } catch (err) {
    console.error('[FTP Upload Error]', err);
    return res.status(500).json({ success: false, message: err.message || '서버 오류' });
  }
});


//적립금 지급 이벤트 03월12일 3천원지급
// ========== [추가] 1회성 적립금 지급 이벤트 API ==========
app.post('/api/event/one-time-reward', async (req, res) => {
  const { memberId } = req.body;

  // 1. 파라미터 유효성 검사 (비회원 guest_ 필터링)
  if (!memberId || typeof memberId !== 'string' || memberId.startsWith('guest_')) {
    return res.status(400).json({ success: false, message: '로그인 후 참여 가능한 이벤트입니다.' });
  }

  const amount = 3000; // 지급할 적립금

  try {
    const collection = db.collection('event_onetime_rewards');

    // 2. 중복 참여 확인
    const alreadyParticipated = await collection.findOne({ memberId });
    if (alreadyParticipated) {
      return res.status(400).json({ success: false, message: '이미 적립 혜택을 받으셨습니다.' });
    }

    // 3. Cafe24 API로 포인트 적립
    const payload = {
      shop_no: 1,
      request: {
        member_id: memberId,
        order_id: null,
        amount: amount,
        type: 'increase',
        reason: '이벤트 3,000원 적립금 1회 지급'
      }
    };

    await apiRequest(
      'POST',
      `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/points`,
      payload
    );

    // 4. 적립 성공 시 참여 기록 저장 (KST 기준)
    const nowKST = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
    await collection.insertOne({
      memberId,
      amount,
      participatedAt: nowKST
    });

    return res.json({ success: true, message: '🎉 3,000원 적립금이 지급되었습니다!' });

  } catch (err) {
    console.error('포인트 지급 오류:', err);

    // Unique Index 충돌 에러 처리 (동시성 방어)
    if (err.code === 11000) {
      return res.status(400).json({ success: false, message: '이미 혜택을 받으셨습니다.' });
    }

    return res.status(500).json({ success: false, message: '서버 오류가 발생했습니다. 잠시 후 다시 시도해주세요.' });
  }
});
// API - 뉴스레터 조회수 증가
app.post('/api/yogibo-jp-news/:id/view', async (req, res) => {
  try {
    const { id } = req.params;

    // postId 대신 id를 사용하도록 수정
    let queryId = (id === 'hardcoded_recovery') ? id : new ObjectId(id);

    // 썸네일 관련 로직은 지우고, 깔끔하게 조회수(views)만 1 올립니다.
    await db.collection('yogiboJPnews').updateOne(
      { _id: queryId },
      { $inc: { views: 1 } },
      { upsert: true }
    );

    res.json({ success: true, message: '조회수 증가 완료' });
  } catch (error) {
    console.error('조회수 업데이트 에러:', error);
    res.status(500).json({ success: false, message: 'Server Error' });
  }
});


// =================================================================
// 🆕 뉴스레터 직접 작성 & AI 생성 기능 (axios 기반 - SDK 불필요)
// =================================================================
// ⚠️ 기존 server.js에서 아래 두 줄을 삭제하세요:
//   const Anthropic = require('@anthropic-ai/sdk');
//   const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
//
// 그리고 기존의 아래 3개 라우트를 삭제한 뒤, 이 파일 내용으로 교체하세요:
//   app.post('/api/yogibo-jp-news', ...)
//   app.post('/api/yogibo-jp-news/generate', ...)
//   app.delete('/api/yogibo-jp-news/:id', ...)
//
// ⚠️ 중요: generate 라우트가 반드시 POST '/api/yogibo-jp-news' 보다 위에 위치해야 합니다.
// .env 파일에 추가: ANTHROPIC_API_KEY=sk-ant-xxxxx
// =================================================================

// ─── AI 뉴스레터 생성 API (OpenAI GPT 사용) ────
app.post('/api/yogibo-jp-news/generate', async (req, res) => {
  try {
    const { prompt, images, style } = req.body;

    if (!prompt) {
      return res.status(400).json({ success: false, message: '프롬프트를 입력해주세요.' });
    }

    const API_KEY = process.env.API_KEY;
    if (!API_KEY) {
      return res.status(500).json({ success: false, message: 'API_KEY 환경변수가 설정되지 않았습니다.' });
    }

    // 스타일 가이드 매핑
    const styleGuides = {
      'product-launch': '신제품 출시 소식을 전하는 톤. 기대감과 혜택을 강조하며, 구매 전환을 유도하는 CTA를 포함.',
      'event-promo': '이벤트/프로모션 안내 톤. 긴급성과 혜택을 부각하며, 참여 방법을 명확히 안내.',
      'brand-story': '브랜드 스토리텔링 톤. 감성적이고 공감을 유도하며, 요기보의 라이프스타일 가치를 전달.',
      'collab': '콜라보/한정판 소식 톤. 희소성과 특별함을 강조하며, 팬심을 자극.',
      'tips-guide': '생활 팁/가이드 톤. 실용적이고 친근하며, 요기보 제품 활용법을 자연스럽게 연결.',
    };
    const selectedStyle = styleGuides[style] || '요기보 브랜드 톤에 맞는 따뜻하고 친근한 뉴스레터.';

    // GPT 메시지 구성
    const messages = [
      {
        role: 'system',
        content: `당신은 요기보(Yogibo) 한국 공식 뉴스레터 에디터입니다.

## 작성 규칙
1. 한국어로 작성합니다.
2. HTML 형식으로 출력합니다. (<html>, <head>, <body> 태그는 제외하고 본문 콘텐츠만)
3. 모든 스타일은 인라인 CSS로 작성합니다. (이메일 호환성)
4. 요기보 브랜드 컬러: 메인 레드(#E8001C), 네이비(#1e293b), 배경 밝은 회색(#f8f9fa)
5. 모바일 친화적인 단일 컬럼 레이아웃 (max-width: 600px, margin: 0 auto)
6. 이미지가 첨부된 경우, 해당 이미지를 분석하여 콘텐츠에 자연스럽게 반영합니다.

## 이미지 플레이스홀더 규칙 (매우 중요)
- 이미지가 들어갈 자리에는 반드시 텍스트로만 {{IMAGE_1}}, {{IMAGE_2}} 형태로 작성합니다.
- 절대로 <img> 태그로 감싸지 마세요. 텍스트 그대로 넣어야 합니다.
- 예시: <div>{{IMAGE_1}}</div> (O)
- 잘못된 예시: <img src="{{IMAGE_1}}"> (X)

## 디자인 가이드
- 최상위 래퍼: <div style="max-width:600px;margin:0 auto;font-family:'Pretendard',sans-serif;background:#ffffff;">
- 헤더 영역: 브랜드 컬러 배경, 흰색 텍스트, padding 30px
- 본문 텍스트: font-size:16px, line-height:1.8, color:#333333, padding:30px
- 소제목: font-size:20px, font-weight:700, color:#1e293b, margin-bottom:16px
- CTA 버튼: display:inline-block, background:#E8001C, color:#ffffff, padding:14px 32px, border-radius:8px, font-weight:700, text-decoration:none
- 구분선: <hr style="border:none;border-top:1px solid #e5e7eb;margin:30px 0;">
- 푸터: font-size:12px, color:#94a3b8, text-align:center, padding:20px 30px

## 스타일 가이드
${selectedStyle}

## 응답 형식
반드시 아래 JSON 형식만 반환하세요 (다른 텍스트 없이):
{"title": "뉴스레터 제목", "content": "<div style='max-width:600px;...'>...HTML 본문...</div>"}`
      }
    ];

    // 유저 메시지 구성 (이미지 포함 가능)
    const userContent = [];

    // 이미지가 있으면 GPT Vision으로 전달
    if (images && Array.isArray(images) && images.length > 0) {
      for (const img of images) {
        if (img.data) {
          userContent.push({
            type: 'image_url',
            image_url: {
              url: `data:${img.mediaType || 'image/jpeg'};base64,${img.data}`,
              detail: 'low'  // 비용 절약 (high로 바꾸면 더 정밀)
            }
          });
        }
      }
    }

    userContent.push({ type: 'text', text: prompt });
    messages.push({ role: 'user', content: userContent });

    console.log('🤖 AI 뉴스레터 생성 요청... (OpenAI GPT)');

    const apiResponse = await axios.post(
      'https://api.openai.com/v1/chat/completions',
      {
        model: 'gpt-4o',  // 이미지 분석 + 텍스트 생성 모두 가능
        messages: messages,
        max_tokens: 4096,
        temperature: 0.7,
      },
      {
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${API_KEY}`,
        },
        timeout: 120000,
      }
    );

    const responseText = apiResponse.data.choices?.[0]?.message?.content || '';

    // JSON 추출
    let parsed;
    try {
      const fenced = responseText.match(/```(?:json)?\s*([\s\S]*?)```/);
      const jsonStr = fenced ? fenced[1].trim() : responseText.trim();
      const jsonMatch = jsonStr.match(/\{[\s\S]*"title"[\s\S]*"content"[\s\S]*\}/);
      if (jsonMatch) {
        parsed = JSON.parse(jsonMatch[0]);
      } else {
        throw new Error('JSON 추출 실패');
      }
    } catch (parseErr) {
      console.warn('JSON 파싱 실패, 전체 텍스트 사용:', parseErr.message);
      parsed = { title: '새 뉴스레터', content: responseText };
    }

    console.log('✅ AI 뉴스레터 생성 완료:', parsed.title);
    res.json({ success: true, title: parsed.title, content: parsed.content });

  } catch (error) {
    console.error('❌ AI 생성 에러:', error.response?.data || error.message);
    const errMsg = error.response?.data?.error?.message || error.message || 'AI 생성 중 오류';
    res.status(500).json({ success: false, message: errMsg });
  }
});


// ─── 2. 새 게시글 생성 API (직접 작성 + AI 작성 공용) ───
app.post('/api/yogibo-jp-news', async (req, res) => {
  try {
    console.log('📥 [새 게시글] req.body:', JSON.stringify(req.body).substring(0, 200));
    const { title, content, status, thumbnail } = req.body;

    if (!title || !content) {
      return res.status(400).json({ success: false, message: '제목과 내용은 필수입니다.' });
    }

    const collection = db.collection('yogiboJPnews');

    const newPost = {
      title,
      content,
      status: status || 'draft',
      thumbnail: thumbnail || null,
      pubDate: new Date(),
      createdAt: new Date(),
      updatedAt: new Date(),
      views: 0,
      source: 'manual',
    };

    const result = await collection.insertOne(newPost);

    res.json({
      success: true,
      message: '새 게시글이 생성되었습니다.',
      data: { _id: result.insertedId, ...newPost }
    });

  } catch (error) {
    console.error('게시글 생성 에러:', error);
    res.status(500).json({ success: false, message: '서버 오류가 발생했습니다.' });
  }
});


// ─── 3. 게시글 삭제 API ─────────────────────────────────
app.delete('/api/yogibo-jp-news/:id', async (req, res) => {
  try {
    const { id } = req.params;

    let queryId;
    if (ObjectId.isValid(id)) {
      queryId = new ObjectId(id);
    } else {
      return res.status(400).json({ success: false, message: '잘못된 ID 형식입니다.' });
    }

    const collection = db.collection('yogiboJPnews');
    const result = await collection.deleteOne({ _id: queryId });

    if (result.deletedCount === 0) {
      return res.status(404).json({ success: false, message: '게시글을 찾을 수 없습니다.' });
    }

    res.json({ success: true, message: '게시글이 삭제되었습니다.' });

  } catch (error) {
    console.error('게시글 삭제 에러:', error);
    res.status(500).json({ success: false, message: '서버 오류가 발생했습니다.' });
  }
});


// ─── 뉴스레터 본문 이미지 FTP 업로드 ─────────────────
app.post('/api/yogibo-jp-news/upload-image', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ success: false, message: '파일 없음' });

    // buffer 대신 path 사용
    const processedBuffer = await sharp(req.file.path)
      .resize(800, null, { fit: 'inside', withoutEnlargement: true })
      .webp({ quality: 82 })
      .toBuffer();

    // 임시 파일 삭제
    fs.unlinkSync(req.file.path);

    const randomHex = crypto.randomBytes(6).toString('hex');
    const filename = `newsletter-${Date.now()}-${randomHex}.webp`;

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
      await client.ensureDir('web/img/news');
      const stream = Readable.from(processedBuffer);
      await client.uploadFrom(stream, filename);
      console.log('📸 뉴스레터 이미지 업로드 성공:', filename);
    } finally {
      client.close();
    }

    const publicUrl = `https://yogibo.cafe24.com/web/img/news/${filename}`;
    return res.json({ success: true, url: publicUrl });

  } catch (err) {
    console.error('[Image Upload Error]', err);
    return res.status(500).json({ success: false, message: err.message || '이미지 업로드 실패' });
  }
});


// =================================================================
// 📚 브랜드 지식베이스 관리 API
// =================================================================

// 카테고리: product-spec(제품), brand-story(브랜드), promotion(프로모션)

// 목록 조회
app.get('/api/brand-knowledge', async (req, res) => {
  try {
    const { category } = req.query;
    const query = category ? { category } : {};
    const docs = await db.collection('brandKnowledge')
      .find(query).sort({ updatedAt: -1 }).toArray();
    res.json({ success: true, data: docs });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

// 새 자료 등록
app.post('/api/brand-knowledge', async (req, res) => {
  try {
    const { title, category, content } = req.body;
    if (!title || !category || !content) {
      return res.status(400).json({ success: false, message: '제목, 카테고리, 내용은 필수입니다.' });
    }
    const doc = {
      title, category, content,
      createdAt: new Date(), updatedAt: new Date()
    };
    const result = await db.collection('brandKnowledge').insertOne(doc);
    res.json({ success: true, data: { _id: result.insertedId, ...doc } });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

// 수정
app.put('/api/brand-knowledge/:id', async (req, res) => {
  try {
    const { title, category, content } = req.body;
    await db.collection('brandKnowledge').updateOne(
      { _id: new ObjectId(req.params.id) },
      { $set: { title, category, content, updatedAt: new Date() } }
    );
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

// 삭제
app.delete('/api/brand-knowledge/:id', async (req, res) => {
  try {
    await db.collection('brandKnowledge').deleteOne({ _id: new ObjectId(req.params.id) });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

// 1. server.js 최상단 (모듈 불러오는 곳)에 아래 두 줄을 추가/수정해주세요.
// 기존 const pdfParse = require('pdf-parse'); 부분은 삭제합니다.

// 2. 문서 추출 라우터 교체
app.post('/api/brand-knowledge/extract', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ success: false, message: '파일 없음' });

    // 파일 타입 확인
    const isPDF = req.file.mimetype === 'application/pdf';
    let extractedText = '';

    // 디스크에 저장된 파일을 버퍼로 먼저 읽어오고 원본은 즉시 삭제
    const fileBuffer = fs.readFileSync(req.file.path);
    fs.unlinkSync(req.file.path);

    if (isPDF) {
      // 🌟 PDF → pdfjs-extract로 텍스트 추출 (안정적인 방식)
      let rawText = '';
      try {
        const data = await new Promise((resolve, reject) => {
          pdfExtract.extractBuffer(fileBuffer, {}, (err, data) => {
            if (err) reject(err);
            else resolve(data);
          });
        });

        // 각 페이지의 텍스트를 모아서 하나의 문자열로 결합
        if (data.pages && data.pages.length > 0) {
          data.pages.forEach(page => {
            if (page.content && page.content.length > 0) {
              page.content.forEach(item => {
                rawText += item.str + ' ';
              });
              rawText += '\n'; // 페이지 구분을 위해 줄바꿈 추가
            }
          });
        }
      } catch (pdfErr) {
        console.error("PDF 추출 중 에러 발생:", pdfErr);
        return res.status(500).json({ success: false, message: 'PDF 텍스트 추출에 실패했습니다.' });
      }

      if (rawText.trim().length < 30) {
        // 텍스트가 거의 없는 이미지형 PDF → GPT에 안내
        return res.status(400).json({
          success: false,
          message: '이미지 기반 PDF입니다. PDF를 캡처하여 이미지(JPG/PNG)로 업로드해주세요.'
        });
      }

      // GPT로 정리 (선택적 - 바로 rawText 써도 됨)
      const API_KEY = process.env.API_KEY;
      if (API_KEY) {
        try {
          const apiResponse = await axios.post(
            'https://api.openai.com/v1/chat/completions',
            {
              model: 'gpt-4o-mini',
              messages: [
                { role: 'system', content: '당신은 문서 정리 전문가입니다. 추출된 PDF 텍스트를 깔끔하게 정리해주세요. 제품명, 가격, 특징, 소재, 사이즈 등 핵심 정보를 구조화하여 반환합니다. 마크다운 없이 일반 텍스트로 정리해주세요.' },
                { role: 'user', content: '다음 PDF에서 추출한 텍스트를 깔끔하게 정리해주세요:\n\n' + rawText.substring(0, 10000) }
              ],
              max_tokens: 4096,
              temperature: 0.2,
            },
            {
              headers: { 'Authorization': `Bearer ${API_KEY}`, 'Content-Type': 'application/json' },
              timeout: 30000,
            }
          );
          extractedText = apiResponse.data.choices?.[0]?.message?.content || rawText;
        } catch (e) {
          console.warn('GPT 정리 실패, 원본 텍스트 사용:', e.message);
          extractedText = rawText;
        }
      } else {
        extractedText = rawText;
      }

    } else {
      // 🌟 이미지 → GPT Vision으로 추출 (기존 로직 유지)
      const API_KEY = process.env.API_KEY;
      if (!API_KEY) return res.status(500).json({ success: false, message: 'API_KEY 필요' });

      const base64 = fileBuffer.toString('base64');
      const mediaType = req.file.mimetype;

      const apiResponse = await axios.post(
        'https://api.openai.com/v1/chat/completions',
        {
          model: 'gpt-4o',
          messages: [
            { role: 'system', content: '당신은 문서 분석 전문가입니다. 이미지에서 텍스트를 정확하게 추출하여 한국어로 정리해주세요. 제품명, 가격, 특징, 소재, 사이즈 등 모든 정보를 포함해주세요. 마크다운 없이 일반 텍스트로 깔끔하게 정리해주세요.' },
            {
              role: 'user', content: [
                { type: 'image_url', image_url: { url: `data:${mediaType};base64,${base64}`, detail: 'high' } },
                { type: 'text', text: '이 문서/이미지의 내용을 빠짐없이 텍스트로 추출해주세요.' }
              ]
            }
          ],
          max_tokens: 4096,
          temperature: 0.2,
        },
        {
          headers: { 'Authorization': `Bearer ${API_KEY}`, 'Content-Type': 'application/json' },
          timeout: 60000,
        }
      );
      extractedText = apiResponse.data.choices?.[0]?.message?.content || '';
    }

    console.log('📄 텍스트 추출 완료:', extractedText.substring(0, 100) + '...');
    res.json({ success: true, text: extractedText, filename: req.file.originalname });

  } catch (e) {
    console.error('문서 추출 에러:', e.response?.data || e.message);
    res.status(500).json({ success: false, message: e.response?.data?.error?.message || e.message });
  }
});








// ========== [추가] 뷰저블(배너 클릭 히트맵) 트래킹 API ==========
app.post('/api/track-click', async (req, res) => {
  try {
    const { x, y, bannerId, url, screenWidth, screenHeight, imageUrl, deviceType } = req.body;

    if (x === undefined || y === undefined || !bannerId) {
      return res.status(400).json({ success: false, message: '필수 데이터가 누락되었습니다.' });
    }

    // ===== IP 제외 목록 체크 =====
    const clientIp = (req.headers['x-forwarded-for'] || req.ip || '').split(',')[0].trim();
    const excluded = await db.collection('ipExcludes').findOne({ ip: clientIp });
    if (excluded) {
      return res.status(200).json({ success: true, message: '제외 IP - 데이터 수집 안 함' });
    }
    // ============================

    const clickData = {
      bannerId,
      url,
      imageUrl: imageUrl || '', // 관리자 화면 표시를 위한 이미지 URL
      deviceType: deviceType || 'pc', // pc 또는 mobile 구분 (기본값 pc)
      coordinates: { x, y },
      screenSize: { width: screenWidth, height: screenHeight },
      timestamp: new Date(),
    };

    const result = await db.collection('bannerClicks').insertOne(clickData);

    res.json({ success: true, message: '클릭 데이터가 저장되었습니다.', data: clickData });
  } catch (err) {
    console.error('클릭 트래킹 저장 오류:', err);
    res.status(500).json({ success: false, message: '서버 오류가 발생했습니다.' });
  }
});

// ========== [추가] 뷰저블(배너 클릭 히트맵) 데이터 조회 API ==========
app.get('/api/track-click', async (req, res) => {
  try {
    const { bannerId, deviceType, startDate, endDate } = req.query;
    const query = {};
    if (bannerId) query.bannerId = bannerId;

    // 디바이스 필터: deviceType 필드가 없는 구버전 데이터도 올바르게 처리
    if (deviceType && deviceType !== 'all') {
      if (deviceType === 'pc') {
        // pc 선택 시: deviceType이 'pc'이거나 아예 없는(null/undefined) 기존 데이터 포함
        query.$or = [{ deviceType: 'pc' }, { deviceType: { $exists: false } }, { deviceType: null }];
      } else {
        query.deviceType = deviceType; // mobile 등 명확한 값만 필터
      }
    }

    // 날짜 기간 필터링
    if (startDate || endDate) {
      query.timestamp = {};
      if (startDate) query.timestamp.$gte = new Date(`${startDate}T00:00:00.000Z`);
      if (endDate) query.timestamp.$lte = new Date(`${endDate}T23:59:59.999Z`);
    }

    const clicks = await db.collection('bannerClicks').find(query).toArray();
    res.json({ success: true, data: clicks });
  } catch (err) {
    console.error('클릭 트래킹 조회 오류:', err);
    res.status(500).json({ success: false, message: '서버 오류가 발생했습니다.' });
  }
});

// ========== [추가] 뷰저블(배너 히트맵) 배너별 기초 통계 목록 API ==========
app.get('/api/track-click/summary', async (req, res) => {
  try {
    const { startDate, endDate, deviceType } = req.query;
    const matchStage = {};

    // 디바이스 필터: deviceType 필드가 없는 구버전 데이터도 올바르게 처리
    if (deviceType && deviceType !== 'all') {
      if (deviceType === 'pc') {
        // pc 선택 시: deviceType이 'pc'이거나 아예 없는(null/undefined) 기존 데이터 포함
        matchStage.$or = [{ deviceType: 'pc' }, { deviceType: { $exists: false } }, { deviceType: null }];
      } else {
        matchStage.deviceType = deviceType; // mobile 등 명확한 값만 필터
      }
    }

    // 날짜 필터
    if (startDate || endDate) {
      matchStage.timestamp = {};
      if (startDate) matchStage.timestamp.$gte = new Date(`${startDate}T00:00:00.000Z`);
      if (endDate) matchStage.timestamp.$lte = new Date(`${endDate}T23:59:59.999Z`);
    }

    const pipeline = [];
    if (Object.keys(matchStage).length > 0) pipeline.push({ $match: matchStage });

    pipeline.push(
      { $sort: { timestamp: -1 } },
      {
        $group: {
          // 배너ID와 기기별로 각각 그룹화하여 표시합니다
          _id: { bannerId: "$bannerId", deviceType: "$deviceType" },
          imageUrl: { $first: "$imageUrl" },
          screenWidth: { $first: "$screenSize.width" },
          screenHeight: { $first: "$screenSize.height" },
          clickCount: { $sum: 1 }
        }
      },
      { $sort: { clickCount: -1 } }
    );

    const summary = await db.collection('bannerClicks').aggregate(pipeline).toArray();

    // 프론트엔드에서 쓰기 쉽도록 데이터 평탄화 (Flatten)
    const formattedSummary = summary.map(item => ({
      bannerId: item._id.bannerId,
      deviceType: item._id.deviceType || 'pc',
      imageUrl: item.imageUrl,
      screenWidth: item.screenWidth,
      screenHeight: item.screenHeight,
      clickCount: item.clickCount
    }));

    res.json({ success: true, data: formattedSummary });
  } catch (err) {
    console.error('히트맵 통계 조회 오류:', err);
    res.status(500).json({ success: false, message: '서버 오류가 발생했습니다.' });
  }
});

// ========== [추가] 배너 비노출(아카이브) 토글 API ==========
// POST /api/banner-archive  body: { bannerId, deviceType }
app.post('/api/banner-archive', async (req, res) => {
  try {
    const { bannerId, deviceType } = req.body;
    if (!bannerId) return res.status(400).json({ success: false, message: 'bannerId 필요' });

    const col = db.collection('bannerArchive');
    const existing = await col.findOne({ bannerId, deviceType: deviceType || 'pc' });

    if (existing) {
      // 이미 아카이브된 경우 → 복원
      await col.deleteOne({ _id: existing._id });
      res.json({ success: true, archived: false });
    } else {
      // 새로 아카이브
      await col.insertOne({ bannerId, deviceType: deviceType || 'pc', archivedAt: new Date() });
      res.json({ success: true, archived: true });
    }
  } catch (err) {
    console.error('배너 아카이브 오류:', err);
    res.status(500).json({ success: false, message: '서버 오류' });
  }
});

// GET /api/banner-archive - 아카이브된 배너 목록 조회
app.get('/api/banner-archive', async (req, res) => {
  try {
    const archived = await db.collection('bannerArchive').find({}).toArray();
    res.json({ success: true, data: archived });
  } catch (err) {
    res.status(500).json({ success: false });
  }
});



// ========== [추가] 제외 IP 관리 API ==========
// GET: 제외 IP 목록 조회
app.get('/api/ip-exclude', async (req, res) => {
  try {
    const list = await db.collection('ipExcludes').find({}).toArray();
    res.json({ success: true, data: list });
  } catch (err) { res.status(500).json({ success: false }); }
});

// POST: IP 제외 등록
app.post('/api/ip-exclude', async (req, res) => {
  try {
    const { ip, label } = req.body;
    if (!ip) return res.status(400).json({ success: false, message: 'ip 필요' });
    const exists = await db.collection('ipExcludes').findOne({ ip });
    if (exists) return res.json({ success: false, message: '이미 등록된 IP입니다.' });
    await db.collection('ipExcludes').insertOne({ ip, label: label || '', createdAt: new Date() });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ success: false }); }
});

// DELETE: IP 제외 해제
app.delete('/api/ip-exclude/:ip', async (req, res) => {
  try {
    const ip = decodeURIComponent(req.params.ip);
    await db.collection('ipExcludes').deleteOne({ ip });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ success: false }); }
});

// 현재 접속 IP 확인용
app.get('/api/my-ip', (req, res) => {
  const ip = (req.headers['x-forwarded-for'] || req.ip || '').split(',')[0].trim();
  res.json({ success: true, ip });
});





// ========== [추가] 이벤트 퀴즈 세션 API ==========
const { v4: uuidv4 } = require('uuid');

// POST /api/event/session - 새 세션 시작
app.post('/api/event/session', async (req, res) => {
  try {
    const { visitorId, userAgent } = req.body;
    const ip = (req.headers['x-forwarded-for'] || req.ip || '').split(',')[0].trim();
    const sessionId = uuidv4();
    await db.collection('eventSessions').insertOne({
      sessionId, visitorId: visitorId || 'unknown', ip, userAgent: userAgent || '',
      path: [], finalResult: null, applicantName: null, applicantPhone: null,
      startedAt: new Date(), completedAt: null
    });
    res.json({ success: true, sessionId });
  } catch (err) { res.status(500).json({ success: false }); }
});

// PATCH /api/event/session/:id - 경로(답변) 추가
app.patch('/api/event/session/:id', async (req, res) => {
  try {
    const { question, answer } = req.body;
    await db.collection('eventSessions').updateOne(
      { sessionId: req.params.id },
      { $push: { path: { question, answer, timestamp: new Date() } } }
    );
    res.json({ success: true });
  } catch (err) { res.status(500).json({ success: false }); }
});

// POST /api/event/session/:id/complete - 최종결과 + 응모정보 저장
app.post('/api/event/session/:id/complete', async (req, res) => {
  try {
    const { finalResult, applicantName, applicantPhone } = req.body;
    await db.collection('eventSessions').updateOne(
      { sessionId: req.params.id },
      { $set: { finalResult, applicantName: applicantName || '', applicantPhone: applicantPhone || '', completedAt: new Date() } }
    );
    res.json({ success: true });
  } catch (err) { res.status(500).json({ success: false }); }
});

// GET /api/event/session/:id - 단일 세션 조회
app.get('/api/event/session/:id', async (req, res) => {
  try {
    const session = await db.collection('eventSessions').findOne({ sessionId: req.params.id });
    if (!session) return res.status(404).json({ success: false });
    res.json({ success: true, data: session });
  } catch (err) { res.status(500).json({ success: false }); }
});

// GET /api/event/sessions - 전체 세션 목록 (관리자)
app.get('/api/event/sessions', async (req, res) => {
  try {
    const list = await db.collection('eventSessions').find({}).sort({ startedAt: -1 }).limit(200).toArray();
    res.json({ success: true, data: list });
  } catch (err) { res.status(500).json({ success: false }); }
});



// =================================================================
// 🌟 어썸 피플 관련 데이터 (0시 초기화 및 10시 10분 덮어쓰기 스케줄러)
// =================================================================

// 1. 타겟 제품 및 정규화 로직
const AWESOME_TARGET_PRODUCTS = [
  "요기보 피라미드", "요기보 피라미드 프리미엄", "요기보 피라미드 프리미엄 플러스",
  "요기보 드롭", "요기보 드롭 프리미엄", "요기보 드롭 프리미엄 플러스",
  "요기보 슬림", "요기보 슬림 프리미엄", "요기보 슬림 프리미엄 플러스",
  "요기보 카카오프렌즈 피라미드", "요기보 카카오프렌즈 피라미드 프리미엄", "요기보 카카오프렌즈 피라미드 프리미엄 플러스",
  "요기보 카카오프렌즈 드롭", "요기보 카카오프렌즈 드롭 프리미엄", "요기보 카카오프렌즈 드롭 프리미엄 플러스",
  "요기보 카카오프렌즈 슬림", "요기보 카카오프렌즈 슬림 프리미엄", "요기보 카카오프렌즈 슬림 프리미엄 플러스",
  "솔리드 스퀴지보", "스퀴지보 하트", "스퀴지보 애니멀",
  "요기보 메이트", "요기보 플랜트 메이트",
  "요기보 메가 메이트 팍스", "요기보 메가 메이트 티렉스", "요기보 메가 메이트 유니콘",
  "요기보 메이트 나르왈", "요기보 메이트 우파루파", "요기보 메이트 라쿤",
  "요기보 메이트 드래곤", "요기보 메이트 티렉스", "요기보 메이트 샤크",
  "요기보 메이트 헤지호그", "요기보 메이트 디노", "요기보 메이트 도그",
  "요기보 메이트 코알라", "요기보 메이트 판다", "요기보 메이트 펭귄",
  "요기보 메이트 옥토푸스", "요기보 메이트 엘리펀트", "요기보 메이트 팍스",
  "요기보 메이트 돌핀", "요기보 메이트 지라프", "요기보 메이트 써니",
  "요기보 메이트 아로", "요기보 메이트 스트라우프"
];

const normalizeAwesome = (str) => str.replace(/요기보/g, '').replace(/[^가-힣a-zA-Z0-9]/g, '').toLowerCase();

const awesomeSearchTargets = AWESOME_TARGET_PRODUCTS.map(name => ({
  originalName: name,
  searchKey: normalizeAwesome(name)
})).sort((a, b) => b.searchKey.length - a.searchKey.length);

const AWESOME_EXCLUDE_KEYWORDS = ["플랜트 스퀴지보", "비즈", "커버"].map(name => normalizeAwesome(name));


// 🌟 [추가] 이름 마스킹 헬퍼 (비회원용)
const maskName = (name) => {
  if (!name || name.length < 2) return '어썸피플';
  if (name.length === 2) return name[0] + '*';
  return name[0] + '*'.repeat(name.length - 2) + name[name.length - 1];
};

// 🌟 [추가] 회원 ID 마스킹 헬퍼 (예: yogibo123 -> yog***)
const maskMemberId = (id) => {
  if (!id || id.length < 2) return id;
  if (id.length <= 3) return id.substring(0, 1) + '**';
  return id.substring(0, 3) + '*'.repeat(id.length - 3);
};


// 2. 10시 10분 매출 집계 & 구매자 리스트 추출 통합 로직
async function aggregateAwesomeSalesData() {
  if (!db) {
    console.error('❌ [Awesome People] DB 연결이 되어있지 않아 작업을 중단합니다.');
    return { success: false, message: 'DB 연결 오류' };
  }

  try {
    console.log('🔄 [Awesome People] 10:10 온/오프라인 매출 집계 및 구매자 추출 시작...');

    const today = new Date();
    const currentYear = today.getFullYear();
    const currentMonth = today.getMonth() + 1;
    const startDateStr = `${currentYear}-03-10`;

    // 🌟 [추가] 최근 7일 기준일 계산
    const sevenDaysAgoStr = moment().tz('Asia/Seoul').subtract(7, 'days').format('YYYY-MM-DD');

    const offlineBase = 'https://port-0-realtime-lzgmwhc4d9883c97.sel4.cloudtype.app/api/orders';
    const onlineBase = 'https://port-0-onorder-lzgmwhc4d9883c97.sel4.cloudtype.app/api/online/orders';

    const fetchPromises = [];
    const timestamp = new Date().getTime();

    // 3월부터 현재 월까지 병렬 호출
    for (let m = 3; m <= currentMonth; m++) {
      const monthParam = `${currentYear}-${String(m).padStart(2, '0')}`;
      fetchPromises.push(axios.get(`${offlineBase}?month=${monthParam}&store=all&_t=${timestamp}`));
      fetchPromises.push(axios.get(`${onlineBase}?month=${monthParam}&store=all&_t=${timestamp}`));
    }

    const responses = await Promise.allSettled(fetchPromises);

    const allOrders = responses.reduce((acc, res) => {
      if (res.status === 'fulfilled' && res.value.data && res.value.data.success && res.value.data.orders) {
        return acc.concat(res.value.data.orders);
      }
      return acc;
    }, []);

    let grandTotalQty = 0;
    let grandTotalAmount = 0;
    let recentBuyers = []; // 🌟 구매자 리스트 배열

    const productSummary = allOrders.reduce((acc, cur) => {
      if (cur.date < startDateStr) return acc;

      const rawProductName = (cur.productName || "");
      const normalizedRawName = normalizeAwesome(rawProductName);

      const isExcluded = AWESOME_EXCLUDE_KEYWORDS.some(exWord => normalizedRawName.includes(exWord));
      if (isExcluded) return acc;

      const matchedObj = awesomeSearchTargets.find(target => normalizedRawName.includes(target.searchKey));
      if (!matchedObj) return acc;

      // 🌟 [핵심] 7일 이내 구매자면 ID/이름 추출
      if (cur.date >= sevenDaysAgoStr) {
        const rawId = String(cur.memberId || cur.member_id || cur.userId || '');
        let displayUser = '';

        // 회원 ID가 존재하고 비회원(guest)이 아닐 경우 ID 마스킹
        if (rawId && rawId.toLowerCase() !== 'guest' && rawId !== '비회원') {
          displayUser = maskMemberId(rawId);
        } else {
          // 비회원이나 네이버페이일 경우 이름 마스킹
          const rawName = String(cur.buyerName || cur.buyer_name || cur.billing_name || cur.name || cur.customerName || '어썸피플');
          displayUser = maskName(rawName);
        }

        if (!recentBuyers.includes(displayUser)) {
          recentBuyers.push(displayUser);
        }
      }

      const displayTargetName = matchedObj.originalName;
      const qty = Number(cur.qty) || 0;
      const amount = Number(cur.amount) || 0;

      if (!acc[displayTargetName]) {
        acc[displayTargetName] = { qty: 0, amount: 0 };
      }

      acc[displayTargetName].qty += qty;
      acc[displayTargetName].amount += amount;

      grandTotalQty += qty;
      grandTotalAmount += amount;

      return acc;
    }, {});

    const executedAtKST = moment().tz('Asia/Seoul').toDate();
    const rewardAmount = Math.floor(grandTotalAmount * 0.01);

    // 최신 구매자가 먼저 나오도록 배열 뒤집기
    recentBuyers.reverse();

    const collection = db.collection('asSomeDtat');
    await collection.updateOne(
      { docType: 'awesome_daily_summary' },
      {
        $set: {
          totalQuantity: grandTotalQty,
          totalAmount: grandTotalAmount,
          rewardAmount: rewardAmount,
          productDetails: productSummary,
          recentBuyers: recentBuyers, // 🌟 추출한 ID/이름 리스트 저장
          period: `${startDateStr} ~ ${moment(executedAtKST).format('YYYY-MM-DD')}`,
          updatedAt: executedAtKST,
          status: 'calculated_at_1010'
        }
      },
      { upsert: true }
    );
    console.log(`✅ [Awesome People] 집계 및 덮어쓰기 완료! (구매자: ${recentBuyers.length}명, 총 매출: ${grandTotalAmount}원)`);

    return { success: true, grandTotalAmount, rewardAmount };

  } catch (error) {
    console.error('❌ [Awesome People] 매출 집계 스케줄러 에러:', error);
    return { success: false, message: error.message };
  }
}

// 4. 매일 10시 10분에 실데이터로 덮어쓰는 스케줄러
cron.schedule('10 10 * * *', async () => {
  console.log('⏰ [Cron] 10:10 어썸피플 매출 집계 스케줄러 작동 시작!');

  // 하나로 통합된 집계 함수 실행!
  const result = await aggregateAwesomeSalesData();

  try {
    const logCollection = db.collection('awesome_sync_logs');
    const executedAtKST = moment().tz('Asia/Seoul').toDate();

    if (result.success) {
      console.log(`🟢 [Cron] 10:10 스케줄러 정상 작동 완료`);
      await logCollection.insertOne({
        type: 'cron_1010_sync',
        status: 'SUCCESS',
        grandTotalAmount: result.grandTotalAmount,
        rewardAmount: result.rewardAmount,
        executedAt: executedAtKST
      });
    } else {
      console.error(`🔴 [Cron] 10:10 스케줄러 작동 실패: ${result.message}`);
      await logCollection.insertOne({
        type: 'cron_1010_sync',
        status: 'FAIL',
        errorMessage: result.message,
        executedAt: executedAtKST
      });
    }
  } catch (logError) {
    console.error('❌ [Cron] DB에 스케줄러 로그 에러:', logError);
  }
}, {
  scheduled: true,
  timezone: "Asia/Seoul"
});

// 5. 테스트용 수동 동기화 라우터
app.get('/api/awesome-people/manual-sync', async (req, res) => {
  const result = await aggregateAwesomeSalesData();

  if (result.success) {
    res.json({
      success: true,
      message: '어썸 피플 데이터 및 최근 구매자 리스트 강제 동기화가 완료되었습니다.',
      data: result
    });
  } else {
    res.status(500).json({
      success: false,
      message: `데이터 집계 실패: ${result.message}`
    });
  }
});

// 6. 프론트엔드 데이터 제공용 GET API
app.get('/api/awesome-people/summary', async (req, res) => {
  try {
    const data = await db.collection('asSomeDtat').findOne({ docType: 'awesome_daily_summary' });
    res.json({
      success: true,
      totalAmount: data ? (data.rewardAmount || 0) : 0,
      originalTotalAmount: data ? data.totalAmount : 0,
      totalQuantity: data ? (data.totalQuantity || 0) : 0,
      recentBuyers: data ? (data.recentBuyers || []) : [],
      updatedAt: data ? data.updatedAt : null
    });
  } catch (error) {
    console.error('데이터 조회 에러:', error);
    res.status(500).json({ success: false, totalAmount: 0, totalQuantity: 0, recentBuyers: [] });
  }
});

// =========================================================================
// [추가] 어썸피플 타겟 제품 구매자(Cafe24 회원 ID) 직접 조회 API
// =========================================================================
app.get('/api/cafe24/awesome-buyers', async (req, res) => {
  try {
    const { startDate, endDate } = req.query;
    if (!startDate || !endDate) {
      return res.status(400).json({ success: false, message: 'startDate, endDate가 필요합니다.' });
    }

    // 타겟 제품 목록 (프론트와 동일하게 설정)
    const TARGET_PRODUCTS = [
        "요기보 피라미드", "요기보 피라미드 프리미엄", "요기보 피라미드 프리미엄 플러스",
        "요기보 드롭", "요기보 드롭 프리미엄", "요기보 드롭 프리미엄 플러스",
        "요기보 슬림", "요기보 슬림 프리미엄", "요기보 슬림 프리미엄 플러스",
        "요기보 카카오프렌즈 피라미드", "요기보 카카오프렌즈 피라미드 프리미엄", "요기보 카카오프렌즈 피라미드 프리미엄 플러스",
        "요기보 카카오프렌즈 드롭", "요기보 카카오프렌즈 드롭 프리미엄", "요기보 카카오프렌즈 드롭 프리미엄 플러스",
        "요기보 카카오프렌즈 슬림", "요기보 카카오프렌즈 슬림 프리미엄", "요기보 카카오프렌즈 슬림 프리미엄 플러스",
        "솔리드 스퀴지보", "스퀴지보 하트", "스퀴지보 애니멀",
        "요기보 메이트", "요기보 플랜트 메이트",
        "요기보 메가 메이트 팍스", "요기보 메가 메이트 티렉스", "요기보 메가 메이트 유니콘",
        "요기보 메이트 나르왈", "요기보 메이트 우파루파", "요기보 메이트 라쿤",
        "요기보 메이트 드래곤", "요기보 메이트 티렉스", "요기보 메이트 샤크",
        "요기보 메이트 헤지호그", "요기보 메이트 디노", "요기보 메이트 도그",
        "요기보 메이트 코알라", "요기보 메이트 판다", "요기보 메이트 펭귄",
        "요기보 메이트 옥토푸스", "요기보 메이트 엘리펀트", "요기보 메이트 팍스",
        "요기보 메이트 돌핀", "요기보 메이트 지라프", "요기보 메이트 써니",
        "요기보 메이트 아로", "요기보 메이트 스트라우프"
    ];
    const normalize = (str) => str.replace(/요기보/g, '').replace(/[^가-힣a-zA-Z0-9]/g, '').toLowerCase();
    const searchTargets = TARGET_PRODUCTS.map(name => ({ originalName: name, searchKey: normalize(name) }));
    const EXCLUDE_KEYWORDS = ["플랜트 스퀴지보", "비즈", "커버"].map(name => normalize(name));

    const fetchFromCafe24 = async (url, params, retry = false) => {
      try {
        return await axios.get(url, { params, headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json', 'X-Cafe24-Api-Version': CAFE24_API_VERSION } });
      } catch (err) {
        if (err.response && err.response.status === 401 && !retry) {
          await refreshAccessToken();
          return await fetchFromCafe24(url, params, true);
        }
        throw err;
      }
    };

    let allOrders = [];
    let orderHasMore = true;
    let orderOffset = 0;
    
    // 카페24에서 해당 기간의 주문 긁어오기
    while (orderHasMore && orderOffset < 5000) {
      const orderRes = await fetchFromCafe24(
        `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/orders`,
        { shop_no: 1, start_date: startDate, end_date: endDate, date_type: 'pay_date', limit: 100, offset: orderOffset, embed: 'items' }
      );
      const orders = orderRes.data.orders || [];
      allOrders = allOrders.concat(orders);
      
      if (orders.length < 100) orderHasMore = false;
      else orderOffset += 100;
    }

    const uniqueBuyers = new Set();
    
    allOrders.forEach(o => {
      // 비회원 제외
      if (!o.member_id || String(o.member_id).toLowerCase() === 'guest') return; 

      let hasTarget = false;
      (o.items || []).forEach(item => {
        const normalizedRawName = normalize(item.product_name || "");
        if (EXCLUDE_KEYWORDS.some(exWord => normalizedRawName.includes(exWord))) return; 
        
        if (searchTargets.find(target => normalizedRawName.includes(target.searchKey))) {
          hasTarget = true;
        }
      });

      // 타겟 제품이 포함된 주문이면 회원 ID 수집
      if (hasTarget) {
        uniqueBuyers.add(o.member_id);
      }
    });

    res.json({ success: true, count: uniqueBuyers.size, buyers: Array.from(uniqueBuyers) });
  } catch (error) {
    console.error('Cafe24 구매자 조회 에러:', error.message);
    res.status(500).json({ success: false, message: 'Server Error' });
  }
});

// =========================================================================
// [봄꽃 룰렛 이벤트] 전역 상수
// =========================================================================
const ROLLET_COLLECTION = 'event_2026_03_Rollet';
// =========================================================================
// [API 1] 룰렛 돌리기 및 응모 (어드민 확률 + 실시간 재고 연동형 / MongoDB 배열 버그 수정)
// =========================================================================
app.post('/api/raffle/play', async (req, res) => {
  try {
    const { userId } = req.body;

    if (!userId || userId === 'GUEST' || userId === 'null') {
      return res.status(401).json({ success: false, message: '회원 로그인 후 참여 가능합니다.' });
    }

    const todayStr = moment().tz('Asia/Seoul').format('YYYY-MM-DD');
    const rolletCollection = db.collection(ROLLET_COLLECTION);
    const stockCollection = db.collection('raffle_daily_stock'); 

    // 1. 중복 참여 체크
    const existingEntry = await rolletCollection.findOne({ userId: userId, entryDate: todayStr });
    if (existingEntry) {
      return res.json({ success: false, code: 'ALREADY_ENTERED', message: '오늘은 이미 참여하셨습니다.' });
    }

    // 2. 오늘의 재고 및 어드민이 설정한 확률 데이터 불러오기
    let dailyData = await stockCollection.findOne({ date: todayStr });
    
    if (!dailyData) {
      return res.status(400).json({ success: false, message: '금일 이벤트가 준비되지 않았습니다.' });
    }

    // 3. 재고가 0인 상품은 가중치를 0으로 강제 변환
    let totalWeight = 0;
    const activePrizes = dailyData.prizes.map(p => {
      const effectiveWeight = p.currentStock > 0 ? Number(p.prob) : 0;
      totalWeight += effectiveWeight;
      return { ...p, effectiveWeight };
    });

    if (totalWeight <= 0) {
      return res.json({ success: false, code: 'SOLD_OUT', message: '금일 준비된 모든 경품이 소진되었습니다.' });
    }

    // 4. 동적 가중치 기반 랜덤 추첨 
    const randomNum = Math.random() * totalWeight;
    let weightSum = 0;
    let wonPrize = activePrizes[activePrizes.length - 1]; 

    for (let i = 0; i < activePrizes.length; i++) {
      weightSum += activePrizes[i].effectiveWeight;
      if (randomNum <= weightSum) {
        wonPrize = activePrizes[i];
        break;
      }
    }

    // 5. 당첨된 상품의 재고 차감 (Atomic Update)
    // 💡 [수정됨] $elemMatch를 사용하여 정확히 일치하는 객체만 찾아내도록 수정!
    const updateResult = await stockCollection.findOneAndUpdate(
      { 
        date: todayStr, 
        prizes: { 
          $elemMatch: { 
            name: wonPrize.name, 
            currentStock: { $gt: 0 } 
          } 
        }
      },
      { 
        $inc: { "prizes.$.currentStock": -1 } 
      },
      { returnDocument: 'after' }
    );

    const updatedDoc = updateResult && updateResult.value ? updateResult.value : updateResult;

    // 간발의 차로 재고가 털렸을 경우 3등(오리 비눗방울) 우회 처리
    if (!updatedDoc) {
      console.warn(`[룰렛] ${userId}님이 ${wonPrize.name}에 당첨되었으나 재고 소진됨. 3등 우회 처리.`);
      
      const fallbackPrizeName = "오리 비눗방울";
      // 💡 [수정됨] 우회 차감 로직에도 동일하게 $elemMatch 적용
      const fallbackResult = await stockCollection.findOneAndUpdate(
        { 
          date: todayStr, 
          prizes: { 
            $elemMatch: { 
              name: fallbackPrizeName, 
              currentStock: { $gt: 0 } 
            } 
          }
        },
        { $inc: { "prizes.$.currentStock": -1 } },
        { returnDocument: 'after' }
      );

      const fallbackDoc = fallbackResult && fallbackResult.value ? fallbackResult.value : fallbackResult;

      if (!fallbackDoc) {
         return res.json({ success: false, code: 'SOLD_OUT', message: '금일 준비된 모든 경품이 소진되었습니다.' });
      }
      wonPrize.name = fallbackPrizeName; 
    }

    // 6. 최종 당첨 결과 DB 기록
    const newEntry = {
      userId: userId,
      optionName: wonPrize.name,
      entryDate: todayStr,
      createdAt: new Date(),
    };
    await rolletCollection.insertOne(newEntry);

    res.json({ success: true, prizeName: wonPrize.name });

  } catch (error) {
    console.error('룰렛 응모 오류:', error);
    res.status(500).json({ success: false, message: '서버 오류 발생' });
  }
});

// =========================================================================
// [API 2] 응모 현황 조회
// =========================================================================
app.get('/api/raffle/status', async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId || userId === 'GUEST' || userId === 'null') {
      return res.json({ success: true, isEntered: false, message: '로그인 필요' });
    }
    const existingEntry = await db.collection(ROLLET_COLLECTION).findOne({ userId: userId });
    
    if (existingEntry) {
      return res.json({ success: true, isEntered: true, optionName: existingEntry.optionName });
    } else {
      return res.json({ success: true, isEntered: false });
    }
  } catch (error) {
    console.error('룰렛 상태 조회 오류:', error);
    res.status(500).json({ success: false, message: '서버 오류' });
  }
});

// =========================================================================
// [API 3] 관리자용: 룰렛 참여자 목록 데이터 조회 (💡 팝업용 상세 데이터 포함)
// =========================================================================
app.get('/api/raffle/admin/participants', async (req, res) => {
  try {
    const { startDate, endDate } = req.query;
    let query = {};
    if (startDate || endDate) {
      query.entryDate = {};
      if (startDate) query.entryDate.$gte = startDate;
      if (endDate) query.entryDate.$lte = endDate;
    }

    const participants = await db.collection(ROLLET_COLLECTION).find(query).sort({ createdAt: -1 }).toArray();

    if (!participants.length) {
      return res.json({ success: true, data: [] });
    }

    const userIds = participants.map(p => p.userId);
    const minDate = participants[participants.length - 1].createdAt;

    // 장바구니/결제 데이터 한 번에 메모리로 로드
    const [allCarts, allOrders] = await Promise.all([
      db.collection('cart').find({ userId: { $in: userIds }, createdAt: { $gte: minDate } }).toArray(),
      db.collection('orders').find({ userId: { $in: userIds }, status: 'PAID', createdAt: { $gte: minDate } }).toArray()
    ]);

    const enrichedData = participants.map((p) => {
      // 💡 1. 룰렛 참여 시간 이후의 장바구니 내역 필터링
      const userCartItems = allCarts.filter(cart => cart.userId === p.userId && cart.updatedAt >= p.createdAt);
      const hasCart = userCartItems.length > 0;

      // 💡 2. 룰렛 참여 시간 이후의 결제 내역 필터링
      const userPurchaseItems = allOrders.filter(order => order.userId === p.userId && order.createdAt >= p.createdAt);
      const hasPurchase = userPurchaseItems.length > 0;

      return {
        userId: p.userId,
        optionName: p.optionName,
        entryDate: p.entryDate,
        createdAt: p.createdAt,
        hasCart: hasCart,
        hasPurchase: hasPurchase,
        
        // 🚨 프론트 모달창을 위해 상세 데이터(배열)를 다시 꽉꽉 채워서 넘겨줍니다! 🚨
        cartDetails: userCartItems.map(item => ({
          productName: item.productName || '알 수 없는 상품',
          qty: item.qty || 1,
          addedAt: item.updatedAt || item.createdAt
        })),
        purchaseDetails: userPurchaseItems.map(item => ({
          productName: item.productName || '알 수 없는 상품',
          qty: item.qty || 1,
          addedAt: item.createdAt
        }))
      };
    });

    res.json({ success: true, data: enrichedData });
  } catch (error) {
    console.error('참여자 목록 조회 오류:', error);
    res.status(500).json({ success: false, message: '서버 오류' });
  }
});

// =========================================================================
// [API 4] 관리자 모달용: 카페24 Admin API 실시간 장바구니 조회 (투스텝 매핑)
// =========================================================================
app.get('/api/raffle/admin/cart-detail', async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ success: false, message: '회원 ID가 필요합니다.' });

    // 💡 공통 헤더 세팅 (API 버전 필수 포함)
    const CAFE24_HEADERS = (token) => ({
      'Authorization': `Bearer ${token.trim()}`,
      'Content-Type': 'application/json',
      'X-Cafe24-Api-Version': CAFE24_API_VERSION || '2025-12-01'
    });

    // [Step 1] 카페24 장바구니 API 호출
    const getCartFromCafe24 = async (token) => {
      return await axios.get(`https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/carts?member_id=${userId}`, {
        headers: CAFE24_HEADERS(token)
      });
    };

    let cartRes;
    try {
      cartRes = await getCartFromCafe24(accessToken);
    } catch (err) {
      if (err.response && err.response.status === 401) {
        console.log(`[장바구니] 토큰 만료. 재발급 시도...`);
        const newToken = await refreshAccessToken();
        cartRes = await getCartFromCafe24(newToken);
      } else throw err;
    }

    const carts = cartRes.data.carts || [];
    if (carts.length === 0) {
      return res.json({ success: true, data: [] });
    }

    // 장바구니에 있는 고유 상품 번호 추출
    const productNos = [...new Set(carts.map(c => c.product_no))];
    let productMap = {};

    // 💡 [Step 2] 상품 상세 정보(이름, 가격) 호출
    const productRequests = productNos.map(pNo => 
      axios.get(`https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/products/${pNo}?fields=product_name,price`, {
        headers: CAFE24_HEADERS(accessToken) 
      }).catch(e => null) 
    );

    const productResponses = await Promise.all(productRequests);
        
    productResponses.forEach((pRes, idx) => {
      if (pRes && pRes.data && pRes.data.product) {
        // 이름과 가격을 객체 형태로 저장 (가격은 숫자형으로 변환)
        productMap[productNos[idx]] = {
          name: pRes.data.product.product_name,
          price: Number(pRes.data.product.price) || 0 
        };
      }
    });

    // 💡 최종 데이터 조립
    const cartDetails = carts.map(item => {
      // productMap에 해당 상품 번호가 있으면 가져오고, 없으면 기본값 세팅 (방어 코드)
      const pInfo = productMap[item.product_no] || { name: `조회 불가 (상품번호:${item.product_no})`, price: 0 };
      
      return {
        productName: pInfo.name,
        price: pInfo.price, // 가격 데이터 매핑
        qty: item.quantity,
        addedAt: item.created_date 
      };
    });

    res.json({ success: true, data: cartDetails });
  } catch (error) {
    console.error('카페24 장바구니 매핑 에러:', error.response?.data || error.message);
    res.status(500).json({ success: false, message: '서버 오류' });
  }
});

// =========================================================================
// [추가] 프론트엔드 장바구니 트래킹 데이터 수신용 API
// =========================================================================
app.post('/api/trace/cart', async (req, res) => {
  try {
    const { userId, items } = req.body;

    // 데이터 유효성 검사
    if (!userId || !items || !Array.isArray(items)) {
      return res.status(400).json({ success: false, message: '잘못된 데이터 형식입니다.' });
    }

    const nowKST = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));

    // 프론트에서 받은 배열 데이터를 어드민 DB 구조(다큐먼트 단위)에 맞게 매핑
    const cartLogs = items.map(item => ({
      userId: userId,
      productCode: item.productCode,
      productName: item.productName,
      qty: item.qty,
      price: item.price,
      createdAt: nowKST,
      updatedAt: nowKST // 어드민에서 updatedAt을 참조하는 로직 대비
    }));

    // cart 컬렉션에 일괄 저장 (이벤트 트래킹이므로 로그성으로 계속 쌓음)
    if (cartLogs.length > 0) {
      await db.collection('cart').insertMany(cartLogs);
    }

    console.log(`[Cart Tracker] ${userId}님의 장바구니 상품 ${cartLogs.length}개 추적 저장 완료`);
    res.json({ success: true, message: '장바구니 데이터 트래킹 성공' });
    
  } catch (err) {
    console.error('장바구니 핑 수신 에러:', err);
    res.status(500).json({ success: false });
  }
});

// =========================================================================
// [API 5] 관리자용: 룰렛 참여 내역 엑셀 다운로드 (null 상품명 자동 복구 기능 추가)
// =========================================================================
app.get('/api/raffle/admin/excel', async (req, res) => {
  try {
    const { startDate, endDate } = req.query;
    let query = {};
    if (startDate || endDate) {
      query.entryDate = {};
      if (startDate) query.entryDate.$gte = startDate;
      if (endDate) query.entryDate.$lte = endDate;
    }

    const participants = await db.collection(ROLLET_COLLECTION).find(query).sort({ createdAt: -1 }).toArray();

    if (!participants.length) {
      return res.status(404).send('데이터가 없습니다.');
    }

    const userIds = participants.map(p => p.userId);
    const minDate = participants[participants.length - 1].createdAt;

    // 1. DB에서 장바구니 내역 조회
    const allCarts = await db.collection('cart').find({ 
      userId: { $in: userIds }, 
      createdAt: { $gte: minDate } 
    }).toArray();

    // 💡 [핵심 추가] DB에 productName이 null로 저장된 경우를 대비해 Cafe24에서 실시간으로 이름을 가져옵니다.
    const CAFE24_HEADERS = (token) => ({
      'Authorization': `Bearer ${token.trim()}`,
      'Content-Type': 'application/json',
      'X-Cafe24-Api-Version': CAFE24_API_VERSION || '2025-12-01'
    });

    // 이름이 없거나 'null'인 상품들의 번호(productCode)만 추출
    const cartProductNos = [...new Set(allCarts.filter(c => !c.productName || c.productName === 'null').map(c => c.productCode))].filter(Boolean);
    let cartProductMap = {};
    
    // 누락된 상품들의 이름을 카페24에서 다시 조회해 맵에 저장
    if (cartProductNos.length > 0) {
      const pRequests = cartProductNos.map(pNo => 
        axios.get(`https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/products/${pNo}?fields=product_name`, {
          headers: CAFE24_HEADERS(accessToken) 
        }).catch(e => null) 
      );
      const pResponses = await Promise.all(pRequests);
      pResponses.forEach((pRes, idx) => {
        if (pRes && pRes.data && pRes.data.product) {
          cartProductMap[cartProductNos[idx]] = pRes.data.product.product_name;
        }
      });
    }

    // 2. Cafe24 API로 기간 내 실제 결제 내역 긁어오기
    const sDate = moment(minDate).tz('Asia/Seoul').format('YYYY-MM-DD');
    const eDate = moment().tz('Asia/Seoul').add(1, 'days').format('YYYY-MM-DD'); 

    const getOrdersFromCafe24 = async (token, offset) => {
      return await axios.get(`https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/orders`, {
        params: { shop_no: 1, start_date: sDate, end_date: eDate, date_type: 'order_date', limit: 100, offset: offset, embed: 'items' },
        headers: CAFE24_HEADERS(token)
      });
    };

    let allCafe24Orders = [];
    let orderHasMore = true;
    let orderOffset = 0;
    let currentToken = accessToken;

    while (orderHasMore && orderOffset < 3000) { 
      try {
        const orderRes = await getOrdersFromCafe24(currentToken, orderOffset);
        const orders = orderRes.data.orders || [];
        allCafe24Orders = allCafe24Orders.concat(orders);
        if (orders.length < 100) orderHasMore = false;
        else orderOffset += 100;
      } catch (err) {
        if (err.response && err.response.status === 401) {
          currentToken = await refreshAccessToken();
          const orderRes = await getOrdersFromCafe24(currentToken, orderOffset);
          const orders = orderRes.data.orders || [];
          allCafe24Orders = allCafe24Orders.concat(orders);
          if (orders.length < 100) orderHasMore = false;
          else orderOffset += 100;
        } else {
          break; 
        }
      }
    }

    // 3. 참여자별 데이터 매핑 및 텍스트 조립
    const enrichedData = participants.map((p) => {
      // (1) 장바구니 텍스트 조립
      const userCarts = allCarts.filter(cart => cart.userId === p.userId && cart.updatedAt >= p.createdAt);
      const cartText = userCarts.length > 0 
        ? userCarts.map(c => {
            // 💡 null 일 경우 방금 매핑해온 새 이름으로 교체!
            let pName = c.productName;
            if (!pName || pName === 'null') {
              pName = cartProductMap[c.productCode] || '상품명 확인불가';
            }
            return `${pName} (${c.qty || 1}개) - ${(Number(c.price) || 0).toLocaleString()}원`;
          }).join('\n')
        : 'X';

      // (2) 결제내역 텍스트 조립
      const userOrders = allCafe24Orders.filter(o => 
        o.member_id === p.userId && 
        new Date(o.order_date) >= new Date(p.createdAt)
      );

      let purchaseText = 'X';
      if (userOrders.length > 0) {
        const purchaseItems = [];
        userOrders.forEach(o => {
          (o.items || []).forEach(item => {
            purchaseItems.push(`${item.product_name} (${item.quantity}개) - ${(Number(item.product_price) || 0).toLocaleString()}원`);
          });
        });
        purchaseText = purchaseItems.length > 0 ? purchaseItems.join('\n') : 'X';
      }

      return {
        userId: p.userId,
        optionName: p.optionName,
        entryDate: p.entryDate,
        createdAt: p.createdAt,
        cartText: cartText,
        purchaseText: purchaseText
      };
    });

    // 4. 엑셀 워크북 생성
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('봄꽃룰렛_참여자');

    worksheet.columns = [
      { header: 'No', key: 'index', width: 8 },
      { header: '회원 ID', key: 'userId', width: 20 },
      { header: '당첨 경품', key: 'optionName', width: 20 },
      { header: '응모일(기준)', key: 'entryDate', width: 15 },
      { header: '응모일시(상세)', key: 'createdAt', width: 25 },
      { header: '장바구니 상세 내역', key: 'cartText', width: 50 },
      { header: '결제 상세 내역', key: 'purchaseText', width: 50 },
    ];

    enrichedData.forEach((entry, idx) => {
      const row = worksheet.addRow({
        index: enrichedData.length - idx,
        userId: entry.userId,
        optionName: entry.optionName,
        entryDate: entry.entryDate,
        createdAt: entry.createdAt ? new Date(entry.createdAt).toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' }) : '-',
        cartText: entry.cartText,
        purchaseText: entry.purchaseText
      });
      
      row.getCell('cartText').alignment = { wrapText: true, vertical: 'top' };
      row.getCell('purchaseText').alignment = { wrapText: true, vertical: 'top' };
    });

    const filename = encodeURIComponent(`봄꽃룰렛_결과_${Date.now()}.xlsx`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${filename}`);

    await workbook.xlsx.write(res);
    res.end();
  } catch (error) {
    console.error('엑셀 생성 오류:', error);
    res.status(500).send('엑셀 생성 중 오류가 발생했습니다.');
  }
});

// =========================================================================
// [API 6] 프론트엔드 결제 완료 Ping 수신용
// =========================================================================
app.post('/api/trace/purchase', async (req, res) => {
  try {
    const { userId } = req.body;
    if (!userId) return res.status(400).json({ success: false });

    // DB의 'orders' 컬렉션에 도장 찍기 (대시보드에서 PAID 상태를 찾고 있으므로 PAID로 고정)
    await db.collection('orders').insertOne({
      userId: userId,
      status: 'PAID',
      createdAt: new Date()
    });

    res.json({ success: true, message: '결제 트래킹 완료' });
  } catch (err) {
    console.error('결제 핑 수신 에러:', err);
    res.status(500).json({ success: false });
  }
});

// =========================================================================
// [API 7] 관리자 모달용: 카페24 실시간 결제 내역(주문 상품명) 조회
// =========================================================================
app.get('/api/raffle/admin/purchase-detail', async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ success: false });

    const start_date = moment().tz('Asia/Seoul').startOf('month').format('YYYY-MM-DD');
    const end_date = moment().tz('Asia/Seoul').format('YYYY-MM-DD');

    // 💡 결제 내역 조회 시에도 버전을 명시하여 안정성 확보
    const getOrdersFromCafe24 = async (token) => {
      return await axios.get(`https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/orders?member_id=${userId}&start_date=${start_date}&end_date=${end_date}`, {
        headers: { 
          'Authorization': `Bearer ${token.trim()}`, 
          'Content-Type': 'application/json',
          'X-Cafe24-Api-Version': CAFE24_API_VERSION || '2025-12-01'
        }
      });
    };

    let orderRes;
    try {
      orderRes = await getOrdersFromCafe24(accessToken);
    } catch (err) {
      if (err.response && err.response.status === 401) {
        console.log(`[주문조회] 토큰 만료. 재발급 시도...`);
        const newToken = await refreshAccessToken();
        orderRes = await getOrdersFromCafe24(newToken);
      } else throw err;
    }

    const orders = orderRes.data.orders || [];
    let purchaseDetails = [];

    orders.forEach(order => {
      if (order.items && order.items.length > 0) {
        order.items.forEach(item => {
          purchaseDetails.push({
            productName: item.product_name,
            price: Number(item.product_price) || 0, // 💡 [수정됨] 프론트엔드 모달을 위해 결제 금액 추가!
            qty: item.quantity,
            addedAt: order.order_date
          });
        });
      }
    });

    res.json({ success: true, data: purchaseDetails });
  } catch (error) {
    console.error('카페24 주문 연동 에러:', error.response?.data || error.message);
    res.status(500).json({ success: false });
  }
});

// =========================================================================
// [API 8] 스퀴지보 게임 시작 (날짜별 카운트 +1)
// =========================================================================
app.post('/api/squibo/start', async (req, res) => {
  try {
    const today = moment().tz('Asia/Seoul').format('YYYY-MM-DD');
    await db.collection('event_2026_03_Squibo').updateOne(
      { date: today },
      { $inc: { count: 1 } },
      { upsert: true }
    );
    res.json({ success: true, message: '카운트 증가 완료' });
  } catch (error) {
    console.error('스퀴지보 카운트 에러:', error);
    res.status(500).json({ success: false });
  }
});

// =========================================================================
// [API 9] 관리자용: 스퀴지보 게임 날짜별 통계 조회 (대시보드 로드용)
// =========================================================================
app.get('/api/squibo/admin/stats', async (req, res) => {
  try {
    const { startDate, endDate } = req.query;
    let query = {};
    if (startDate || endDate) {
      query.date = {};
      if (startDate) query.date.$gte = startDate;
      if (endDate) query.date.$lte = endDate;
    }

    const stats = await db.collection('event_2026_03_Squibo').find(query).sort({ date: -1 }).toArray();
    res.json({ success: true, data: stats });
  } catch (error) {
    console.error('스퀴지보 통계 조회 에러:', error);
    res.status(500).json({ success: false });
  }
});


// =========================================================================
// [API] 관리자용: 일자별 재고/확률 데이터 조회
// =========================================================================
app.get('/api/raffle/admin/stock', async (req, res) => {
  try {
    const { date } = req.query;
    if (!date) return res.status(400).json({ success: false, message: '날짜 파라미터가 필요합니다.' });

    const dailyData = await db.collection('raffle_daily_stock').findOne({ date: date });
    
    if (dailyData) {
      res.json({ success: true, data: dailyData });
    } else {
      // 해당 날짜에 데이터가 없으면 false를 반환하여 프론트가 기본값을 띄우도록 유도
      res.json({ success: false, message: '설정된 데이터가 없습니다.' });
    }
  } catch (error) {
    console.error('재고 조회 에러:', error);
    res.status(500).json({ success: false });
  }
});

// =========================================================================
// [API] 관리자용: 일자별 재고/확률 데이터 저장 및 수정 (Upsert)
// =========================================================================
app.post('/api/raffle/admin/stock', async (req, res) => {
  try {
    const { date, prizes } = req.body;

    if (!date || !prizes || !Array.isArray(prizes)) {
      return res.status(400).json({ success: false, message: '잘못된 데이터 형식입니다.' });
    }

    // 해당 날짜(date)를 기준으로 데이터를 덮어쓰거나(새로 생성) 업데이트
    await db.collection('raffle_daily_stock').updateOne(
      { date: date },
      { 
        $set: { 
          date: date,
          prizes: prizes, // [{name, totalStock, currentStock, prob}, ...]
          updatedAt: new Date()
        } 
      },
      { upsert: true }
    );

    console.log(`[Admin] ${date} 룰렛 재고/확률 설정 완료`);
    res.json({ success: true, message: '저장 완료' });

  } catch (error) {
    console.error('재고 저장 에러:', error);
    res.status(500).json({ success: false });
  }
});


// =========================================================================
// [API] 10초 디톡스 게임 연동 API
// =========================================================================

// 1. 상태 조회 및 데이터 병합
app.get('/api/game/detox/status', async (req, res) => {
  const { memberId, guestId, ip } = req.query;
  const collection = db.collection('detox_game_users');
  
  try {
    let userDoc = null;
    let showSignupMsg = false;

    // 회원의 경우
    if (memberId && memberId !== 'null' && memberId !== 'GUEST') {
      userDoc = await collection.findOne({ memberId });
      
      // 회원 기록이 없다면, guestId나 ip로 남겨진 이전 기록 찾기
      if (!userDoc) {
        let guestDoc = await collection.findOne({ $or: [{ guestId }, { ip }], memberId: { $exists: false } });
        
        if (guestDoc) {
          // 병합 진행
          await collection.updateOne(
            { _id: guestDoc._id },
            { 
              $set: { 
                memberId: memberId,
                signupRewardGiven: true,
                showSignupMsg: true, // 프론트에서 1회 보여주기 위함
                hearts: (guestDoc.hearts || 0) + 2
              }
            }
          );
          userDoc = await collection.findOne({ _id: guestDoc._id });
        } else {
          // 완전 신규 회원 참가자
          const newDoc = {
            memberId,
            guestId,
            ip,
            hearts: 3, // 기본 1 + 가입 2 = 3
            isSuccess: false,
            signupRewardGiven: true,
            successRewardGiven: false,
            showSignupMsg: false, // 처음부터 회원이면 팝업 없음
            createdAt: new Date()
          };
          const result = await collection.insertOne(newDoc);
          userDoc = { _id: result.insertedId, ...newDoc };
        }
      } else {
        // 기존 회원 기록이 있는 경우 - 가입 보상을 안 받았다면?
        if (!userDoc.signupRewardGiven) {
          await collection.updateOne(
            { _id: userDoc._id },
            { $set: { signupRewardGiven: true, showSignupMsg: true }, $inc: { hearts: 2 } }
          );
          userDoc = await collection.findOne({ _id: userDoc._id });
        }
      }
      
      // 메세지 표시 여부 체크 후 리셋
      if (userDoc.showSignupMsg) {
        showSignupMsg = true;
        await collection.updateOne({ _id: userDoc._id }, { $set: { showSignupMsg: false } });
      }
      
    } else {
      // 비회원의 경우
      if (guestId) {
        userDoc = await collection.findOne({ guestId, memberId: { $exists: false } });
      }
      if (!userDoc) {
        const newDoc = {
          guestId,
          ip,
          hearts: 1, // 비회원 기본 1
          isSuccess: false,
          signupRewardGiven: false,
          successRewardGiven: false,
          showSignupMsg: false,
          createdAt: new Date()
        };
        const result = await collection.insertOne(newDoc);
        userDoc = { _id: result.insertedId, ...newDoc };
      }
    }

    res.json({ 
      success: true, 
      hearts: userDoc.hearts,
      isSuccess: userDoc.isSuccess,
      successRewardGiven: userDoc.successRewardGiven,
      showSignupMsg: showSignupMsg 
    });
  } catch (error) {
    console.error('게임 상태 조회 에러:', error);
    res.status(500).json({ success: false });
  }
});

// 2. 게임 실패 (하트 감소)
app.post('/api/game/detox/fail', async (req, res) => {
  const { memberId, guestId } = req.body;
  const collection = db.collection('detox_game_users');
  
  try {
    let query = {};
    if (memberId && memberId !== 'null' && memberId !== 'GUEST') {
      query = { memberId };
    } else {
      query = { guestId, memberId: { $exists: false } };
    }

    const userDoc = await collection.findOne(query);
    if (!userDoc) {
      return res.status(404).json({ success: false, message: 'User not found' });
    }

    const newHearts = Math.max(0, (userDoc.hearts || 0) - 1);
    await collection.updateOne({ _id: userDoc._id }, { $set: { hearts: newHearts } });

    res.json({ success: true, hearts: newHearts });
  } catch (error) {
    console.error('게임 실패 처리 에러:', error);
    res.status(500).json({ success: false });
  }
});

// 3. 게임 성공
app.post('/api/game/detox/success', async (req, res) => {
  const { memberId, guestId } = req.body;
  const collection = db.collection('detox_game_users');
  
  try {
    let query = {};
    if (memberId && memberId !== 'null' && memberId !== 'GUEST') {
      query = { memberId };
    } else {
      query = { guestId, memberId: { $exists: false } };
    }

    await collection.updateOne(query, { $set: { isSuccess: true } });
    res.json({ success: true });
  } catch (error) {
    console.error('게임 성공 처리 에러:', error);
    res.status(500).json({ success: false });
  }
});


// ========== [9] 서버 초기화 및 시작 (가장 중요) ==========
(async function initialize() {
  const client = new MongoClient(MONGODB_URI); // 옵션 생략 가능

  try {
    // 1. 서버 시작 전 DB 연결 (싱글톤)
    await client.connect();
    db = client.db(DB_NAME);
    console.log("✅ MongoDB Connected (Single Connection)");

    // 2. 토큰 로드
    await getTokensFromDB();

    // 3. 서버 리스닝
    const PORT = process.env.PORT || 6000;
    app.listen(PORT, () => {
      console.log(`Server is running on port ${PORT}`);
    });

  } catch (err) {
    console.error("서버 시작 실패:", err);
  }
})();

