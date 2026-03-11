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
const { translate: googleTranslate } = require('@vitalets/google-translate-api');
// ========== [추가] 일본 요기보 뉴스레터 연동 (RSS/Atom) ==========
const Parser = require('rss-parser');

const multer = require('multer');         
const sharp = require('sharp');             
const ftp = require('basic-ftp');
const { Readable } = require('stream');

const crypto = require('crypto');           
 
// multer 설정 (메모리에 임시 저장)
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 }, // 최대 5MB
  fileFilter: (req, file, cb) => {
    const allowed = ['image/jpeg', 'image/png', 'image/webp', 'image/gif'];
    if (allowed.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error('허용되지 않는 파일 형식입니다. (jpg/png/webp/gif만 가능)'));
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
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, "public")));

// MongoDB 컬렉션명 정의
const tokenCollectionName = "tokens";

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

// 토큰 갱신 함수
async function refreshAccessToken() {
  const now = new Date().toLocaleTimeString();
  console.log(`\n[${now}] 🚨 토큰 갱신 프로세스 시작! (원인: 401 에러 또는 강제 만료)`);
  // ▼ [진단용 코드] 변수 값이 제대로 들어오는지 확인
  console.log('DEBUG CHECK:', {
      CID: process.env.CAFE24_CLIENT_ID, // 이 값이 undefined나 null이면 안됨
      SECRET: process.env.CAFE24_CLIENT_SECRET ? 'EXIST' : 'MISSING'
  });

  try {
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
      console.log(`   - New Access Token: ${newAccessToken.substring(0, 10)}...`);
      
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
  }
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
            } catch (e) {}

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
                } catch (e) {}
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
          $or: [ { visitorId: visitorId }, { userIp: userIp } ],
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
                              { case: { $eq: ["$utmData.campaign", "home_main"] },  then: "브검 : 홈페이지 메인" },
                              { case: { $eq: ["$utmData.campaign", "naver_main"] }, then: "브검 : 1월 말할 수 없는 편안함(메인)" },
                              { case: { $eq: ["$utmData.campaign", "naver_sub1"] }, then: "브검 : 1월 말할 수 없는 편안함(서브1)_10%" },
                              { case: { $eq: ["$utmData.campaign", "naver_sub2"] }, then: "브검 : 1월 말할 수 없는 편안함(서브2)_20%" },
                              { case: { $eq: ["$utmData.campaign", "naver_sub3"] }, then: "브검 : 1월 말할 수 없는 편안함(서브3)_갓생" },
                              { case: { $eq: ["$utmData.campaign", "naver_sub4"] }, then: "브검 : 1월 말할 수 없는 편안함(서브4)_무료배송" },
                              { case: { $eq: ["$utmData.campaign", "naver_sub5"] }, then: "브검 : 1월 말할 수 없는 편안함(서브5)_가까운매장" },
                              { case: { $eq: ["$utmData.content", "employee_discount"] }, then: "메타 : 1월 말할 수 없는 편안함(직원 할인 찬스)" },
                              { case: { $eq: ["$utmData.content", "areading_group1"] },   then: "메타 : 1월 말할 수 없는 편안함(sky독서소파)" },
                              { case: { $eq: ["$utmData.content", "areading_group2"] },   then: "메타 : 1월 말할 수 없는 편안함(sky독서소파2)" },
                              { case: { $eq: ["$utmData.content", "special_price1"] },    then: "메타 : 1월 말할 수 없는 편안함(신년특가1)" },
                              { case: { $eq: ["$utmData.content", "special_price2"] },    then: "메타 : 1월 말할 수 없는 편안함(신년특가2)" },
                              { case: { $eq: ["$utmData.content", "horse"] },             then: "메타 : 1월 말할 수 없는 편안함(말 ai아님)" },
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
          { case: { $eq: ["$utmData.campaign", "home_main"] },  then: "브검 : 홈페이지 메인" },
          { case: { $eq: ["$utmData.campaign", "naver_main"] }, then: "브검 : 1월 말할 수 없는 편안함(메인)" },
          { case: { $eq: ["$utmData.campaign", "naver_sub1"] }, then: "브검 : 1월 말할 수 없는 편안함(서브1)_10%" },
          { case: { $eq: ["$utmData.campaign", "naver_sub2"] }, then: "브검 : 1월 말할 수 없는 편안함(서브2)_20%" },
          { case: { $eq: ["$utmData.campaign", "naver_sub3"] }, then: "브검 : 1월 말할 수 없는 편안함(서브3)_갓생" },
          { case: { $eq: ["$utmData.campaign", "naver_sub4"] }, then: "브검 : 1월 말할 수 없는 편안함(서브4)_무료배송" },
          { case: { $eq: ["$utmData.campaign", "naver_sub5"] }, then: "브검 : 1월 말할 수 없는 편안함(서브5)_가까운매장" },
          { case: { $eq: ["$utmData.content", "employee_discount"] }, then: "메타 : 1월 말할 수 없는 편안함(직원 할인 찬스)" },
          { case: { $eq: ["$utmData.content", "areading_group1"] },   then: "메타 : 1월 말할 수 없는 편안함(sky독서소파)" },
          { case: { $eq: ["$utmData.content", "areading_group2"] },   then: "메타 : 1월 말할 수 없는 편안함(sky독서소파2)" },
          { case: { $eq: ["$utmData.content", "special_price1"] },    then: "메타 : 1월 말할 수 없는 편안함(신년특가1)" },
          { case: { $eq: ["$utmData.content", "special_price2"] },    then: "메타 : 1월 말할 수 없는 편안함(신년특가2)" },
          { case: { $eq: ["$utmData.content", "horse"] },             then: "메타 : 1월 말할 수 없는 편안함(말 ai아님)" },
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

///yogibo.jp rss 피드 


// 1. 피드 파싱 및 DB 자동 저장 로직 (Upsert 방식)
async function fetchAndSaveYogiboJPNews() {
  if (!db) {
    console.log('⚠️ DB 연결이 안 되어있어 뉴스레터 동기화를 보류합니다.');
    return;
  }

  try {
    console.log('🇯🇵 [Yogibo JP] 일본 뉴스레터 업데이트 확인 중...');
    
    // 일본 요기보 Atom 피드 가져오기
    const feed = await parser.parseURL('https://yogibo.jp/blogs/life.atom');
    const collection = db.collection('yogiboJPnews');
    let newCount = 0;

    for (const item of feed.items) {
      // 고유 ID 추출 (id가 없으면 link를 고유 키로 사용)
      const uniqueId = item.id || item.link;

      // $setOnInsert를 사용하여 DB에 없을 때만 'draft' 상태로 새로 추가
      const result = await collection.updateOne(
        { guid: uniqueId }, // 기준 키
        {
          $setOnInsert: {
            guid: uniqueId,
            title: item.title,
            content: item.content, // 일본어 원본 HTML
            link: item.link,
            pubDate: item.pubDate ? new Date(item.pubDate) : new Date(),
            status: 'draft', // 기본 상태는 무조건 '임시저장'
            createdAt: new Date()
          }
        },
        { upsert: true }
      );

      // upsertedId가 있으면 새로 추가된 글이라는 뜻
      if (result.upsertedId) {
        newCount++;
        console.log(`🆕 새 뉴스레터 발견 및 임시저장: ${item.title}`);
      }
    }

    if (newCount === 0) {
      console.log('✅ [Yogibo JP] 새로운 뉴스레터가 없습니다.');
    } else {
      console.log(`✅ [Yogibo JP] 총 ${newCount}개의 새 뉴스레터 동기화 완료`);
    }

  } catch (error) {
    console.error('❌ [Yogibo JP] 뉴스레터 가져오기 실패:', error.message);
  }
}

// 2. 스케줄러 등록 (1시간마다 실행되도록 설정)
cron.schedule('0 * * * *', () => {
  fetchAndSaveYogiboJPNews();
});


// 3. API - 뉴스레터 목록 불러오기 (프론트/관리자 페이지용)
app.get('/api/yogibo-jp-news', async (req, res) => {
  try {
    // 프론트엔드에서는 ?status=published 로 라이브 글만 요청하고,
    // 관리자 페이지에서는 쿼리 없이 전체를 다 가져옵니다.
    const { status } = req.query; 
    const query = status ? { status } : {};

    const newsList = await db.collection('yogiboJPnews')
      .find(query)
      .sort({ pubDate: -1 }) // 최신 발행일 순
      .toArray();

    res.json({ success: true, data: newsList });
  } catch (error) {
    console.error('뉴스레터 조회 에러:', error);
    res.status(500).json({ success: false, message: 'Server Error' });
  }
});

// 4. API - 뉴스레터 번역/내용 수정 및 라이브 발행 (관리자 페이지용)
app.put('/api/yogibo-jp-news/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { title, content, status } = req.body;
    
    const updateData = {};
    if (title) updateData.title = title;
    if (content) updateData.content = content;
    if (status) updateData.status = status; // 'draft' <-> 'published' 변경
    updateData.updatedAt = new Date();

    const result = await db.collection('yogiboJPnews').updateOne(
      { _id: new ObjectId(id) },
      { $set: updateData }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({ success: false, message: '게시글을 찾을 수 없습니다.' });
    }

    res.json({ success: true, message: '게시글이 성공적으로 업데이트되었습니다.' });
  } catch (error) {
    console.error('뉴스레터 업데이트 에러:', error);
    res.status(500).json({ success: false, message: 'Server Error' });
  }
});

// 5. [테스트용] 즉시 동기화 API (스케줄러 1시간 기다리기 답답할 때 호출)
app.get('/api/test/fetch-jp-news', async (req, res) => {
  await fetchAndSaveYogiboJPNews();
  res.json({ success: true, message: '수동 피드 동기화가 실행되었습니다. 서버 로그를 확인하세요.' });
});



// [추가/수정] 구글 무료 번역 API 라우터 (이미지 엑박 방지 적용)
app.post('/api/translate-news', async (req, res) => {
  const { title, content } = req.body;

  try {
    console.log('🔄 구글 번역기로 한글 초벌 번역 중...');

    // 1. 제목 번역
    const titleResult = await googleTranslate(title, { from: 'ja', to: 'ko' });

    // ==========================================
    // 2. 본문 번역 전: <img> 태그 임시 추출 및 마스킹
    // ==========================================
    const imgTags = [];
    // 정규식으로 <img ...> 태그를 모두 찾아서 배열에 넣고, 그 자리는 임시 글자로 치환합니다.
    let maskedContent = content.replace(/<img[^>]*>/gi, (match) => {
      imgTags.push(match);
      return `___IMG_PLACEHOLDER_${imgTags.length - 1}___`; 
    });

    // 3. 마스킹된 안전한 본문만 구글 번역기에 돌립니다.
    const contentResult = await googleTranslate(maskedContent, { from: 'ja', to: 'ko' });
    let translatedHtml = contentResult.text;

    // ==========================================
    // 4. 번역 후: 빼두었던 <img> 태그 원상 복구
    // ==========================================
    // 구글 번역기가 임의로 띄어쓰기를 넣었을 수 있으므로 유연하게 찾아서 원래 이미지로 덮어씌웁니다.
    translatedHtml = translatedHtml.replace(/___\s*IMG_PLACEHOLDER_\s*(\d+)\s*___/gi, (match, index) => {
      return imgTags[parseInt(index)] || match;
    });

    console.log('✅ 번역 완료 (이미지 보호 성공)!');

    res.json({
      success: true,
      translatedTitle: titleResult.text,
      translatedContent: translatedHtml
    });

  } catch (error) {
    console.error('❌ 번역 에러:', error);
    res.status(500).json({ success: false, message: '구글 번역 중 오류가 발생했습니다.' });
  }
});
app.post('/api/yogibo-jp-news/:id/thumbnail-upload', upload.single('file'), async (req, res) => {
  const postId = req.params.id;

  try {
    if (!req.file) return res.status(400).json({ success: false, message: '파일 없음' });

    // 이미지 처리
    const processedBuffer = await sharp(req.file.buffer)
      .resize(800, 500, { fit: 'cover', position: 'center' })
      .webp({ quality: 82 })
      .toBuffer();

    const randomHex = crypto.randomBytes(6).toString('hex');
    const filename = `news-${postId}-${Date.now()}-${randomHex}.webp`;
    const remotePath = `web/img/news/${filename}`; // 슬래시 제거

    // FTP 업로드 (basic-ftp 사용)
    const client = new ftp.Client();
    client.ftp.verbose = true; // 디버그 로그 활성화
    try {
      await client.access({
        host: process.env.FTP_HOST || 'yogibo.ftp.cafe24.com',
        port: process.env.FTP_PORT ? Number(process.env.FTP_PORT) : 21,
        user: process.env.FTP_USER,
        password: process.env.FTP_PASS,
        secure: 'explicit',
      });

      // 디렉토리 존재 확인 및 생성
      await client.ensureDir('web/img/news'); // 슬래시 제거
      console.log('디렉토리 생성 또는 확인 성공');

      // Buffer를 스트림으로 감싸서 업로드
      const stream = Readable.from(processedBuffer);
      await client.uploadFrom(stream, remotePath);
      console.log('파일 업로드 성공:', remotePath);
    } finally {
      client.close();
    }

    // public URL 생성
    const publicUrl = `https://yogibo.cafe24.com/${remotePath}`;

    // DB 업데이트
    const result = await db.collection('yogiboJPnews').updateOne(
      { _id: new ObjectId(postId) },
      { $set: { thumbnail: publicUrl, thumbnailUpdatedAt: new Date() } }
    );
    
    if (result.matchedCount === 0) {
      return res.status(404).json({ success: false, message: '게시글 없음' });
    }

    return res.json({ success: true, url: publicUrl, filename });

  } catch (err) {
    console.error('[FTP Upload Error]', err);
    return res.status(500).json({ success: false, message: err.message || '서버 오류' });
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