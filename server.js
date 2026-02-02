const express = require("express");
const bodyParser = require("body-parser");
const fs = require("fs");
const path = require("path");
const cors = require("cors");
const compression = require("compression");
const axios = require("axios");
const { MongoClient } = require("mongodb");
require("dotenv").config();
const ExcelJS = require('exceljs'); // Excel 파일 생성을 위한 라이브러리
const moment = require('moment-timezone');
// ========== [1] 환경변수 및 기본 설정 ==========
let accessToken = process.env.ACCESS_TOKEN || 'pPhbiZ29IZ9kuJmZ3jr15C';
let refreshToken = process.env.REFRESH_TOKEN || 'CMLScZx0Bh3sIxlFTHDeMD';
const CAFE24_CLIENT_ID = process.env.CAFE24_CLIENT_ID;
const CAFE24_CLIENT_SECRET = process.env.CAFE24_CLIENT_SECRET;
const DB_NAME = process.env.DB_NAME;
const MONGODB_URI = process.env.MONGODB_URI;
const CAFE24_MALLID = process.env.CAFE24_MALLID;  // mall_id가 반드시 설정되어야 함
const OPEN_URL = process.env.OPEN_URL;
const API_KEY = process.env.API_KEY;
const FINETUNED_MODEL = process.env.FINETUNED_MODEL || "gpt-3.5-turbo";
const CAFE24_API_VERSION = process.env.CAFE24_API_VERSION || '2024-06-01';
const CATEGORY_NO = process.env.CATEGORY_NO || 858; // 카테고리 번호 (예: 858)
// ========== [2] Express 앱 기본 설정 ==========
const app = express();
app.use(cors());
app.use(compression());
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, "public")));

// MongoDB에서 토큰을 저장할 컬렉션명
const tokenCollectionName = "tokens";
// ========== [3] MongoDB 토큰 관리 함수 ==========
async function getTokensFromDB() {
  const client = new MongoClient(MONGODB_URI);
  try {
    await client.connect();
    const db = client.db(DB_NAME);
    const collection = db.collection(tokenCollectionName);
    const tokensDoc = await collection.findOne({});
    if (tokensDoc) {
      accessToken = tokensDoc.accessToken;
      refreshToken = tokensDoc.refreshToken;
      console.log('MongoDB에서 토큰 로드 성공:', tokensDoc);
    } else {
      console.log('MongoDB에 저장된 토큰이 없습니다. 초기 토큰을 저장합니다.');
      await saveTokensToDB(accessToken, refreshToken);
    }
  } catch (error) {
    console.error('토큰 로드 중 오류:', error);
  } finally {
    await client.close();
  }
}

async function saveTokensToDB(newAccessToken, newRefreshToken) {
  const client = new MongoClient(MONGODB_URI);
  try {
    await client.connect();
    const db = client.db(DB_NAME);
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
    console.log('MongoDB에 토큰 저장 완료');
  } catch (error) {
    console.error('토큰 저장 중 오류:', error);
  } finally {
    await client.close();
  }
}

async function refreshAccessToken() {
  console.log('401 에러 발생: MongoDB에서 토큰 정보 다시 가져오기...');
  // 기존 토큰 갱신 로직: MongoDB에서 최신 토큰을 다시 불러옴
  await getTokensFromDB();
  console.log('MongoDB에서 토큰 갱신 완료:', accessToken, refreshToken);
  return accessToken;
}
// ========== [4] Cafe24 API 요청 함수 ==========
async function apiRequest(method, url, data = {}, params = {}) {
  console.log(`Request: ${method} ${url}`);
  console.log("Params:", params);
  console.log("Data:", data);
  try {
    const response = await axios({
      method,
      url,
      data,
      params,
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
        'X-Cafe24-Api-Version': CAFE24_API_VERSION
      },
    });
    return response.data;
  } catch (error) {
    if (error.response && error.response.status === 401) {
      // 토큰이 만료된 경우, MongoDB에서 최신 토큰을 불러와 재발급 후 재요청
      console.log('Access Token 만료. 갱신 중...');
      await refreshAccessToken();
      return apiRequest(method, url, data, params);
    } else {
      console.error('API 요청 오류:', error.response ? error.response.data : error.message);
      throw error;
    }
  }
}


//럭키 드로우 이벤트 추가 
/**
 * 예시: member_id를 기반으로 고객 데이터를 가져오기
 */
async function getCustomerDataByMemberId(memberId) {
  // 무조건 MongoDB에서 토큰을 로드하여 사용
  await getTokensFromDB();
  // MALLID 대신 CAFE24_MALLID를 사용합니다.
  const url = `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/customersprivacy`;
  const params = { member_id: memberId };
  try {
    const data = await apiRequest('GET', url, {}, params);
    console.log('Customer Data:', JSON.stringify(data, null, 2));
    return data;
  } catch (error) {
    console.error(`Error fetching customer data for member_id ${memberId}:`, error);
    throw error;
  }
}


// MongoDB 연결 및 Express 서버 설정 (이벤트 참여 데이터 저장)
const clientInstance = new MongoClient(MONGODB_URI, { useUnifiedTopology: true });
clientInstance.connect()
  .then(() => {
    console.log('MongoDB 연결 성공');
    const db = clientInstance.db(DB_NAME);
    const entriesCollection = db.collection('entries');
    
    // 참여자 수 반환 라우트 (entriesCollection 사용)
    app.get('/api/entry/count', async (req, res) => {
      try {
        const count = await entriesCollection.countDocuments();
        res.json({ count });
      } catch (error) {
        console.error('참여자 수 가져오기 오류:', error);
        res.status(500).json({ error: '서버 내부 오류' });
      }
    });
    
    app.post('/api/entry', async (req, res) => {
      const { memberId } = req.body;
      if (!memberId) {
        return res.status(400).json({ error: 'memberId 값이 필요합니다.' });
      }
      try {
        // 고객 데이터 가져오기 (권한 부여 포함)
        const customerData = await getCustomerDataByMemberId(memberId);
        if (!customerData || !customerData.customersprivacy) {
          return res.status(404).json({ error: '고객 데이터를 찾을 수 없습니다.' });
        }
        
        // customersprivacy가 배열인 경우 첫 번째 항목 선택
        let customerPrivacy = customerData.customersprivacy;
        if (Array.isArray(customerPrivacy)) {
          customerPrivacy = customerPrivacy[0];
        }
        
        // 필요한 필드 추출: member_id, name, cellphone, email, address1, address2, sms, gender
        const { member_id, name, cellphone, email, address1, address2, sms, gender } = customerPrivacy;
        
        // 중복 참여 확인
        const existingEntry = await entriesCollection.findOne({ memberId: member_id });
        if (existingEntry) {
          return res.status(409).json({ message: '' });
        }
        
        // 한국 시간 기준 날짜 생성
        const createdAtKST = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
        
        // 저장할 객체 생성 (address1과 address2 모두 저장, 고객 성함(name) 추가)
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
    
        const result = await entriesCollection.insertOne(newEntry);
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
    
    app.get('/api/lucky/download', async (req, res) => {
      try {
        const entries = await entriesCollection.find({}).toArray();
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
          // address1과 address2 합치기 (address2가 있을 경우)
          const fullAddress = entry.address1 + (entry.address2 ? ' ' + entry.address2 : '');
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
  })
  .catch(err => {
    console.error('MongoDB 연결 실패:', err);
  });


  





// ==============================
// (1) 개인정보 수집·이용 동의(선택) 업데이트
async function updatePrivacyConsent(memberId) {
  const url = `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/privacyconsents`;
  const payload = {
    shop_no: 1,
    request: {
      member_id:   memberId,
      consent_type:'marketing',
      agree:       'T',
      issued_at:   new Date().toISOString()
    }
  };
  try {
    return await apiRequest('POST', url, payload);
  } catch (err) {
    if (err.response?.data?.error?.message.includes('No API found')) {
      console.warn('privacyconsents 엔드포인트 미지원, 패스');
      return;
    }
    throw err;
  }
}

// ==============================
// (2) SMS 수신동의 업데이트
async function updateMarketingConsent(memberId) {
  const url = `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/customersprivacy/${memberId}`;
  const payload = {
    request: {
      shop_no:   1,
      member_id: memberId,
      sms:       'T'
    }
  };
  return apiRequest('PUT', url, payload);
}


// ==============================
// (4) 매장용 이벤트 참여 엔드포인트
app.post('/api/event/marketing-consent', async (req, res) => {
  const { memberId, store } = req.body;
  if (!memberId || !store) {
    return res.status(400).json({ error: 'memberId와 store가 필요합니다.' });
  }

  const client = new MongoClient(MONGODB_URI);
  try {
    await client.connect();
    const coll = client.db(DB_NAME).collection('marketingConsentEvent');

    // 중복 참여 방지
    if (await coll.findOne({ memberId })) {
      return res.status(409).json({ success: false, message: '이미 참여 완료하신 고객입니다.' });
    }

    // SMS 수신동의 업데이트
    await updateMarketingConsent(memberId);

    // 참여 기록 저장
    const seoulNow = new Date(
      new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' })
    );
    await coll.insertOne({ memberId, store, participatedAt: seoulNow });

    res.json({ success: true, message: '참여 완료!' });
  } catch (err) {
    console.error('이벤트 처리 오류:', err.response?.data || err.message);
    res.status(500).json({ error: '서버 오류가 발생했습니다.' });
  } finally {
    await client.close();
  }
});

// ==============================
// (5) 자사몰용 이벤트 참여 엔드포인트
app.post('/api/event/marketing-consent-company', async (req, res) => {
  const { memberId } = req.body;
  if (!memberId) {
    return res.status(400).json({ error: 'memberId가 필요합니다.' });
  }

  const client = new MongoClient(MONGODB_URI);
  try {
    await client.connect();
    const coll = client.db(DB_NAME).collection('marketingConsentCompanyEvent');

    // 중복 참여 방지
    if (await coll.findOne({ memberId })) {
      return res.status(409).json({ message: '이미 참여하셨습니다.' });
    }

    // 1) 개인정보 동의
    await updatePrivacyConsent(memberId);
    // 2) SMS 수신동의
    await updateMarketingConsent(memberId);
    // 3) 적립금 지급
    await giveRewardPoints(memberId, 5000, '자사몰 마케팅 수신동의 이벤트 보상');

    // 4) 지급 기록 저장
    const seoulNow = new Date(
      new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' })
    );
    await coll.insertOne({ memberId, rewardedAt: seoulNow });

    res.json({ success: true, message: '적립금 지급 완료!' });
  } catch (err) {
    console.error('자사몰 이벤트 처리 오류:', err.response?.data || err.message);
    res.status(500).json({ error: '서버 오류가 발생했습니다.' });
  } finally {
    await client.close();
  }
});

// ==============================
// (7) 자사몰용 참여 내역 엑셀 다운로드
app.get('/api/event/marketing-consent-company-export', async (req, res) => {
  const client = new MongoClient(MONGODB_URI);
  try {
    await client.connect();
    const coll = client.db(DB_NAME).collection('marketingConsentCompanyEvent');
    const docs = await coll.find({})
      .project({ _id: 0, rewardedAt: 1, memberId: 1 })
      .toArray();

    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet('자사몰 참여 내역');

    ws.columns = [
      { header: '참여 날짜', key: 'rewardedAt', width: 25 },
      { header: '회원 아이디', key: 'memberId',    width: 20 },
    ];

    docs.forEach(d => {
      ws.addRow({
        rewardedAt: d.rewardedAt.toLocaleString('ko-KR'),
        memberId:   d.memberId
      });
    });

    const companyFilename = '자사몰_참여_내역.xlsx';
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="company_export.xlsx"; filename*=UTF-8''${encodeURIComponent(companyFilename)}`
    );
    res.setHeader(
      'Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    );

    await wb.xlsx.write(res);
    res.end();
  } catch (err) {
    console.error(err);
    res.status(500).send('엑셀 생성 중 오류가 발생했습니다.');
  } finally {
    await client.close();
  }
});


// ==========================================================
// [2월 이벤트] 매일 출석체크 & 누적 보상 시스템
// ==========================================================

// 1. 이벤트 상태 조회 (초기 진입 시 호출)
// - 누적 참여 횟수(count)
// - 오늘 참여 여부(todayDone)
// - 마케팅 수신동의 여부(sms, email) 반환
app.get('/api/event/status', async (req, res) => {
  const { memberId } = req.query;

  if (!memberId) {
    return res.status(400).json({ success: false, message: 'memberId가 필요합니다.' });
  }

  const client = new MongoClient(MONGODB_URI);
  
  try {
    await client.connect();
    const db = client.db(DB_NAME);
    
    // [1] DB에서 참여 기록 조회
    const eventDoc = await db.collection('event_daily_checkin').findOne({ memberId });
    
    let myCount = 0;
    let isTodayDone = false;

    if (eventDoc) {
      myCount = eventDoc.count || 0;
      
      // 마지막 참여 날짜가 '오늘(한국시간)'인지 확인
      const lastDate = moment(eventDoc.lastParticipatedAt).tz('Asia/Seoul');
      const today = moment().tz('Asia/Seoul');
      
      if (lastDate.isSame(today, 'day')) {
        isTodayDone = true;
      }
    }

    // [2] Cafe24 API로 실시간 마케팅 동의 여부 조회
    // (기존 apiRequest 함수 활용)
    const cafe24Url = `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/customers?member_id=${memberId}&fields=sms,news_mail`;
    let smsConsent = 'F';
    let emailConsent = 'F';

    try {
      const cafe24Res = await apiRequest('GET', cafe24Url);
      if (cafe24Res.customers && cafe24Res.customers.length > 0) {
        smsConsent = cafe24Res.customers[0].sms;
        emailConsent = cafe24Res.customers[0].news_mail;
      }
    } catch (apiErr) {
      console.error('Cafe24 회원정보 조회 실패(이벤트):', apiErr.message);
      // API 실패해도 이벤트 참여 정보는 줘야 하므로 기본값 'F' 유지하고 진행
    }

    // [3] 최종 응답
    res.json({
      success: true,
      count: myCount,      // 현재 누적 횟수 (0, 1, 2 ...)
      todayDone: isTodayDone, // 오늘 참여 했는지 (true/false)
      sms: smsConsent,     // SMS 동의 여부
      email: emailConsent  // 이메일 동의 여부
    });

  } catch (err) {
    console.error('이벤트 상태 조회 에러:', err);
    res.status(500).json({ success: false, message: '서버 에러' });
  } finally {
    await client.close();
  }
});


// 2. 이벤트 참여하기 (버튼 클릭 시 호출)
// - 1일 1회 체크
// - 카운트 +1 증가
app.post('/api/event/participate', async (req, res) => {
  const { memberId } = req.body;

  if (!memberId) {
    return res.status(400).json({ success: false, message: '로그인이 필요합니다.' });
  }

  const client = new MongoClient(MONGODB_URI);

  try {
    await client.connect();
    const db = client.db(DB_NAME);
    const collection = db.collection('event_daily_checkin');

    // [1] 기존 기록 조회
    const eventDoc = await collection.findOne({ memberId });
    
    const nowKST = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
    const todayMoment = moment(nowKST).tz('Asia/Seoul');

    // [2] 오늘 이미 참여했는지 체크
    if (eventDoc) {
      const lastDate = moment(eventDoc.lastParticipatedAt).tz('Asia/Seoul');
      
      if (lastDate.isSame(todayMoment, 'day')) {
        return res.json({ success: false, message: '오늘 이미 출석체크를 완료하셨습니다.' });
      }
    }

    // [3] 데이터 업데이트 (Upsert)
    // - 없으면 생성(count: 1), 있으면 count + 1
    // - history 배열에 참여 시간 기록
    const updateResult = await collection.findOneAndUpdate(
      { memberId: memberId },
      { 
        $inc: { count: 1 },                // 횟수 1 증가
        $set: { lastParticipatedAt: nowKST }, // 마지막 참여 시간 갱신
        $push: { history: nowKST },           // 이력 저장
        $setOnInsert: { firstParticipatedAt: nowKST } // 처음일 때만 생성일 저장
      },
      { upsert: true, returnDocument: 'after' } // 업데이트 후의 최신 문서 반환
    );

    // 업데이트된 최신 카운트 가져오기
    // (MongoDB 드라이버 버전에 따라 returnDocument 구조가 다를 수 있어 안전하게 처리)
    const updatedDoc = updateResult.value || updateResult; 
    const newCount = updatedDoc ? updatedDoc.count : (eventDoc ? eventDoc.count + 1 : 1);

    console.log(`[이벤트 참여] ${memberId}님 ${newCount}회차 출석 완료`);

    res.json({
      success: true,
      message: '출석체크가 완료되었습니다!',
      count: newCount
    });

  } catch (err) {
    console.error('이벤트 참여 처리 에러:', err);
    res.status(500).json({ success: false, message: '서버 에러가 발생했습니다.' });
  } finally {
    await client.close();
  }
});


// ==========================================================
// [API 1] 로그 수집 (최종: IP차단 + Dev예외 + 재방문 로직)
// ==========================================================
app.post('/api/trace/log', async (req, res) => {
  try {
      // --------------------------------------------------------
      // 1. IP 확인 및 차단 필터 (개발자 예외 적용)
      // --------------------------------------------------------
      let userIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '';
      if (userIp.includes(',')) userIp = userIp.split(',')[0].trim();

      // 차단할 IP 리스트 (사무실 등 공용 IP)
      const BLOCKED_IPS = ['127.0.0.1', '61.99.75.10']; 
      
      // 프론트에서 보낸 '나 개발자야(isDev)' 신호 받기
      const { isDev } = req.body; 

      // ★ IP가 차단 목록에 있어도, isDev가 true면 통과 / false면 차단
      if (BLOCKED_IPS.includes(userIp) && !isDev) {
          return res.json({ success: true, msg: 'IP Filtered' });
      }

      // --------------------------------------------------------
      // 2. 요청 데이터 파싱
      // --------------------------------------------------------
      let { eventTag, visitorId, currentUrl, prevUrl, utmData, deviceType } = req.body;

      console.log('[LOG] 요청:', { 
          visitorId, 
          currentUrl: currentUrl?.substring(0, 50), 
          userIp,
          isDev // 디버깅용 확인
      });

      const isRealMember = visitorId && !/guest_/i.test(visitorId) && visitorId !== 'null';

      // --------------------------------------------------------
      // 3. 회원 병합 로직 (로그인 시)
      // --------------------------------------------------------
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

      // --------------------------------------------------------
      // 4. 게스트 ID 재사용 로직 (비회원)
      // --------------------------------------------------------
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

      // --------------------------------------------------------
      // 5. ★ [핵심] 세션 유지 및 재방문(Retention) 판별
      // --------------------------------------------------------
      let isNewSession = true;
      let skipReason = null;
      let isRevisit = false; 

      if (visitorId) {
          // 가장 최근 로그 1개 조회
          const lastLog = await db.collection('visit_logs1Event').findOne(
              { visitorId: visitorId },
              { sort: { createdAt: -1 } }
          );

          if (lastLog) {
              const timeDiff = Date.now() - new Date(lastLog.createdAt).getTime();
              const SESSION_TIMEOUT = 30 * 60 * 1000; // 30분

              // [A] 중복 저장 방지 (2분 이내 + 동일 URL)
              if (timeDiff < 2 * 60 * 1000 && lastLog.currentUrl === currentUrl) {
                  skipReason = 'Duplicate (same URL within 2min)';
              }

              // [B] 세션 판별
              if (timeDiff < SESSION_TIMEOUT) {
                  // === 세션 유지 중 (페이지 이동/새로고침) ===
                  isNewSession = false;
                  
                  // ★ 중요: 세션이 유지되는 동안은 재방문 여부를 새로 계산하지 않고
                  // 직전 로그의 상태를 그대로 물려받습니다. (Inherit)
                  isRevisit = lastLog.isRevisit || false; 
                  
              } else {
                  // === 새로운 세션 시작 (30분 경과 후 재접속) ===
                  isNewSession = true;
                  
                  // ★ 이때만 "과거(24시간 전)에 방문한 적 있는가?"를 체크합니다.
                  const pastLog = await db.collection('visit_logs1Event').findOne({
                      visitorId: visitorId,
                      createdAt: { $lt: new Date(Date.now() - 24 * 60 * 60 * 1000) } 
                  });

                  if (pastLog) {
                      isRevisit = true; // 24시간 전 기록 있음 -> 재방문 유저
                      console.log(`[REVISIT] 재방문 유저 확인: ${visitorId}`);
                  } else {
                      isRevisit = false; // 24시간 전 기록 없음 -> 신규 또는 하루 내 재접속
                  }
              }
          } else {
              // 로그가 아예 없음 -> 완전 신규
              isRevisit = false;
          }
      }

      if (skipReason) {
          console.log(`[SKIP] ${skipReason}`);
          return res.json({ success: true, msg: skipReason });
      }

      // --------------------------------------------------------
      // 6. 진입점(Entry Point) 체크
      // --------------------------------------------------------
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

      // --------------------------------------------------------
      // 7. 최종 저장
      // --------------------------------------------------------
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
          isRevisit: isRevisit, // 계산된 재방문 값 저장
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


// ==========================================================
// [API 1-1] 체류 시간 업데이트
// ==========================================================
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

// ==========================================================
// [API 2] 관리자 대시보드용: 단순 태그별 요약
// ==========================================================
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
// ==========================================================
// [API 3] 방문자 목록 조회 (수정: searchId 필드 추가)
// ==========================================================
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
                  // 회원은 visitorId, 비회원은 IP로 그룹핑
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
              // ★ [추가] 프론트에서 사용할 검색용 ID 명시
              $addFields: {
                  searchId: "$_id"  // Journey API 호출 시 사용할 ID
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
// ==========================================================
// [API 4] 특정 유저 이동 경로 (수정: 회원/비회원 분리)
// ==========================================================
app.get('/api/trace/journey/:visitorId', async (req, res) => {
  const { visitorId } = req.params;
  const { startDate, endDate } = req.query;

  console.log('[Journey] 요청:', { visitorId, startDate, endDate });

  try {
      // ==========================================================
      // [1] 날짜 필터링 준비
      // ==========================================================
      let dateFilter = null;
      
      if (startDate) {
          const start = new Date(startDate + 'T00:00:00.000Z');
          const end = endDate 
              ? new Date(endDate + 'T23:59:59.999Z') 
              : new Date(startDate + 'T23:59:59.999Z');
          
          dateFilter = { $gte: start, $lte: end };
      }

      // ==========================================================
      // [2] ★ 검색 대상이 IP인지, 회원ID인지, 게스트ID인지 판단
      // ==========================================================
      const isIpFormat = /^(\d{1,3}\.){3}\d{1,3}$/.test(visitorId) || visitorId.includes(':');
      const isGuestId = visitorId.toLowerCase().startsWith('guest_');
      const isMemberId = !isIpFormat && !isGuestId;

      let baseQuery = {};
      let clickQuery = {};

      // ==========================================================
      // [3] ★ 케이스별 쿼리 생성 (핵심 수정)
      // ==========================================================
      
      if (isMemberId) {
          // ★ 케이스 1: 회원 ID로 검색 → 해당 회원 기록만!
          console.log('[Journey] 회원 ID로 검색:', visitorId);
          baseQuery = { visitorId: visitorId };
          clickQuery = { visitorId: visitorId };
      } 
      else if (isIpFormat) {
          // ★ 케이스 2: IP로 검색 (비회원 목록에서 클릭) → 해당 IP의 게스트 기록만!
          console.log('[Journey] IP로 검색 (게스트만):', visitorId);
          baseQuery = { 
              userIp: visitorId,
              visitorId: { $regex: /^guest_/i }  // ★ 게스트만!
          };
          clickQuery = { 
              ip: visitorId,
              visitorId: { $regex: /^guest_/i }  // ★ 게스트만!
          };
      }
      else if (isGuestId) {
          // ★ 케이스 3: 게스트 ID로 검색 → 해당 게스트 + 같은 IP의 다른 게스트
          console.log('[Journey] 게스트 ID로 검색:', visitorId);
          
          // 먼저 이 게스트의 IP 찾기
          const guestLog = await db.collection('visit_logs1Event').findOne(
              { visitorId: visitorId },
              { projection: { userIp: 1 } }
          );
          
          if (guestLog && guestLog.userIp) {
              // 같은 IP의 게스트 기록들만 (회원 제외!)
              baseQuery = {
                  userIp: guestLog.userIp,
                  visitorId: { $regex: /^guest_/i }  // ★ 게스트만!
              };
              clickQuery = {
                  ip: guestLog.userIp,
                  visitorId: { $regex: /^guest_/i }  // ★ 게스트만!
              };
          } else {
              // IP 못 찾으면 해당 게스트 ID만
              baseQuery = { visitorId: visitorId };
              clickQuery = { visitorId: visitorId };
          }
      }

      // ==========================================================
      // [4] 날짜 조건 추가
      // ==========================================================
      if (dateFilter) {
          baseQuery = { $and: [baseQuery, { createdAt: dateFilter }] };
          clickQuery = { $and: [clickQuery, { createdAt: dateFilter }] };
      }

      console.log('[Journey] 방문 쿼리:', JSON.stringify(baseQuery));

      // ==========================================================
      // [5] 방문 기록 조회
      // ==========================================================
      const views = await db.collection('visit_logs1Event')
          .find(baseQuery)
          .sort({ createdAt: 1 })
          .project({ currentUrl: 1, createdAt: 1, visitorId: 1, _id: 0 })
          .toArray();

      console.log('[Journey] 방문 기록:', views.length, '건');

      const formattedViews = views.map(v => ({
          type: 'VIEW',
          title: v.currentUrl,
          url: v.currentUrl,
          timestamp: v.createdAt
      }));

      // ==========================================================
      // [6] 클릭 기록 조회
      // ==========================================================
      const clicks = await db.collection('event01ClickData')
          .find(clickQuery)
          .sort({ createdAt: 1 })
          .project({ sectionName: 1, sectionId: 1, createdAt: 1, _id: 0 })
          .toArray();

      console.log('[Journey] 클릭 기록:', clicks.length, '건');

      const formattedClicks = clicks.map(c => ({
          type: 'CLICK',
          title: `👉 [클릭] ${c.sectionName}`,
          url: '',
          timestamp: c.createdAt
      }));

      // 7. 합치기 및 정렬
      const journey = [...formattedViews, ...formattedClicks];
      journey.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

      res.json({ success: true, journey });

  } catch (error) {
      console.error('[Journey Error]', error);
      res.status(500).json({ msg: 'Server Error', error: error.message });
  }
});

// ==========================================================
// [API 5] 퍼널 분석 (수정: 1월 신규 UTM 매핑 적용)
// ==========================================================
app.get('/api/trace/funnel', async (req, res) => {
  try {
      const { startDate, endDate } = req.query;

      let dateFilter = {};
      if (startDate || endDate) {
          dateFilter = {};
          if (startDate) dateFilter.$gte = new Date(startDate + "T00:00:00.000Z");
          if (endDate) dateFilter.$lte = new Date(endDate + "T23:59:59.999Z");
      }

      // 유효 방문자 추출
      const validVisitors = await db.collection('visit_logs1Event').distinct('visitorId', {
          createdAt: dateFilter,
          currentUrl: { $regex: '1_promotion.html|index.html|store.html' } // UTM 랜딩이 다양해져서 조건 확장
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
                  // ★ [수정됨] 1월 신규 UTM 매핑 로직 (이미지 기준)
                  channelName: {
                      $switch: {
                          branches: [
                              // 1. 네이버 브랜드 검색 (Campaign 기준)
                              { case: { $eq: ["$utmData.campaign", "home_main"] },  then: "브검 : 홈페이지 메인" },
                              { case: { $eq: ["$utmData.campaign", "naver_main"] }, then: "브검 : 1월 말할 수 없는 편안함(메인)" },
                              { case: { $eq: ["$utmData.campaign", "naver_sub1"] }, then: "브검 : 1월 말할 수 없는 편안함(서브1)_10%" },
                              { case: { $eq: ["$utmData.campaign", "naver_sub2"] }, then: "브검 : 1월 말할 수 없는 편안함(서브2)_20%" },
                              { case: { $eq: ["$utmData.campaign", "naver_sub3"] }, then: "브검 : 1월 말할 수 없는 편안함(서브3)_갓생" },
                              { case: { $eq: ["$utmData.campaign", "naver_sub4"] }, then: "브검 : 1월 말할 수 없는 편안함(서브4)_무료배송" },
                              { case: { $eq: ["$utmData.campaign", "naver_sub5"] }, then: "브검 : 1월 말할 수 없는 편안함(서브5)_가까운매장" },

                              // 2. 메타 광고 (Content 기준)
                              { case: { $eq: ["$utmData.content", "employee_discount"] }, then: "메타 : 1월 말할 수 없는 편안함(직원 할인 찬스)" },
                              { case: { $eq: ["$utmData.content", "areading_group1"] },   then: "메타 : 1월 말할 수 없는 편안함(sky독서소파)" },
                              { case: { $eq: ["$utmData.content", "areading_group2"] },   then: "메타 : 1월 말할 수 없는 편안함(sky독서소파2)" },
                              { case: { $eq: ["$utmData.content", "special_price1"] },    then: "메타 : 1월 말할 수 없는 편안함(신년특가1)" },
                              { case: { $eq: ["$utmData.content", "special_price2"] },    then: "메타 : 1월 말할 수 없는 편안함(신년특가2)" },
                              { case: { $eq: ["$utmData.content", "horse"] },             then: "메타 : 1월 말할 수 없는 편안함(말 ai아님)" },

                              // 3. 카카오 플친 (Campaign 기준)
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




// ==========================================================
// [API] Cafe24 카테고리 전체 정보 조회 (무한 스크롤링 방식)
// ==========================================================
app.get('/api/meta/categories', async (req, res) => {
  const url = `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/categories`;
  
  try {
      let allCategories = [];
      let offset = 0;
      let hasMore = true;
      const LIMIT = 100; // API가 허용하는 최대값

      console.log(`[Category] 카테고리 전체 데이터 수집 시작...`);

      // ★ [핵심] 100개씩 끊어서 끝까지 다 가져오는 루프
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
                  offset: offset,   // 0, 100, 200... 식으로 증가
                  fields: 'category_no,category_name' 
              }
          });

          const cats = response.data.categories;
          
          if (cats && cats.length > 0) {
              allCategories = allCategories.concat(cats);
              
              // 가져온 개수가 100개 미만이면 거기가 마지막 페이지임
              if (cats.length < LIMIT) {
                  hasMore = false; 
              } else {
                  offset += LIMIT; // 다음 100개를 가지러 감
              }
          } else {
              // 데이터가 비어있으면 종료
              hasMore = false;
          }
      }

      // 프론트엔드용 매핑 데이터 생성 { '1017': '요기보 서포트...' }
      const categoryMap = {};
      allCategories.forEach(cat => {
          categoryMap[cat.category_no] = cat.category_name;
      });

      console.log(`[Category] 총 ${allCategories.length}개의 카테고리 로드 완료`);
      res.json({ success: true, data: categoryMap });

  } catch (error) {
      // 토큰 만료 처리
      if (error.response && error.response.status === 401) {
          try {
              console.log('Token expired. Refreshing...');
              await refreshAccessToken();
              return res.redirect(req.originalUrl); // 재시도
          } catch (e) {
              return res.status(401).json({ error: "Token refresh failed" });
          }
      }
      console.error("카테고리 전체 조회 실패:", error.message);
      res.status(500).json({ success: false, message: 'Server Error' });
  }
});


// ==========================================================
// [신규 API] Cafe24 전체 상품 정보 조회 (상품명 매핑용)
// ==========================================================
app.get('/api/meta/products', async (req, res) => {
  const url = `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/products`;
  
  try {
      let allProducts = [];
      let offset = 0;
      let hasMore = true;
      const LIMIT = 100; // 한 번에 가져올 최대 개수

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
                  // ★ 중요: 무거운 정보 빼고 번호랑 이름만 가져와서 속도 최적화
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

      // 프론트엔드용 매핑 데이터 생성 { '1258': '요기보 맥스' }
      const productMap = {};
      allProducts.forEach(prod => {
          productMap[prod.product_no] = prod.product_name;
      });

      console.log(`[Product] 총 ${allProducts.length}개의 상품 정보 로드 완료`);
      res.json({ success: true, data: productMap });

  } catch (error) {
      // 토큰 만료 처리
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







// ==========================================================
// [API 7] 섹션 클릭 로그 저장 (수정됨: visitorId 저장 추가)
// ==========================================================
app.post('/api/trace/click', async (req, res) => {
  try {
      // 1. IP 가져오기
      let userIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '';
      if (userIp.includes(',')) {
          userIp = userIp.split(',')[0].trim();
      }

      // IP 차단 로직
      const BLOCKED_IPS = ['127.0.0.1', '::1'];
      if (BLOCKED_IPS.includes(userIp)) {
          return res.json({ success: true, msg: 'IP Filtered' });
      }

      // ★ [수정 1] req.body에서 visitorId를 받아와야 함!
      const { sectionId, sectionName, visitorId } = req.body;

      if (!sectionId || !sectionName) {
          return res.status(400).json({ success: false, msg: 'Missing Data' });
      }

      // 2. DB 저장 객체 생성
      const clickLog = {
          sectionId,
          sectionName,
          // ★ [수정 2] visitorId가 있으면 저장 (없으면 guest)
          visitorId: visitorId || 'guest', 
          ip: userIp,
          createdAt: new Date()
      };

      // ★ [수정 3] Collection 이름을 'event01ClickData'로 통일 (읽는 쪽과 맞춰야 함)
      await db.collection('event01ClickData').insertOne(clickLog);
      
      res.json({ success: true });

  } catch (e) {
      console.error(e);
      res.status(500).json({ success: false });
  }
});



// ==========================================================
// [API 8] 섹션 클릭 통계 조회 (날짜 필터링 적용)
// ==========================================================
app.get('/api/trace/clicks/stats', async (req, res) => {
  try {
      const { startDate, endDate } = req.query;
      
      // ★ [핵심] 날짜 필터링 조건 생성
      let matchStage = {};
      if (startDate || endDate) {
          matchStage.createdAt = {};
          // 시작일 00:00:00 부터
          if (startDate) matchStage.createdAt.$gte = new Date(startDate + "T00:00:00.000Z");
          // 종료일 23:59:59 까지
          if (endDate) matchStage.createdAt.$lte = new Date(endDate + "T23:59:59.999Z");
      }

      // DB 집계 (기간 조건 -> 그룹핑 -> 카운트)
      const stats = await db.collection('event01ClickData').aggregate([
          { $match: matchStage },     // 1. 날짜로 먼저 거르기
          {
              $group: {
                  _id: "$sectionId",                
                  name: { $first: "$sectionName" }, 
                  count: { $sum: 1 }                
              }
          },
          { $sort: { count: -1 } }    // 2. 많은 순 정렬
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
// ==========================================================
// [API] 특정 버튼 클릭 사용자 조회 (수정됨: 클릭 DB 직접 조회로 누락 방지)
// ==========================================================
app.get('/api/trace/visitors/by-click', async (req, res) => {
  try {
      const { sectionId, startDate, endDate } = req.query;
      
      // 1. 날짜 범위 설정
      const start = startDate ? new Date(startDate + 'T00:00:00.000Z') : new Date(0);
      const end = endDate ? new Date(endDate + 'T23:59:59.999Z') : new Date();

      // 2. 클릭 로그 조회 (여기서 직접 유저 리스트를 만듭니다)
      const clickLogs = await db.collection('event01ClickData').find({
          sectionId: sectionId,
          createdAt: { $gte: start, $lte: end }
      }).sort({ createdAt: -1 }).toArray(); // 최신순 정렬

      if (clickLogs.length === 0) {
          return res.json({ success: true, visitors: [], msg: '클릭 기록 없음' });
      }

      // 3. 중복 제거 및 데이터 포맷팅
      const uniqueVisitors = {};
      
      for (const log of clickLogs) {
          const vid = log.visitorId || log.ip || 'Unknown'; // ID 없으면 IP라도 사용
          
          // 이미 리스트에 없으면 추가 (최신 클릭 기준)
          if (!uniqueVisitors[vid]) {
              uniqueVisitors[vid] = {
                  _id: vid,
                  lastAction: log.createdAt,
                  // guest_로 시작하거나 null이면 비회원, 아니면 회원
                  isMember: (vid && !vid.startsWith('guest_') && vid !== 'null' && vid !== 'guest'),
                  currentUrl: '', // 클릭 로그엔 URL이 없을 수 있음
                  userIp: log.ip,
                  count: 1 // 클릭 횟수
              };
          } else {
              uniqueVisitors[vid].count++; // 이미 있으면 카운트 증가
          }
      }

      // 4. 배열로 변환
      const visitors = Object.values(uniqueVisitors);

      res.json({ success: true, visitors: visitors });

  } catch (error) {
      console.error('클릭 방문자 조회 실패:', error);
      res.status(500).json({ success: false, message: '서버 오류' });
  }
});

// ==========================================================
// [API 9] 인기 페이지 및 방문자 그룹핑 조회 (핵심 기능)
// ==========================================================
app.get('/api/trace/stats/pages', async (req, res) => {
  try {
    const { startDate, endDate } = req.query;
    let matchStage = {};

    // 날짜 필터링
    if (startDate || endDate) {
        matchStage.createdAt = {};
        if (startDate) matchStage.createdAt.$gte = new Date(startDate + "T00:00:00.000Z");
        if (endDate) matchStage.createdAt.$lte = new Date(endDate + "T23:59:59.999Z");
    }

    // URL별 그룹핑 -> 방문자 ID 수집 (중복 제거)
    const pipeline = [
      { $match: matchStage },
      {
        $group: {
          _id: "$currentUrl", // URL 기준으로 묶음
          count: { $sum: 1 }, // 단순 조회수
          visitors: { $addToSet: "$visitorId" } // 방문자 ID 리스트 (중복제거됨)
        }
      },
      { 
        $project: {
            url: "$_id",
            count: 1,
            visitors: 1,
            visitorCount: { $size: "$visitors" } // 고유 방문자 수
        }
      },
      { $sort: { count: -1 } }, // 조회수 높은 순 정렬
      { $limit: 100 } // 상위 100개만 (성능 위해)
    ];

    // 메모리 부족 방지 옵션 포함
    const data = await db.collection('visit_logs1Event').aggregate(pipeline, { allowDiskUse: true }).toArray();
    res.json({ success: true, data });

  } catch (err) {
    console.error(err);
    res.status(500).json({ msg: 'Server Error' });
  }
});
// ==========================================================
// [API 10] 카테고리 -> 상품 이동 흐름 분석 (목록간 이동 제외, 순수 상품만)
// ==========================================================
app.get('/api/trace/stats/flow', async (req, res) => {
  try {
    const { startDate, endDate } = req.query;
    
    // ★ 핵심 수정: 현재 페이지(currentUrl)는 상품이어야 함
    // Cafe24에서 list.html은 목록이므로, product가 들어있더라도 list.html은 제외해야 함!
    let matchStage = {
        // 1. 이전 페이지: 'category' 또는 'list.html' 포함 (목록)
        prevUrl: { $regex: 'category|list.html' },
        
        // 2. 현재 페이지: 'product' 또는 'detail.html' 포함 (상품)
        // AND 조건: 'list.html'은 포함하면 안 됨 (이게 있으면 목록페이지임)
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
      // 3. [카테고리 URL] + [상품 URL] 조합으로 그룹핑
      {
        $group: {
          _id: { category: "$prevUrl", product: "$currentUrl" },
          count: { $sum: 1 },
          visitors: { $addToSet: "$visitorId" }
        }
      },
      { $sort: { count: -1 } },
      // 4. 다시 [카테고리] 기준으로 묶기
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


// by-click 라우트 내부
app.get('/by-click', async (req, res) => {
  const { sectionId, startDate, endDate } = req.query;

  console.log('=== 요청 파라미터 ===');
  console.log({ sectionId, startDate, endDate });

  // 실제 DB 조회 직전 쿼리 조건을 로그로 확인
  const query = {
      sectionId: sectionId, // 여기가 DB랑 똑같은지 확인!
      // 날짜 조건...
  };
  console.log('=== MongoDB 쿼리 조건 ===', JSON.stringify(query, null, 2));

  const result = await db.collection('visitors').find(query).toArray();
  console.log('=== 검색된 개수 ===', result.length);
  
  res.json({ success: true, visitors: result });
});

// ==========================================================
// [API 11] 특정 채널 방문자 목록 조회
// ✅ /api/trace/funnel 과 "100% 동일한 귀속 로직" 버전
//    - validVisitors 필터 동일
//    - channelName 매핑 로직 동일(utmData 기준)
//    - uniqueId(게스트=IP, 회원=visitorId) 동일
// ==========================================================
app.get('/api/trace/visitors/by-channel', async (req, res) => {
  try {
    const { channelName, startDate, endDate } = req.query;

    if (!channelName) {
      return res.status(400).json({ success: false, msg: 'Missing channelName' });
    }

    // 1) 날짜 필터 (퍼널과 동일)
    let dateFilter = {};
    if (startDate || endDate) {
      dateFilter = {};
      if (startDate) dateFilter.$gte = new Date(startDate + "T00:00:00.000Z");
      if (endDate) dateFilter.$lte = new Date(endDate + "T23:59:59.999Z");
    }

    // 2) validVisitors (퍼널과 동일)
    const validVisitors = await db.collection('visit_logs1Event').distinct('visitorId', {
      createdAt: dateFilter,
      currentUrl: { $regex: '1_promotion.html|index.html|store.html' }
    });

    if (!validVisitors || validVisitors.length === 0) {
      return res.json({ success: true, visitors: [] });
    }

    // 3) 채널 매핑 로직 (퍼널과 동일: utmData만 사용)
    const channelNameExpr = {
      $switch: {
        branches: [
          // 1. 네이버 브랜드 검색 (Campaign 기준)
          { case: { $eq: ["$utmData.campaign", "home_main"] },  then: "브검 : 홈페이지 메인" },
          { case: { $eq: ["$utmData.campaign", "naver_main"] }, then: "브검 : 1월 말할 수 없는 편안함(메인)" },
          { case: { $eq: ["$utmData.campaign", "naver_sub1"] }, then: "브검 : 1월 말할 수 없는 편안함(서브1)_10%" },
          { case: { $eq: ["$utmData.campaign", "naver_sub2"] }, then: "브검 : 1월 말할 수 없는 편안함(서브2)_20%" },
          { case: { $eq: ["$utmData.campaign", "naver_sub3"] }, then: "브검 : 1월 말할 수 없는 편안함(서브3)_갓생" },
          { case: { $eq: ["$utmData.campaign", "naver_sub4"] }, then: "브검 : 1월 말할 수 없는 편안함(서브4)_무료배송" },
          { case: { $eq: ["$utmData.campaign", "naver_sub5"] }, then: "브검 : 1월 말할 수 없는 편안함(서브5)_가까운매장" },

          // 2. 메타 광고 (Content 기준)
          { case: { $eq: ["$utmData.content", "employee_discount"] }, then: "메타 : 1월 말할 수 없는 편안함(직원 할인 찬스)" },
          { case: { $eq: ["$utmData.content", "areading_group1"] },   then: "메타 : 1월 말할 수 없는 편안함(sky독서소파)" },
          { case: { $eq: ["$utmData.content", "areading_group2"] },   then: "메타 : 1월 말할 수 없는 편안함(sky독서소파2)" },
          { case: { $eq: ["$utmData.content", "special_price1"] },    then: "메타 : 1월 말할 수 없는 편안함(신년특가1)" },
          { case: { $eq: ["$utmData.content", "special_price2"] },    then: "메타 : 1월 말할 수 없는 편안함(신년특가2)" },
          { case: { $eq: ["$utmData.content", "horse"] },             then: "메타 : 1월 말할 수 없는 편안함(말 ai아님)" },

          // 3. 카카오 플친 (Campaign 기준)
          { case: { $eq: ["$utmData.campaign", "message_main"] }, then: "플친 : 1월 말할 수 없는 편안함(메인)" },
          { case: { $eq: ["$utmData.campaign", "message_sub1"] }, then: "플친 : 1월 말할 수 없는 편안함(10%)" },
          { case: { $eq: ["$utmData.campaign", "message_sub2"] }, then: "플친 : 1월 말할 수 없는 편안함(20%)" },
          { case: { $eq: ["$utmData.campaign", "message_sub3"] }, then: "플친 : 1월 말할 수 없는 편안함(지원이벤트)" },
          { case: { $eq: ["$utmData.campaign", "message_sub4"] }, then: "플친 : 1월 말할 수 없는 편안함(무료배송)" }
        ],
        default: "직접/기타 방문"
      }
    };

    // 4) 방문자 리스트 생성 (퍼널과 같은 대상/기준으로)
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

      // "사람 단위"로 묶기
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




// ========== [17] 서버 시작 ==========
// (추가 초기화 작업이 필요한 경우)
// 아래는 추가적인 초기화 작업 후 서버를 시작하는 예시입니다.
(async function initialize() {
  await getTokensFromDB();
  const PORT = process.env.PORT || 6000;
  app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
  });
})();
