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

// ========== [1] 환경변수 및 기본 설정 ==========
let accessToken = process.env.ACCESS_TOKEN; // 초기값 비워둠 (DB에서 로드)
let refreshToken = process.env.REFRESH_TOKEN //;
const CAFE24_CLIENT_ID = process.env.CAFE24_CLIENT_ID;
const CAFE24_CLIENT_SECRET = process.env.CAFE24_CLIENT_SECRET;
const DB_NAME = process.env.DB_NAME;
const MONGODB_URI = process.env.MONGODB_URI;
const CAFE24_MALLID = process.env.CAFE24_MALLID;
const CAFE24_API_VERSION = process.env.CAFE24_API_VERSION || '2025-12-01';

// ========== [2] Express 앱 기본 설정 ==========
const app = express();
app.use(cors());
app.use(compression());
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, "public")));

// MongoDB 컬렉션명
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
  console.log('토큰 만료 감지: Cafe24 서버에 토큰 갱신 요청 중...');
  
  try {
    // 1. Cafe24 OAuth 갱신 요청을 위한 헤더 생성 (Basic Auth)
    const authHeader = Buffer.from(`${CAFE24_CLIENT_ID}:${CAFE24_CLIENT_SECRET}`).toString('base64');

    // 2. Cafe24에 갱신 요청 (POST)
    const response = await axios({
      method: 'POST',
      url: `https://${CAFE24_MALLID}.cafe24api.com/api/v2/oauth/token`,
      headers: {
        'Authorization': `Basic ${authHeader}`,
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      data: `grant_type=refresh_token&refresh_token=${refreshToken}`
    });

    // 3. 응답 받은 새 토큰 정보 추출
    const { access_token, refresh_token } = response.data;

    // 4. 전역 변수 업데이트
    accessToken = access_token;
    // Cafe24는 갱신 시 새 Refresh Token을 줄 수도 있고 안 줄 수도 있음 (주면 업데이트)
    if (refresh_token) {
      refreshToken = refresh_token;
    }

    console.log('Cafe24 토큰 갱신 성공!');

    // 5. DB에 새 토큰 저장 (중요: 그래야 다음 재시작 때도 유지됨)
    await saveTokensToDB(accessToken, refreshToken);

    return accessToken;

  } catch (error) {
    console.error('토큰 갱신 실패 (치명적 오류):', error.response ? error.response.data : error.message);
    // 리프레시 토큰까지 만료된 경우일 수 있음 -> 알림 필요
    throw error;
  }
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
      console.log('Access Token 만료. 갱신 중...');
      await refreshAccessToken();
      return apiRequest(method, url, data, params);
    } else {
      console.error('API 요청 오류:', error.response ? error.response.data : error.message);
      throw error;
    }
  }
}



// ========== [럭키 드로우 이벤트 관련 함수] ==========
async function getCustomerDataByMemberId(memberId) {
  await getTokensFromDB();
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
// ★ [수정] 전역 clientInstance 대신 각 라우트에서 연결하도록 변경 권장하지만,
// 기존 코드 흐름을 유지하기 위해 여기서는 별도 처리된 라우트들만 유지
const clientInstance = new MongoClient(MONGODB_URI, { useUnifiedTopology: true });
clientInstance.connect()
  .then(() => {
    console.log('MongoDB 연결 성공 (이벤트용)');
    const db = clientInstance.db(DB_NAME);
    const entriesCollection = db.collection('entries');
    
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
        const customerData = await getCustomerDataByMemberId(memberId);
        if (!customerData || !customerData.customersprivacy) {
          return res.status(404).json({ error: '고객 데이터를 찾을 수 없습니다.' });
        }
        
        let customerPrivacy = customerData.customersprivacy;
        if (Array.isArray(customerPrivacy)) {
          customerPrivacy = customerPrivacy[0];
        }
        
        const { member_id, name, cellphone, email, address1, address2, sms, gender } = customerPrivacy;
        
        const existingEntry = await entriesCollection.findOne({ memberId: member_id });
        if (existingEntry) {
          return res.status(409).json({ message: '' });
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



// ==========================================================
// 2월 출석체크 부분
// ==========================================================


// ==========================================================
// [이벤트 API 1] 상태 조회
// - 우리 DB(F) -> Cafe24 조회(T) -> 우리 DB 업데이트(T, EXISTING)
// ==========================================================
app.get('/api/event/status', async (req, res) => {
  const { memberId } = req.query;
  if (!memberId) return res.status(400).json({ success: false, message: 'memberId required' });

  const client = new MongoClient(MONGODB_URI);
  try {
    await client.connect();
    const collection = client.db(DB_NAME).collection('event_daily_checkin');
    
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

    // 2. 우리 DB 미동의 상태면 Cafe24 API '조회' (GET만 수행)
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

            // ★ [DB저장] 기존 동의자로 확인됨 -> 'EXISTING'
            if (realConsent) {
                console.log(`[Sync] ${memberId} 기존 동의 확인 -> DB 업데이트 (EXISTING)`);
                await collection.updateOne(
                    { memberId: memberId },
                    { 
                        $set: { 
                            marketingAgreed: true, 
                            marketingAgreedAt: new Date(),
                            consentType: 'EXISTING' // ★ 기존 동의자
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
  } finally {
    await client.close();
  }
});

// ==========================================================
// [이벤트 API 2] 참여하기 (3회 제한 및 0회차 버그 수정)
// ==========================================================
app.post('/api/event/participate', async (req, res) => {
  const { memberId } = req.body;
  if (!memberId) return res.status(400).json({ success: false, message: 'Login required' });

  const client = new MongoClient(MONGODB_URI);
  try {
    await client.connect();
    const collection = client.db(DB_NAME).collection('event_daily_checkin');

    const eventDoc = await collection.findOne({ memberId });
    const nowKST = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
    const todayMoment = moment(nowKST).tz('Asia/Seoul');

    if (eventDoc) {
      // 3회 이상이면 차단
      if ((eventDoc.count || 0) >= 3) {
         return res.json({ success: false, message: '모든 이벤트 참여가 완료되었습니다!' });
      }
      // 날짜 중복 체크 (기록이 있을 때만)
      if (eventDoc.lastParticipatedAt) {
        const lastDate = moment(eventDoc.lastParticipatedAt).tz('Asia/Seoul');
        if (lastDate.isSame(todayMoment, 'day')) {
          return res.json({ success: false, message: '오늘 이미 참여하셨습니다.' });
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
  } finally {
    await client.close();
  }
});

// ==========================================================
// [이벤트 API 3] 마케팅 동의 (신규 동의자 구분 저장)
// - 이벤트 페이지에서 버튼 클릭 시 'NEW' 타입으로 저장
// ==========================================================
app.post('/api/event/marketing-consent', async (req, res) => {
  const { memberId } = req.body;
  if (!memberId) return res.status(400).json({ error: 'memberId required' });

  const client = new MongoClient(MONGODB_URI);
  try {
    await client.connect();
    const collection = client.db(DB_NAME).collection('event_daily_checkin');

    // ★ [DB저장] 버튼 클릭 -> 'NEW'
    await collection.updateOne(
      { memberId: memberId },
      { 
        $set: { 
            marketingAgreed: true, 
            marketingAgreedAt: new Date(),
            consentType: 'NEW' // ★ 신규 동의자 (이벤트 참여)
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
  } finally {
    await client.close();
  }
});
// ==========================================================
// [이벤트 API 4] 엑셀 다운로드 (시간 제외, 날짜만 출력)
// ==========================================================
app.get('/api/event/download', async (req, res) => {
  const client = new MongoClient(MONGODB_URI);
  try {
    await client.connect();
    const entries = await client.db(DB_NAME).collection('event_daily_checkin').find({}).toArray();

    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Participants');

    worksheet.columns = [
      { header: 'ID', key: 'memberId', width: 20 },
      { header: 'Count', key: 'count', width: 10 },
      { header: 'Marketing', key: 'marketingAgreed', width: 15 },
      { header: '동의 구분', key: 'consentType', width: 25 }, 
      { header: 'Last Action', key: 'lastParticipatedAt', width: 15 }, // 너비 줄임
      { header: 'First Action', key: 'firstParticipatedAt', width: 15 } // 너비 줄임
    ];

    entries.forEach(entry => {
      // ★ [수정] HH:mm:ss 제거하고 날짜만 표시 ('YYYY-MM-DD')
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
  } finally {
    await client.close();
  }
});





// ==========================================================
// [API 1] 로그 수집
// ==========================================================
app.post('/api/trace/log', async (req, res) => {
  const client = new MongoClient(MONGODB_URI); // ★ client 정의
  try {
    await client.connect();
    const db = client.db(DB_NAME); // ★ db 정의
      
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
  } finally {
      await client.close(); // ★ 연결 종료
  }
});


// ==========================================================
// [API 1-1] 체류 시간 업데이트
// ==========================================================
app.post('/api/trace/log/exit', async (req, res) => {
  let { logId, duration } = req.body;
  if (!logId || duration === undefined) return res.status(400).send('Missing Data');

  const client = new MongoClient(MONGODB_URI); // ★ 추가
  try {
    await client.connect(); // ★ 추가
    const db = client.db(DB_NAME); // ★ 추가

    await db.collection('visit_logs1Event').updateOne(
      { _id: new ObjectId(logId) }, 
      { $set: { duration: parseInt(duration) } }
    );
    res.send('OK');
  } catch (error) {
    console.error(error);
    res.status(500).send('Error');
  } finally {
      await client.close(); // ★ 추가
  }
});

// ==========================================================
// [API 2] 관리자 대시보드용: 단순 태그별 요약
// ==========================================================
app.get('/api/trace/summary', async (req, res) => {
  const client = new MongoClient(MONGODB_URI); // ★ 추가
  try {
    await client.connect(); // ★ 추가
    const db = client.db(DB_NAME); // ★ 추가

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
  } finally {
      await client.close(); // ★ 추가
  }
});

// ==========================================================
// [API 3] 방문자 목록 조회
// ==========================================================
app.get('/api/trace/visitors', async (req, res) => {
  const client = new MongoClient(MONGODB_URI); // ★ 추가
  try {
      await client.connect(); // ★ 추가
      const db = client.db(DB_NAME); // ★ 추가

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
  } finally {
      await client.close(); // ★ 추가
  }
});

// ==========================================================
// [API 4] 특정 유저 이동 경로
// ==========================================================
app.get('/api/trace/journey/:visitorId', async (req, res) => {
  const { visitorId } = req.params;
  const { startDate, endDate } = req.query;
  const client = new MongoClient(MONGODB_URI); // ★ 추가

  console.log('[Journey] 요청:', { visitorId, startDate, endDate });

  try {
      await client.connect(); // ★ 추가
      const db = client.db(DB_NAME); // ★ 추가

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
          console.log('[Journey] 회원 ID로 검색:', visitorId);
          baseQuery = { visitorId: visitorId };
          clickQuery = { visitorId: visitorId };
      } 
      else if (isIpFormat) {
          console.log('[Journey] IP로 검색 (게스트만):', visitorId);
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
          console.log('[Journey] 게스트 ID로 검색:', visitorId);
          
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

      console.log('[Journey] 방문 쿼리:', JSON.stringify(baseQuery));

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

      const journey = [...formattedViews, ...formattedClicks];
      journey.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

      res.json({ success: true, journey });

  } catch (error) {
      console.error('[Journey Error]', error);
      res.status(500).json({ msg: 'Server Error', error: error.message });
  } finally {
      await client.close(); // ★ 추가
  }
});

// ==========================================================
// [API 5] 퍼널 분석
// ==========================================================
app.get('/api/trace/funnel', async (req, res) => {
  const client = new MongoClient(MONGODB_URI); // ★ 추가
  try {
      await client.connect(); // ★ 추가
      const db = client.db(DB_NAME); // ★ 추가

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
  } finally {
      await client.close(); // ★ 추가
  }
});

// ==========================================================
// [API] Cafe24 카테고리 전체 정보 조회
// ==========================================================
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

// ==========================================================
// [신규 API] Cafe24 전체 상품 정보 조회
// ==========================================================
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

// ==========================================================
// [API 7] 섹션 클릭 로그 저장
// ==========================================================
app.post('/api/trace/click', async (req, res) => {
  const client = new MongoClient(MONGODB_URI); // ★ 추가
  try {
      await client.connect(); // ★ 추가
      const db = client.db(DB_NAME); // ★ 추가

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
  } finally {
      await client.close(); // ★ 추가
  }
});

// ==========================================================
// [API 8] 섹션 클릭 통계 조회
// ==========================================================
app.get('/api/trace/clicks/stats', async (req, res) => {
  const client = new MongoClient(MONGODB_URI); // ★ 추가
  try {
      await client.connect(); // ★ 추가
      const db = client.db(DB_NAME); // ★ 추가

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
  } finally {
      await client.close(); // ★ 추가
  }
});

// ==========================================================
// [API] 특정 버튼 클릭 사용자 조회
// ==========================================================
app.get('/api/trace/visitors/by-click', async (req, res) => {
  const client = new MongoClient(MONGODB_URI); // ★ 추가
  try {
      await client.connect(); // ★ 추가
      const db = client.db(DB_NAME); // ★ 추가

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
  } finally {
      await client.close(); // ★ 추가
  }
});

// ==========================================================
// [API 9] 인기 페이지 및 방문자 그룹핑 조회
// ==========================================================
app.get('/api/trace/stats/pages', async (req, res) => {
  const client = new MongoClient(MONGODB_URI); // ★ 추가
  try {
    await client.connect(); // ★ 추가
    const db = client.db(DB_NAME); // ★ 추가

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
  } finally {
      await client.close(); // ★ 추가
  }
});

// ==========================================================
// [API 10] 카테고리 -> 상품 이동 흐름 분석
// ==========================================================
app.get('/api/trace/stats/flow', async (req, res) => {
  const client = new MongoClient(MONGODB_URI); // ★ 추가
  try {
    await client.connect(); // ★ 추가
    const db = client.db(DB_NAME); // ★ 추가

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
  } finally {
      await client.close(); // ★ 추가
  }
});

// ==========================================================
// [API 11] 특정 채널 방문자 목록 조회
// ==========================================================
app.get('/api/trace/visitors/by-channel', async (req, res) => {
  const client = new MongoClient(MONGODB_URI); // ★ 추가
  try {
    await client.connect(); // ★ 추가
    const db = client.db(DB_NAME); // ★ 추가

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
  } finally {
      await client.close(); // ★ 추가
  }
});

// [임시] DB 토큰 강제 업데이트 (새로 발급받은 토큰으로 DB 덮어쓰기)
app.get('/force-update-token', async (req, res) => {
  const client = new MongoClient(MONGODB_URI);
  try {
      await client.connect();
      const db = client.db(DB_NAME);
      
      // 현재 코드 상단 변수에 들어있는 '새 토큰' 값으로 DB를 강제 업데이트합니다.
      await db.collection('tokens').updateOne(
          {}, 
          { 
              $set: { 
                  accessToken: accessToken, // 코드 맨 윗줄의 새 토큰
                  refreshToken: refreshToken, // 코드 맨 윗줄의 새 리프레시 토큰
                  updatedAt: new Date()
              } 
          },
          { upsert: true }
      );

      res.send(`
          <h1>DB 업데이트 완료!</h1>
          <p><b>현재 적용된 토큰:</b> ${accessToken.substring(0, 10)}...</p>
          <p>이제 이벤트 페이지에서 버튼을 다시 눌러보세요.</p>
      `);
  } catch (e) {
      res.send(`에러 발생: ${e.message}`);
  } finally {
      await client.close();
  }
});




//인스타그램 연동 부분

const INSTAGRAM_TOKEN = process.env.INSTAGRAM_TOKEN;
const SALLYFELLTOKEN = process.env.SALLYFELLTOKEN;


// 기존 /api/instagramFeed 엔드포인트 수정
app.get("/api/instagramFeed", async (req, res) => {
  try {
    const pageLimit = 40;
    // Instagram Graph API 요청 URL 구성
    const url = `https://graph.instagram.com/v22.0/me/media?access_token=${INSTAGRAM_TOKEN}&fields=id,caption,media_url,permalink,media_type,timestamp&limit=${pageLimit}`;
    const response = await axios.get(url);
    const feedData = response.data;
    
    // 가져온 인스타그램 데이터를 DB에 저장
    saveInstagramFeedData(feedData);
    
    res.json(feedData);
  } catch (error) {
    console.error("Error fetching Instagram feed:", error.message);
    res.status(500).json({ error: "Failed to fetch Instagram feed" });
  }
});

//샐리필 전용
app.get("/api/instagramSallyFeed", async (req, res) => {
  try {
    const pageLimit = 16;
    // Instagram Graph API 요청 URL 구성
    const url = `https://graph.instagram.com/v22.0/me/media?access_token=${SALLYFELLTOKEN}&fields=id,caption,media_url,permalink,media_type,timestamp&limit=${pageLimit}`;
    const response = await axios.get(url);
    const feedData = response.data;
    
    // 가져온 인스타그램 데이터를 DB에 저장
    saveInstagramFeedData(feedData);
    
    res.json(feedData);
  } catch (error) {
    console.error("Error fetching Instagram feed:", error.message);
    res.status(500).json({ error: "Failed to fetch Instagram feed" });
  }
});



app.get('/api/instagramToken', (req, res) => {
  const token = process.env.INSTAGRAM_TOKEN;
  if (token) {
    res.json({ token });
  } else {
    res.status(500).json({ error: 'INSTAGRAM_TOKEN is not set in environment variables.' });
  }
});


app.get('/api/sallyfeelToken', (req, res) => {
  const token = process.env.SALLYFELLTOKEN;
  if (token) {
    res.json({ token });
  } else {
    res.status(500).json({ error: 'INSTAGRAM_TOKEN is not set in environment variables.' });
  }
});


// 인스타그램 피드 데이터를 MongoDB에 저장하는 함수 추가
async function saveInstagramFeedData(feedData) {
  try {
    const client = new MongoClient(MONGODB_URI, { useUnifiedTopology: true });
    await client.connect();
    const db = client.db(DB_NAME);
    const instagramCollection = db.collection('instagramData');
    
    const feedItems = feedData.data || [];
    for (const item of feedItems) {
      // 각 인스타그램 게시물을 id를 기준으로 upsert 처리
      await instagramCollection.updateOne(
        { id: item.id },
        { $set: item },
        { upsert: true }
      );
    }
    await client.close();
    console.log("Instagram feed data saved to DB successfully.");
  } catch (err) {
    console.error("Error saving Instagram feed data to DB:", err);
  }
}
app.post('/api/trackClick', async (req, res) => {
  const { postId } = req.body;
  if (!postId) {
    return res.status(400).json({ error: 'postId 값이 필요합니다.' });
  }
  try {
    const client = new MongoClient(MONGODB_URI, { useUnifiedTopology: true });
    await client.connect();
    const db = client.db(DB_NAME);
    const collection = db.collection('instaClickdata');
    
    // postId를 기준으로 클릭 카운터를 1 증가 (upsert: document가 없으면 생성)
    await collection.updateOne(
      { postId: postId },
      { $inc: { counter: 1 } },
      { upsert: true }
    );
    
    await client.close();
    res.status(200).json({ message: 'Click tracked successfully', postId });
  } catch (error) {
    console.error("Error tracking click event:", error);
    res.status(500).json({ error: 'Error tracking click event' });
  }
});
//인스타 클릭데이터 가져오기
app.get('/api/getClickCount', async (req, res) => {
  const postId = req.query.postId;
  if (!postId) {
    return res.status(400).json({ error: 'postId query parameter is required' });
  }
  try {
    const client = new MongoClient(MONGODB_URI, { useUnifiedTopology: true });
    await client.connect();
    const db = client.db(DB_NAME);
    const collection = db.collection('instaClickdata');
    
    // postId 기준으로 document를 찾고, counter 필드 반환 (없으면 0)
    const doc = await collection.findOne({ postId: postId });
    const clickCount = doc && doc.counter ? doc.counter : 0;
    
    await client.close();
    res.status(200).json({ clickCount });
  } catch (error) {
    console.error("Error fetching click count:", error);
    res.status(500).json({ error: 'Error fetching click count' });
  }
});



// ========== [17] 서버 시작 ==========
(async function initialize() {
  await getTokensFromDB();
  const PORT = process.env.PORT || 6000;
  app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
  });
})();