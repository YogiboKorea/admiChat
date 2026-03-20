

// ==============================
// (1) 개인정보 수집·이용 동의(선택) 업데이트
// POST /api/v2/admin/privacyconsents
async function updatePrivacyConsent(memberId) {
  const url = `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/privacyconsents`;
  const payload = {
    shop_no: 1,
    request: {
      member_id:  memberId,
      consent_type: 'marketing',            // "마케팅 목적" 동의
      agree:        'T',                    // 동의
      issued_at:    new Date().toISOString()
    }
  };

  try {
    return await apiRequest('POST', url, payload);
  } catch (err) {
    // 엔드포인트가 없다고 나오면 무시하고 다음 단계로
    if (err.response?.data?.error?.message.includes('No API found')) {
      console.warn('privacyconsents 엔드포인트 미지원, 패스');
      return;
    }
    throw err;
  }
}

// ==============================
// (2) SMS 수신동의 업데이트
// PUT /api/v2/admin/customersprivacy/{member_id}
async function updateMarketingConsent(memberId) {
  const url = `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/customersprivacy/${memberId}`;
  const payload = {
    request: {
      shop_no:   1,
      member_id: memberId,
      sms:       'T'    // SMS 수신동의만 T로!
      // news_mail: 'T'  // 뉴스메일은 건드리지 않습니다
    }
  };
  return apiRequest('PUT', url, payload);
}

// ==============================
// (3) 적립금 지급 함수
async function giveRewardPoints(memberId, amount, reason) {
  const url = `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/points`;
  const payload = {
    shop_no: 1,
    request: {
      member_id: memberId,
      amount,
      type:   'increase',
      reason
    }
  };
  return apiRequest('POST', url, payload);
}

// ==============================
// (4) 이벤트 참여 엔드포인트
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
      return res.status(409).json({ message: '이미 참여하셨습니다.' });
    }

    // 1) 마케팅 목적 개인정보 수집·이용 동의(선택)
    await updatePrivacyConsent(memberId);

    // 2) SMS 수신동의 업데이트
    await updateMarketingConsent(memberId);

    // 3) 적립금 5원 지급
    await giveRewardPoints(memberId, 5, '마케팅 수신동의 이벤트 참여 보상');

    // 4) 참여 기록 저장 (서울시간)
    const seoulNow = new Date(
      new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' })
    );
    await coll.insertOne({ memberId, store, participatedAt: seoulNow });

    res.json({ success: true, message: '참여 및 보상 지급 완료!' });
  } catch (err) {
    console.error('이벤트 처리 오류:', err.response?.data || err.message);
    res.status(500).json({ error: '서버 오류가 발생했습니다.' });
  } finally {
    await client.close();
  }
});
