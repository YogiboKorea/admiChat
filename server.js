const express = require("express");
const bodyParser = require("body-parser");
const fs = require("fs");
const path = require("path");
const cors = require("cors");
const compression = require("compression");
const axios = require("axios");
const { MongoClient } = require("mongodb");
require("dotenv").config();

// ========== [1] 환경변수 및 기본 설정 ==========
let accessToken = process.env.ACCESS_TOKEN || 'pPhbiZ29IZ9kuJmZ3jr15C';
let refreshToken = process.env.REFRESH_TOKEN || 'CMLScZx0Bh3sIxlFTHDeMD';
const CAFE24_CLIENT_ID = process.env.CAFE24_CLIENT_ID;
const CAFE24_CLIENT_SECRET = process.env.CAFE24_CLIENT_SECRET;
const DB_NAME = process.env.DB_NAME;
const MONGODB_URI = process.env.MONGODB_URI;
const CAFE24_MALLID = process.env.CAFE24_MALLID;
const OPEN_URL = process.env.OPEN_URL;  // 예: "https://api.openai.com/v1/chat/completions"
const API_KEY = process.env.API_KEY;    // OpenAI API 키
const FINETUNED_MODEL = process.env.FINETUNED_MODEL || "gpt-3.5-turbo";
const CAFE24_API_VERSION = process.env.CAFE24_API_VERSION || '2024-06-01';

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
      console.log('Access Token 만료. 갱신 중...');
      await refreshAccessToken();
      return apiRequest(method, url, data, params);
    } else {
      console.error('API 요청 오류:', error.response ? error.response.data : error.message);
      throw error;
    }
  }
}

// ========== [5] Yogibo 브랜드 시스템 프롬프트 ==========
const YOGIBO_SYSTEM_PROMPT = `
마케팅 용으로 사용되는 데이터야 전문데이터를 회사 직원인 마케터 들에게 전달 하는 방식으로
간결하게 해당 데이터를 받아와 정리해서 답변해줘
`;

// ========== [6] OpenAI GPT (fallback) 함수 ==========
async function getGPT3TurboResponse(userInput) {
  try {
    const response = await axios.post(
      OPEN_URL,
      {
        model: FINETUNED_MODEL,
        messages: [
          { role: "system", content: YOGIBO_SYSTEM_PROMPT },
          { role: "user", content: userInput }
        ]
      },
      {
        headers: {
          'Authorization': `Bearer ${API_KEY}`,
          'Content-Type': 'application/json'
        }
      }
    );
    const gptAnswer = response.data.choices[0].message.content;
    return gptAnswer;
  } catch (error) {
    console.error("Error calling OpenAI:", error.message);
    return "요기보 챗봇 오류가 발생했습니다. 다시 시도 부탁드립니다.";
  }
}

// ========== [7] 최근 2주간 날짜 계산 및 상위 10개 상품 조회 함수 ==========
function getLastTwoWeeksDates() {
  const now = new Date();
  const end_date = now.toISOString().split('T')[0];
  const pastDate = new Date(now);
  pastDate.setDate(now.getDate() - 14);
  const start_date = pastDate.toISOString().split('T')[0];
  return { start_date, end_date };
}

// 장바구니에 담긴 수 기준 상위 10개 상품 조회 함수
async function getTop10ProductsByAddCart() {
  const { start_date, end_date } = getLastTwoWeeksDates();
  const url = 'https://ca-api.cafe24data.com/carts/action';
  const params = {
    mall_id: 'yogibo',      // 실제 몰 아이디로 변경
    shop_no: 1,             // 기본 샵 번호 (DEFAULT 1)
    start_date,
    end_date,
    device_type: 'total',   // pc, mobile, total 중 선택
    limit: 100,             // 최소 50, 최대 1000 (여기서는 100)
    offset: 0,
    sort: 'add_cart_count', // 정렬 기준: 장바구니에 담긴수
    order: 'desc'           // 내림차순 정렬
  };

  try {
    const response = await axios.get(url, {
      headers: {
        'Authorization': 'Bearer {access_token}', // 실제 access token으로 교체
        'Content-Type': 'application/json'
      },
      params
    });

    // 응답 데이터가 배열 형태라고 가정
    const products = response.data;

    // 상위 10개 상품 추출 후 순위 및 카운트 문구 추가
    const top10ProductsWithMessage = products.slice(0, 10).map((product, index) => {
      const rank = index + 1;
      const productName = product.product_name || '상품';
      const count = product.add_cart_count || 0;
      return {
        ...product,
        displayText: `${rank}위: ${productName} - 총 ${count} 개 상품이 장바구니에 담겨 있습니다.`
      };
    });

    // 데이터가 제대로 불러와졌는지 console.log로 출력
    console.log("불러온 상위 10개 상품 데이터:", top10ProductsWithMessage);

    return top10ProductsWithMessage;
  } catch (error) {
    console.error('Error fetching products:', error.response ? error.response.data : error.message);
    throw error;
  }
}

// ========== [8] 챗봇 관련 보조 함수 ==========
async function findAnswer(userInput, memberId) {
  // 기본적으로 질문을 이해하지 못한 경우를 반환하는 예시
  return { text: "질문을 이해하지 못했어요. 좀더 자세히 입력 해주시겠어요", videoHtml: null, description: null, imageUrl: null };
}

function normalizeSentence(sentence) {
  return sentence.trim();
}

async function saveConversationLog(memberId, userInput, answerText) {
  // 실제 로그 저장 로직 구현 (예: DB에 저장)
  console.log(`로그 저장 - 회원ID: ${memberId}, 질문: ${userInput}, 답변: ${answerText}`);
}

// ========== [9] /chat 라우팅 ==========
app.post("/chat", async (req, res) => {
  const userInput = req.body.message;
  const memberId = req.body.memberId; // 프론트에서 전달한 회원 ID
  if (!userInput) {
    return res.status(400).json({ error: "Message is required" });
  }

  // 사용자가 "최근 2주간 장바구니 담긴 베스트 상품 10개"를 요청한 경우
  if (
    userInput.includes("최근 2주") &&
    userInput.includes("장바구니") &&
    userInput.includes("장바구니 베스트 상품") &&
    userInput.includes("베스트 상품 10개")
  ) {
    try {
      const topProducts = await getTop10ProductsByAddCart();
      return res.json({ 
        text: "최근 2주간 장바구니에 많이 담긴 상위 10개 상품입니다.",
        data: topProducts 
      });
    } catch(error) {
      return res.status(500).json({ text: "데이터를 가져오는 중 오류가 발생했습니다." });
    }
  }

  // 기본 챗봇 로직 처리
  try {
    const answer = await findAnswer(userInput, memberId);
    let finalAnswer = answer;
    if (answer.text === "질문을 이해하지 못했어요. 좀더 자세히 입력 해주시겠어요") {
      const gptResponse = await getGPT3TurboResponse(userInput);
      finalAnswer = {
        text: gptResponse,
        videoHtml: null,
        description: null,
        imageUrl: null
      };
    }
    // "내아이디" 검색어가 아니라면 대화 로그 저장
    if (normalizeSentence(userInput) !== "내 아이디") {
      await saveConversationLog(memberId, userInput, finalAnswer.text);
    }
    return res.json(finalAnswer);
  } catch (error) {
    console.error("Error in /chat endpoint:", error.message);
    return res.status(500).json({
      text: "질문을 이해하지 못했어요. 좀더 자세히 입력 해주시겠어요",
      videoHtml: null,
      description: null,
      imageUrl: null
    });
  }
});

// ========== [10] 서버 시작 ==========
(async function initialize() {
  await getTokensFromDB();  // MongoDB에서 토큰 불러오기
  const PORT = process.env.PORT || 6000;
  app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
  });
})();
