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
const CAFE24_MALLID = process.env.CAFE24_MALLID;  // mall_id가 반드시 설정되어야 함
const OPEN_URL = process.env.OPEN_URL;  
const API_KEY = process.env.API_KEY;
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

// ========== [5] 최근 2주간 날짜 계산 및 상위 10개 상품 조회 함수 ==========
function getLastTwoWeeksDates() {
  const now = new Date();
  const end_date = now.toISOString().split('T')[0];
  const pastDate = new Date(now);
  pastDate.setDate(now.getDate() - 14);
  const start_date = pastDate.toISOString().split('T')[0];
  return { start_date, end_date };
}

// 제품 상세정보를 가져오는 함수 (/api/v2/admin/products/{product_no})
// mall_id를 쿼리 파라미터로 추가하여 호출
async function getProductDetail(product_no) {
  const url = `https://yogibo.cafe24api.com/api/v2/admin/products/${product_no}?mall_id=${CAFE24_MALLID}`;
  try {
    const response = await axios.get(url, {
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json'
      }
    });
    // 반환된 응답에서 product_name만 추출
    if (response.data && response.data.product && response.data.product.product_name) {
      console.log(`Product detail for ${product_no}:`, response.data.product.product_name);
      return response.data.product.product_name;
    }
    return null;
  } catch (error) {
    console.error(`Error fetching product detail for product_no ${product_no}:`, error.response ? error.response.data : error.message);
    return null;
  }
}

// 장바구니에 담긴 수 기준 상위 10개 상품 조회 함수 (상세정보의 product_name만 사용)
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
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json'
      },
      params
    });

    console.log("API 응답 데이터:", response.data);
    //배열
    let products = response.data;
    if (!Array.isArray(products)) {
      if (products.action && Array.isArray(products.action)) {
        products = products.action;
      } else if (products.products && Array.isArray(products.products)) {
        products = products.products;
      } else if (products.data && Array.isArray(products.data)) {
        products = products.data;
      } else {
        throw new Error("Unexpected product data structure");
      }
    }

    const top10 = products.slice(0, 10);

    // 각 상품에 대해 product_no를 활용하여 상세 API 호출 후, 받은 product_name을 사용
    const updatedTop10 = await Promise.all(
      top10.map(async (product, index) => {
        const detailName = await getProductDetail(product.product_no);
        // detailName이 있으면 이를 사용, 없으면 기본값 '상품'
        const finalName = detailName || '상품';
        return {
          ...product,
          rank: index + 1,
          product_name: finalName,
          displayText: `${index + 1}위: ${finalName} - 총 ${product.add_cart_count || 0} 개 상품이 장바구니에 담겨 있습니다.`
        };
      })
    );

    console.log("불러온 상위 10개 상품 데이터:", updatedTop10);
    return updatedTop10;
  } catch (error) {
    console.error('Error fetching products:', error.response ? error.response.data : error.message);
    throw error;
  }
}

// ========== [6] 채팅 엔드포인트 (/chat) ==========
app.post("/chat", async (req, res) => {
  const userInput = req.body.message;
  const memberId = req.body.memberId;
  if (!userInput) {
    return res.status(400).json({ error: "Message is required" });
  }

  if (userInput.includes("장바구니 베스트 10 알려줘")) {
    try {
      const topProducts = await getTop10ProductsByAddCart();
      const productListText = topProducts.map(prod => prod.displayText).join("<br>");
      return res.json({
        text: "최근 2주간 장바구니에 많이 담긴 상위 10개 상품 정보입니다.<br>" + productListText
      });
    } catch (error) {
      return res.status(500).json({ text: "데이터를 가져오는 중 오류가 발생했습니다." });
    }
  }

  return res.json({ text: "입력하신 메시지를 처리할 수 없습니다." });
});

// ========== [7] 서버 시작 ==========
(async function initialize() {
  await getTokensFromDB();
  const PORT = process.env.PORT || 6000;
  app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
  });
})();
