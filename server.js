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

// ========== [5] 최근 30일(약 4주간) 날짜 계산 ==========
function getLastTwoWeeksDates() {
  const now = new Date();
  const end_date = now.toISOString().split('T')[0];
  const pastDate = new Date(now);
  pastDate.setDate(now.getDate() - 30);
  const start_date = pastDate.toISOString().split('T')[0];
  return { start_date, end_date };
}

// ========== [6] 제품 상세정보를 가져오는 함수 ==========
async function getProductDetail(product_no) {
  const url = `https://yogibo.cafe24api.com/api/v2/admin/products/${product_no}?mall_id=${CAFE24_MALLID}`;
  try {
    const response = await axios.get(url, {
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json'
      }
    });
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

// ========== [7] 장바구니에 담긴 수 기준 상위 10개 상품 조회 함수 ==========
async function getTop10ProductsByAddCart() {
  const { start_date, end_date } = getLastTwoWeeksDates();
  const url = 'https://ca-api.cafe24data.com/carts/action';
  const params = {
    mall_id: 'yogibo',  
    shop_no: 1,
    start_date,
    end_date,
    device_type: 'total',
    limit: 100,
    offset: 0,
    sort: 'add_cart_count',
    order: 'desc'
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

    // 각 상품에 대해 상세 API를 호출하여 product_name(세부 API의 값)을 가져온 후 displayText 구성
    const updatedTop10 = await Promise.all(
      top10.map(async (product, index) => {
        const detailName = await getProductDetail(product.product_no);
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

async function getTop10PagesByView() {
  const { start_date, end_date } = getLastTwoWeeksDates();
  const url = 'https://ca-api.cafe24data.com/pages/view';
  const params = {
    mall_id: 'yogibo',
    shop_no: 1,
    start_date,
    end_date,
    limit: 10,
    sort: 'visit_count', // 방문수 기준 정렬
    order: 'desc'
  };

  try {
    const response = await axios.get(url, {
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json'
      },
      params
    });
    console.log("Pages API 응답 데이터:", response.data);
    
    // 응답 데이터가 배열이 아니라면, "view" 배열을 사용
    let pages = response.data;
    if (!Array.isArray(pages)) {
      if (pages.view && Array.isArray(pages.view)) {
        pages = pages.view;
      } else if (pages.pages && Array.isArray(pages.pages)) {
        pages = pages.pages;
      } else if (pages.data && Array.isArray(pages.data)) {
        pages = pages.data;
      } else {
        throw new Error("Unexpected pages data structure");
      }
    }
    
    // 상위 10개 페이지 추출
    const top10Pages = pages.slice(0, 10);
    
    // 각 페이지의 url, count, visit_count를 사용하여 displayText 구성
    const updatedPages = top10Pages.map((page, index) => {
      const urlText = page.url || 'N/A';
      // 여기서 count를 페이지 뷰로 사용합니다.
      const pageView = page.count || 0;
      const visitCount = page.visit_count || 0;
      return {
        ...page,
        rank: index + 1,
        displayText: `${index + 1}위: ${urlText} - 페이지 뷰: ${pageView}, 방문수: ${visitCount}`
      };
    });
    
    console.log("불러온 상위 10 페이지 데이터:", updatedPages);
    return updatedPages;
  } catch(error) {
    console.error("Error fetching pages:", error.response ? error.response.data : error.message);
    throw error;
  }
}



// ========== [9] 채팅 엔드포인트 (/chat) ==========
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
        text: "최근 30일간 장바구니에 많이 담긴 상위 10개 상품 정보입니다.<br>" + productListText
      });
    } catch (error) {
      return res.status(500).json({ text: "상품 데이터를 가져오는 중 오류가 발생했습니다." });
    }
  }

  if (userInput.includes("가장 많이 접속한 페이지") || userInput.includes("페이지 뷰")) {
    try {
      const topPages = await getTop10PagesByView();
      const pageListText = topPages.map(page => page.displayText).join("<br>");
      return res.json({
        text: "가장 많이 접속한 페이지 TOP 10 정보입니다.<br>" + pageListText
      });
    } catch (error) {
      return res.status(500).json({ text: "페이지 데이터를 가져오는 중 오류가 발생했습니다." });
    }
  }

  return res.json({ text: "입력하신 메시지를 처리할 수 없습니다." });
});

// ========== [10] 서버 시작 ==========
(async function initialize() {
  await getTokensFromDB();
  const PORT = process.env.PORT || 6000;
  app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
  });
})();
