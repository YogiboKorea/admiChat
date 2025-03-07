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

// ========== [8] 페이지 뷰 및 방문수 상위 10개 페이지 조회 함수 ==========
async function getTop10PagesByView() {
  const { start_date, end_date } = getLastTwoWeeksDates();
  const url = 'https://ca-api.cafe24data.com/pages/view';
  const params = {
    mall_id: 'yogibo',
    shop_no: 1,
    start_date,
    end_date,
    limit: 10,
    sort: 'visit_count',
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
    
    const top10Pages = pages.slice(0, 10);
    const updatedPages = top10Pages.map((page, index) => {
      const urlText = "http://yogibo.kr" + (page.url || 'N/A');
      const visitCount = page.visit_count || 0;
      const firstVisitCount = page.first_visit_count || 0;
      return {
        ...page,
        rank: index + 1,
        displayText: `${index + 1}위: ${urlText} - 방문자수: ${visitCount}, 처음 접속수: ${firstVisitCount}`
      };
    });
    
    console.log("불러온 상위 10 페이지 데이터:", updatedPages);
    return updatedPages;
  } catch(error) {
    console.error("Error fetching pages:", error.response ? error.response.data : error.message);
    throw error;
  }
}

// ========== [9] 시간대별 결제금액 순위 조회 함수 ==========
function formatCurrency(amount) {
  const num = Number(amount) || 0;
  if (num >= 1e12) {
    return (num / 1e12).toFixed(2) + " 조";
  } else if (num >= 1e8) {
    return (num / 1e8).toFixed(2) + " 억";
  } else {
    return num.toLocaleString('ko-KR') + " 원";
  }
}

async function getSalesTimesRanking() {
  const { start_date, end_date } = getLastTwoWeeksDates();
  const url = 'https://ca-api.cafe24data.com/sales/times';
  const params = {
    mall_id: 'yogibo',
    shop_no: 1,
    start_date,
    end_date,
    limit: 10,
    sort: 'order_amount',
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
    console.log("Sales Times API 응답 데이터:", response.data);
    let times = response.data;
    if (!Array.isArray(times)) {
      if (times.times && Array.isArray(times.times)) {
        times = times.times;
      } else if (times.data && Array.isArray(times.data)) {
        times = times.data;
      } else {
        throw new Error("Unexpected sales times data structure");
      }
    }
    const updatedTimes = times.map((time, index) => {
      const hour = time.hour || 'N/A';
      const buyersCount = time.buyers_count || 0;
      const orderCount = time.order_count || 0;
      const formattedAmount = formatCurrency(time.order_amount || 0);
      return {
        ...time,
        rank: index + 1,
        displayText: `${index + 1}위: ${hour}시 - 구매자수: ${buyersCount}, 구매건수: ${orderCount}, 매출액: ${formattedAmount}`
      };
    });
    console.log("불러온 시간대별 결제금액 순위 데이터:", updatedTimes);
    return updatedTimes;
  } catch(error) {
    console.error("Error fetching sales times:", error.response ? error.response.data : error.message);
    throw error;
  }
}

// ========== [10] 광고 매체별 구매 순위 조회 함수 ==========
async function getTop10AdSales() {
  const { start_date, end_date } = getLastTwoWeeksDates();
  const url = 'https://ca-api.cafe24data.com/visitpaths/adsales';
  const params = {
    mall_id: 'yogibo',
    shop_no: 1,
    start_date,
    end_date,
    device_type: 'total',
    limit: 100,
    offset: 0,
    sort: 'order_amount',
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
    console.log("Ad Sales API 응답 데이터:", response.data);
    let data = response.data;
    let adsales = [];
    if (data.adsales && Array.isArray(data.adsales)) {
      adsales = data.adsales;
    } else {
      throw new Error("Unexpected ad sales data structure");
    }
    const top10 = adsales.slice(0, 10);
    const updatedTop10 = top10.map((item, index) => {
      const formattedAmount = formatCurrency(item.order_amount);
      return {
        rank: index + 1,
        ad: item.ad,
        order_count: item.order_count,
        order_amount: item.order_amount,
        displayText: `${index + 1}위: ${item.ad} - 구매건수: ${item.order_count}, 매출액: ${formattedAmount}`
      };
    });
    console.log("불러온 광고 매체별 구매 순위 데이터:", updatedTop10);
    return updatedTop10;
  } catch (error) {
    console.error("Error fetching ad sales:", error.response ? error.response.data : error.message);
    throw error;
  }
}

// ========== [11] 일별 방문자수 조회 함수 ==========
async function getDailyVisitorStats() {
  const { start_date, end_date } = getLastTwoWeeksDates();
  const url = 'https://ca-api.cafe24data.com/visitors/view';
  const params = {
    mall_id: 'yogibo',
    shop_no: 1,
    start_date,
    end_date,
    device_type: 'total',
    format_type: 'day',
    limit: 100,
    offset: 0,
    sort: 'date',
    order: 'asc'
  };
  try {
    const response = await axios.get(url, {
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json'
      },
      params
    });
    console.log("Daily Visitor Stats API 응답 데이터:", response.data);
    let stats = response.data;
    if (!Array.isArray(stats)) {
      if (stats.view && Array.isArray(stats.view)) {
        stats = stats.view;
      } else if (stats.data && Array.isArray(stats.data)) {
        stats = stats.data;
      } else {
        throw new Error("Unexpected daily visitor stats data structure");
      }
    }
    const updatedStats = stats.map(item => {
      const formattedDate = new Date(item.date).toISOString().split('T')[0];
      return `${formattedDate} 방문자수: ${item.unique_visit_count}`;
    });
    console.log("불러온 일별 방문자수 데이터:", updatedStats);
    return updatedStats;
  } catch (error) {
    console.error("Error fetching daily visitor stats:", error.response ? error.response.data : error.message);
    throw error;
  }
}


// ========== [12] 상세페이지 접속 순위 조회 함수 () ==========
async function getTop10ProductViews() {
  const { start_date, end_date } = getLastTwoWeeksDates();
  const url = 'https://ca-api.cafe24data.com/products/view';
  // limit 값을 충분히 크게 설정해서 전체 데이터를 가져오도록 함 (예: 1000)
  const params = {
    mall_id: 'yogibo',
    start_date,
    end_date,
    limit: 300,  // 전체 데이터를 불러오기 위한 큰 값
    offset: 0
  };

  try {
    const response = await axios.get(url, {
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json'
      },
      params
    });
    console.log("Product View API 응답 데이터:", response.data);

    // 응답 데이터가 배열이면 그대로 사용하고, 아니라면 객체 내의 "count" 또는 "view" 배열을 사용
    let products;
    if (Array.isArray(response.data)) {
      products = response.data;
    } else if (response.data && Array.isArray(response.data.count)) {
      products = response.data.count;
    } else if (response.data && Array.isArray(response.data.view)) {
      products = response.data.view;
    } else {
      console.error("Unexpected product view data structure:", response.data);
      throw new Error("Unexpected product view data structure");
    }
    
    // 전체 리스트가 불러와진 후, 유효한 항목만 필터링 (product_no와 count가 존재하는 경우)
    products = products.filter(item => item.product_no && typeof item.count === "number");
    
    if (products.length === 0) {
      console.log("조회된 상품 뷰 데이터가 없습니다.");
      return [];
    }
    
    // 조회수(count) 기준 내림차순 정렬
    products.sort((a, b) => b.count - a.count);
    const top10 = products.slice(0, 10);
    
    // 각 항목에 대해 product_no를 활용해 상세 API 호출 후, 상세의 product_name 사용
    const updatedProducts = await Promise.all(
      top10.map(async (item, index) => {
        const detailName = await getProductDetail(item.product_no);
        const finalName = detailName || item.product_name || '상품';
        return {
          rank: index + 1,
          product_no: item.product_no,
          product_name: finalName,
          count: item.count,
          displayText: `${index + 1}위: ${finalName} (상품번호: ${item.product_no}) - 조회수: ${item.count}`
        };
      })
    );
    console.log("불러온 상세페이지 접속 순위 데이터:", updatedProducts);
    return updatedProducts;
  } catch (error) {
    console.error("Error fetching product view rankings:", error.response ? error.response.data : error.message);
    throw error;
  }
}





// ========== [13] 채팅 엔드포인트 (/chat) ==========
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

  if (userInput.includes("시간대별 결제금액 순위")) {
    try {
      const salesRanking = await getSalesTimesRanking();
      const rankingText = salesRanking.map(item => item.displayText).join("<br>");
      return res.json({
        text: "시간대별 결제금액 순위입니다.<br>" + rankingText
      });
    } catch (error) {
      return res.status(500).json({ text: "시간대별 결제금액 데이터를 가져오는 중 오류가 발생했습니다." });
    }
  }

  if (userInput.includes("키워드별 구매 순위") || userInput.includes("키워드 순위")) {
    try {
      const keywordSales = await getTop10AdKeywordSales();
      const keywordListText = keywordSales.map(item => item.displayText).join("<br>");
      return res.json({
        text: "키워드별 구매 순위입니다.<br>" + keywordListText
      });
    } catch (error) {
      return res.status(500).json({ text: "키워드별 구매 데이터를 가져오는 중 오류가 발생했습니다." });
    }
  }

  if (userInput.includes("광고") && userInput.includes("순위")) {
    try {
      const adSales = await getTop10AdSales();
      const adSalesText = adSales.map(item => item.displayText).join("<br>");
      return res.json({
        text: "광고 매체별 구매 순위입니다.<br>" + adSalesText
      });
    } catch (error) {
      return res.status(500).json({ text: "광고 매체별 구매 데이터를 가져오는 중 오류가 발생했습니다." });
    }
  }

  if (userInput.includes("실제 방문자수")) {
    try {
      const visitorStats = await getDailyVisitorStats();
      const visitorText = visitorStats.join("<br>");
      return res.json({
        text: "조회 기간 동안의 일별 실제 방문자수입니다.<br>" + visitorText
      });
    } catch (error) {
      return res.status(500).json({ text: "실제 방문자수 데이터를 가져오는 중 오류가 발생했습니다." });
    }
  }
  
  if (userInput.includes("상세페이지 접속순위")) {
    try {
      const productViews = await getTop10ProductViews();
      const productViewsText = productViews.map(prod => prod.displayText).join("<br>");
      return res.json({
        text: "상세페이지 접속 순위 TOP 10 입니다.<br>" + productViewsText
      });
    } catch (error) {
      return res.status(500).json({ text: "상세페이지 접속 순위 데이터를 가져오는 중 오류가 발생했습니다." });
    }
  }

  return res.json({ text: "입력하신 메시지를 처리할 수 없습니다." });
});

// ========== [14] 상세페이지 접속 순위 조회 함수 ==========
async function getTop10ProductViews() {
  const { start_date, end_date } = getLastTwoWeeksDates();
  const url = 'https://ca-api.cafe24data.com/products/view';
  const params = {
    mall_id: 'yogibo',
    start_date,
    end_date
  };
  try {
    const response = await axios.get(url, {
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json'
      },
      params
    });
    console.log("Product View API 응답 데이터:", response.data);
    let products = response.data;
    if (products.count && Array.isArray(products.count)) {
      products = products.count;
    } else {
      throw new Error("Unexpected product view data structure");
    }
    products.sort((a, b) => b.count - a.count);
    const top10 = products.slice(0, 10);
    const updatedProducts = top10.map((item, index) => {
      return {
        rank: index + 1,
        product_no: item.product_no,
        product_name: item.product_name,
        count: item.count,
        displayText: `${index + 1}위: ${item.product_name} (상품번호: ${item.product_no}) - 조회수: ${item.count}`
      };
    });
    console.log("불러온 상세페이지 접속 순위 데이터:", updatedProducts);
    return updatedProducts;
  } catch (error) {
    console.error("Error fetching product view rankings:", error.response ? error.response.data : error.message);
    throw error;
  }
}

// ========== [15] 서버 시작 ==========
(async function initialize() {
  await getTokensFromDB();
  const PORT = process.env.PORT || 6000;
  app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
  });
})();
