const express = require("express");
const bodyParser = require("body-parser");
const fs = require("fs");
const path = require("path");
const cors = require("cors");
const compression = require("compression");
const axios = require("axios");
const { MongoClient } = require("mongodb");
require("dotenv").config();
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

const YOGIBO_SYSTEM_PROMPT = `
"너는 요기보 기업의 마케터로 빈백/소파 브랜드 전문 마케터로 데이터 분석및 차트 분석 다양한 데이터를 가지고 있어 또한 다양한 이벤트들을 기획단계부터 마케팅 광고에 까지
전문적인 지식을 가지고 있는 사람이야 대화에서 다양한 이모티콘을 활용하여 쉽고 친숙하게 대화를 이끌어줘"
`;

async function getGPT3TurboResponse(userInput, aggregatedData) {
  try {
    // 메시지 배열에 시스템 프롬프트와 추가 집계 데이터를 포함
    const messages = [
      { role: "system", content: YOGIBO_SYSTEM_PROMPT }
    ];
    
    // 집계 데이터가 있을 경우 추가 (예: "최근 캠페인 데이터: ...")
    if (aggregatedData) {
      messages.push({ role: "system", content: `집계 데이터: ${aggregatedData}` });
    }
    
    // 마지막에 사용자의 질문 추가
    messages.push({ role: "user", content: userInput });
    
    const response = await axios.post(
      OPEN_URL,
      {
        model: FINETUNED_MODEL,
        messages: messages
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
    console.error("OpenAI 호출 중 오류:", error.message);
    return "요기보 챗봇 오류가 발생했습니다. 다시 시도 부탁드립니다.";
  }
}





// ========== [5] 최근 30일(약 4주간) 날짜 계산 (optional 날짜 사용) ==========
function getLastTwoWeeksDates(providedDates) {
  // 프론트단에서 start_date와 end_date가 제공되면 해당 값을 사용
  if (providedDates && providedDates.start_date && providedDates.end_date) {
    return { start_date: providedDates.start_date, end_date: providedDates.end_date };
  }
  // 제공되지 않은 경우, 현재 기준 30일 전부터 오늘까지 사용
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
    if (response.data && response.data.product) {
      const product = response.data.product;
      console.log(`Product detail for ${product_no}:`, product.product_name);
      // product_name과 list_image를 함께 반환
      return { product_name: product.product_name, list_image: product.list_image };
    }
    return null;
  } catch (error) {
    console.error(`Error fetching product detail for product_no ${product_no}:`, error.response ? error.response.data : error.message);
    return null;
  }
}
// ========== [7] 장바구니에 담긴 수 기준 상위 10개 상품 조회 함수 ==========
async function getTop10ProductsByAddCart(providedDates) {
  const { start_date, end_date } = getLastTwoWeeksDates(providedDates);
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
        const detail = await getProductDetail(product.product_no);
        const finalName = detail ? detail.product_name : '상품';
        const listImage = detail ? detail.list_image : "";
        return {
          ...product,
          rank: index + 1,
          product_name: finalName,
          displayText: `
            <div class="product-ranking">
              <div class="rank">${index + 1}</div>
              <div class="image">
                <img src="${listImage}" alt="이미지"/>
              </div>
              <div class="details">
                <div class="product-name">${finalName}</div>
                <div class="product-count" >
                  총 <strong>${product.add_cart_count || 0}</strong> 개 상품이 담겨 있습니다.
                </div>
              </div>
            </div>
          `
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
async function getTop10PagesByView(providedDates) {
  const { start_date, end_date } = getLastTwoWeeksDates(providedDates);
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
    
    let pages;
    if (Array.isArray(response.data)) {
      pages = response.data;
    } else if (response.data && Array.isArray(response.data.view)) {
      pages = response.data.view;
    } else if (response.data && Array.isArray(response.data.pages)) {
      pages = response.data.pages;
    } else if (response.data && Array.isArray(response.data.data)) {
      pages = response.data.data;
    } else {
      throw new Error("Unexpected pages data structure");
    }
    
    const top10Pages = pages.slice(0, 10);
    const updatedPages = top10Pages.map((page, index) => {
      const urlMapping = {
        '/': '메인',
        '/product/detail.html': '상세페이지',
        '/product/list.html': '목록페이지',
        '/product/search.html': '검색페이지'
      };  
      const urlText = urlMapping[page.url] || page.url;
      const visitCount = page.visit_count || 0;
      const firstVisitCount = page.first_visit_count || 0;
      return {
        ...page,
        rank: index + 1,
        displayText: `
        <div class="product-ranking">
          <div class="rank">${index + 1}</div>
          <div class="details">
            <div class="product-name"><a href="https://yogibo.kr/${urlText}" target="_blank">${urlText}</div></a>
            <div class="product-count" >
             방문자수: ${visitCount}, 처음 접속수: ${firstVisitCount}
            </div>
          </div>
        </div>
      `   
      };
    });
    console.log("불러온 상위 10 페이지 데이터:", updatedPages);
    return updatedPages;
  } catch (error) {
    console.error("Error fetching pages:", error.response ? error.response.data : error.message);
    throw error;
  }
}

//원단위 데이터 
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

// ========== [9] 시간대별 결제금액 순위 조회 함수 ==========
async function getSalesTimesRanking(providedDates) {
  const { start_date, end_date } = getLastTwoWeeksDates(providedDates);
  const url = 'https://ca-api.cafe24data.com/sales/times';
  // limit는 충분히 크게 설정하여 전체 데이터를 받아오도록 함
  const params = {
    mall_id: 'yogibo',
    shop_no: 1,
    start_date,
    end_date,
    limit: 100,
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
    let times;
    if (Array.isArray(response.data)) {
      times = response.data;
    } else if (response.data && Array.isArray(response.data.times)) {
      times = response.data.times;
    } else if (response.data && Array.isArray(response.data.data)) {
      times = response.data.data;
    } else {
      throw new Error("Unexpected sales times data structure");
    }

    // 0시부터 23시까지 기본값(구매자수, 구매건수, 매출액 모두 0)을 가진 배열 생성
    const hourlyData = Array.from({ length: 24 }, (_, hour) => ({
      hour,
      buyers_count: 0,
      order_count: 0,
      order_amount: 0
    }));

    // API 데이터로 해당 시간대의 값을 업데이트 (동일 시간대가 여러 건이면 덮어쓰거나 누적 처리)
    times.forEach(time => {
      const h = Number(time.hour);
      if (!isNaN(h) && h >= 0 && h < 24) {
        // 만약 여러 건이 있다면 누적하거나 최신 데이터로 대체할 수 있음 (여기서는 대체)
        hourlyData[h].buyers_count = time.buyers_count || 0;
        hourlyData[h].order_count = time.order_count || 0;
        hourlyData[h].order_amount = time.order_amount || 0;
      }
    });

    // 각 시간대를 00시, 01시, ... 23시 형식으로 표시하도록 구성
    const updatedTimes = hourlyData.map((time) => {
      const hourLabel = time.hour < 10 ? "0" + time.hour : time.hour;
      const formattedAmount = formatCurrency(time.order_amount);
      return {
        rank: time.hour,
        hour: time.hour,
        buyers_count: time.buyers_count,
        order_count: time.order_count,
        order_amount: time.order_amount,
        displayText: `
          <div class="sales-ranking" style="display:flex; align-items:center; gap:10px; padding:5px; border:1px solid #ddd; border-radius:5px; background:#fff;">
            <div class="rank" style="font-weight:bold; min-width:50px;">${hourLabel}시</div>
            <div class="details" style="display:flex; flex-direction:column;">
              <div class="buyers">구매자수: ${time.buyers_count}</div>
              <div class="orders">구매건수: ${time.order_count}</div>
              <div class="amount">매출액: ${formattedAmount}</div>
            </div>
          </div>
        `
      };
    });

    console.log("불러온 00~23시 시간대별 결제금액 데이터:", updatedTimes);
    return updatedTimes;
  } catch (error) {
    console.error("Error fetching sales times:", error.response ? error.response.data : error.message);
    throw error;
  }
}


//시간대별 매출 통계
app.get("/salesHourly", async (req, res) => {
  const providedDates = {
    start_date: req.query.start_date,
    end_date: req.query.end_date
  };
  try {
    const hourlyData = await getSalesTimesRanking(providedDates);
    res.json(hourlyData);
  } catch (error) {
    console.error("Error fetching sales times (salesHourly):", error.response ? error.response.data : error.message);
    res.status(500).json({ error: "시간대별 매출 데이터를 가져오는 중 오류 발생" });
  }
});



// ========== [10] 광고 매체별 구매 순위 조회 함수 ==========
async function getTop10AdSales(providedDates) {
  const { start_date, end_date } = getLastTwoWeeksDates(providedDates);
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
    let adsales = [];
    if (response.data && response.data.adsales && Array.isArray(response.data.adsales)) {
      adsales = response.data.adsales;
    } else if (response.data && Array.isArray(response.data)) {
      adsales = response.data;
    } else {
      throw new Error("Unexpected ad sales data structure");
    }
    const top10 = adsales.slice(0, 5);
    const updatedTop10 = top10.map((item, index) => {
      const formattedAmount = formatCurrency(item.order_amount);
      return {
        rank: index + 1,
        ad: item.ad,
        order_count: item.order_count,
        order_amount: item.order_amount,
        displayText: `
          <div class="keyword-ranking" style="display:flex; align-items:center; gap:10px; padding:5px; border:1px solid #ddd; border-radius:5px; background:#fff;">
            <div class="rank" style="font-weight:bold; min-width:50px;">${index + 1}위</div>
            <div class="details" style="display:flex; flex-direction:column;">
              <div class="keyword">광고: ${item.ad}</div>
              <div class="orders">구매건수: ${item.order_count}</div>
              <div class="amount">매출액: ${formattedAmount}</div>
            </div>
          </div>
        `
      };
    });
    console.log("불러온 광고 매체별 구매 순위 데이터:", updatedTop10);
    return updatedTop10;
  } catch (error) {
    console.error("Error fetching ad sales:", error.response ? error.response.data : error.message);
    throw error;
  }
}

// ========== 서버측: /adSalesGraph 엔드포인트 추가 ==========
app.get("/adSalesGraph", async (req, res) => {
  const providedDates = {
    start_date: req.query.start_date,
    end_date: req.query.end_date
  };
  try {
    const adSales = await getTop10AdSales(providedDates);
    // 광고 이름과 매출액 데이터를 차트에 사용할 수 있도록 추출
    const labels = adSales.map(item => item.ad);
    const orderAmounts = adSales.map(item => item.order_amount);
    res.json({ labels, orderAmounts });
  } catch (error) {
    console.error("Error fetching ad sales graph data:", error.response ? error.response.data : error.message);
    res.status(500).json({ error: "광고 매체별 판매 데이터를 가져오는 중 오류 발생" });
  }
});
async function getDailyVisitorStats(providedDates) {
  const { start_date, end_date } = getLastTwoWeeksDates(providedDates);
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
    // 디버깅: 응답 데이터의 키 확인
    console.log("Response keys:", Object.keys(response.data));
    
    let stats;
    if (Array.isArray(response.data)) {
      stats = response.data;
    } else if (response.data && Array.isArray(response.data.unique)) {
      stats = response.data.unique;
    } else if (response.data && Array.isArray(response.data.view)) {
      stats = response.data.view;
    } else if (response.data && Array.isArray(response.data.data)) {
      stats = response.data.data;
    } else if (response.data && Array.isArray(response.data.visitors)) {
      stats = response.data.visitors;
    } else {
      throw new Error("Unexpected daily visitor stats data structure");
    }
    
    console.log("Extracted stats length:", stats.length);
    // visit_count 기준 내림차순 정렬 (필요에 따라 제거 가능)
    stats.sort((a, b) => b.visit_count - a.visit_count);
    
    // 각 항목에 대해 순위 없이 날짜와 수치만 구성
    const updatedStats = stats.map(item => {
      const formattedDate = new Date(item.date).toISOString().split('T')[0];
      return `${formattedDate} <br/>- 방문자수: ${item.visit_count}, 처음 방문수: ${item.first_visit_count}, 재방문수: ${item.re_visit_count}`;
    });
    console.log("불러온 일별 방문자수 데이터:", updatedStats);
    return updatedStats;
  } catch (error) {
    console.error("Error fetching daily visitor stats:", error.response ? error.response.data : error.message);
    throw error;
  }
}


app.get("/dailyVisitorStats", async (req, res) => {
  const providedDates = {
    start_date: req.query.start_date,
    end_date: req.query.end_date
  };
  try {
    const stats = await getDailyVisitorStats(providedDates);
    res.json(stats);
  } catch (error) {
    console.error("Error fetching daily visitor stats:", error.response ? error.response.data : error.message);
    res.status(500).json({ error: "일별 방문자 데이터를 가져오는 중 오류가 발생했습니다." });
  }
});




// ========== [12] 상세페이지 접속 순위 조회 함수 ==========
async function getTop10ProductViews(providedDates) {
  const { start_date, end_date } = getLastTwoWeeksDates(providedDates);
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
    
    // 응답 데이터가 문자열이면 JSON 파싱
    let data = response.data;
    if (typeof data === "string") {
      try {
        data = JSON.parse(data);
      } catch (e) {
        console.error("응답 데이터를 JSON으로 파싱하는 데 실패:", e);
        throw new Error("응답 데이터가 유효한 JSON이 아닙니다.");
      }
    }
    
    // 배열 추출: 우선 data.view, 없으면 data.count, 그 외 배열이면 그대로 사용
    let products = [];
    if (data && Array.isArray(data.view)) {
      products = data.view;
    } else if (data && Array.isArray(data.count)) {
      products = data.count;
    } else if (Array.isArray(data)) {
      products = data;
    } else {
      console.error("Unexpected product view data structure:", data);
      throw new Error("Unexpected product view data structure");
    }
    
    // 유효 항목 필터링: product_no와 count가 있는지 확인
    products = products.filter(item => item.product_no && typeof item.count === "number");
    
    if (products.length === 0) {
      console.log("조회된 상품 뷰 데이터가 없습니다.");
      return [];
    }
    
    // 조회수(count) 기준 내림차순 정렬
    products.sort((a, b) => b.count - a.count);
    const top10 = products.slice(0, 10);
    
    // 각 항목에 대해 product_no를 활용해 상세 API 호출, 상세의 product_name 사용
    const updatedProducts = await Promise.all(
      top10.map(async (item, index) => {
        const detail = await getProductDetail(item.product_no);
        // detail이 존재하면 detail.product_name, 그렇지 않으면 item.product_name 객체에서 product_name 추출
        const finalName = (detail && detail.product_name) ||
                          (item.product_name && item.product_name.product_name) ||
                          '상품';
        // detail이 있으면 이미지 URL, 없으면 빈 문자열
        const listImage = detail ? detail.list_image : "";
        
        return {
          rank: index + 1,
          product_no: item.product_no,
          product_name: finalName,
          count: item.count,
          displayText: `
            <div class="product-ranking">
              <div class="rank">${index + 1}</div>
              <div class="image">
                <img src="${listImage}" alt="이미지"/>
              </div>
              <div class="details">
                <div class="product-name">${finalName}</div>
                <div class="product-count">
                  조회수: ${item.count}
                </div>
              </div>
            </div>
          `
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

// ========== [13] 광고별 유입수 순위 조회 함수 ==========
async function getTop10AdInflow(providedDates) {
  const { start_date, end_date } = getLastTwoWeeksDates(providedDates);
  const url = 'https://ca-api.cafe24data.com/visitpaths/ads';
  const params = {
    mall_id: 'yogibo',
    start_date,
    end_date
    // 필요시 shop_no, device_type 등 추가
  };

  try {
    const response = await axios.get(url, {
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json'
      },
      params
    });
    console.log("Ad Inflow API 응답 데이터:", response.data);
    let data = response.data;
    let ads = [];
    if (data.ads && Array.isArray(data.ads)) {
      ads = data.ads;
    } else {
      throw new Error("Unexpected ad inflow data structure");
    }
    // 순방문자수(visit_count)를 숫자로 변환 후 내림차순 정렬
    ads.sort((a, b) => Number(b.visit_count) - Number(a.visit_count));
    const top10 = ads.slice(0, 10);
    const updatedAds = top10.map((item, index) => {
      return {
        rank: index + 1,
        ad: item.ad === "채널 없음" ? "북마크" : item.ad,
        visit_count: item.visit_count,
        displayText: `${index + 1}위: ${item.ad} - 순방문자수: ${item.visit_count}`
      };
    });
    console.log("불러온 광고별 유입수 데이터:", updatedAds);
    return updatedAds;
  } catch (error) {
    console.error("Error fetching ad inflow data:", error.response ? error.response.data : error.message);
    throw error;
  }
}
app.get("/adInflowGraph", async (req, res) => {
  const providedDates = {
    start_date: req.query.start_date,
    end_date: req.query.end_date
  };
  try {
    const adInflow = await getTop10AdInflow(providedDates);
    // 차트에 사용할 데이터: 각 광고명과 해당 유입수
    const labels = adInflow.map(item => item.ad);
    const visitCounts = adInflow.map(item => Number(item.visit_count));
    res.json({ labels, visitCounts });
  } catch (error) {
    console.error("Error fetching ad inflow graph data:", error.response ? error.response.data : error.message);
    res.status(500).json({ error: "광고별 유입수 데이터를 가져오는 중 오류 발생" });
  }
});



// ========== [14] 키워드별 구매 순위 조회 함수 (기존 코드) ==========
async function getTop10AdKeywordSales(providedDates) {
  const { start_date, end_date } = getLastTwoWeeksDates(providedDates);
  const url = 'https://ca-api.cafe24data.com/visitpaths/keywordsales';
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
    console.log("Keyword Sales API 응답 데이터:", response.data);
    let data = response.data;
    let sales = [];
    if (data.keywordsales && Array.isArray(data.keywordsales)) {
      sales = data.keywordsales;
    } else {
      throw new Error("Unexpected keyword sales data structure");
    }
    // 동일 키워드별로 주문 건수와 매출액 합산
    const groupByKeyword = {};
    sales.forEach(item => {
      const keyword = item.keyword || 'N/A';
      if (!groupByKeyword[keyword]) {
        groupByKeyword[keyword] = {
          keyword,
          order_count: 0,
          order_amount: 0
        };
      }
      groupByKeyword[keyword].order_count += item.order_count || 0;
      groupByKeyword[keyword].order_amount += item.order_amount || 0;
    });
    const groupedArray = Object.values(groupByKeyword);
    groupedArray.sort((a, b) => b.order_amount - a.order_amount);
    const top10 = groupedArray.slice(0, 12);
    const updatedTop10 = top10.map((item, index) => {
      const formattedAmount = Number(item.order_amount).toLocaleString('ko-KR') + " 원";
      return {
        rank: index + 1,
        keyword: item.keyword,
        order_count: item.order_count,
        order_amount: item.order_amount,
        displayText: `
          <div class="keyword-ranking" style="display:flex; align-items:center; gap:10px; padding:5px; border:1px solid #ddd; border-radius:5px; background:#fff;">
            <div class="rank" style="font-weight:bold;">${index + 1}</div>
            <div class="details" style="display:flex; flex-direction:column;">
              <div class="keyword">${item.keyword}</div>
              <div class="orders">구매건수: ${item.order_count}</div>
              <div class="amount">매출액: ${formattedAmount}</div>
            </div>
          </div>
        `
      };
    });
    console.log("불러온 키워드별 구매 순위 데이터:", updatedTop10);
    return updatedTop10;
  } catch (error) {
    console.error("Error fetching keyword sales:", error.response ? error.response.data : error.message);
    throw error;
  }
}

// ========== [새 엔드포인트] 키워드별 구매 순위 차트 데이터 반환 ==========
app.get("/keywordSalesGraph", async (req, res) => {
  const providedDates = {
    start_date: req.query.start_date,
    end_date: req.query.end_date
  };
  try {
    const keywordSales = await getTop10AdKeywordSales(providedDates);
    // 차트용 데이터: 각 키워드와 해당 매출액
    const labels = keywordSales.map(item => item.keyword);
    const orderAmounts = keywordSales.map(item => item.order_amount);
    res.json({ labels, orderAmounts });
  } catch (error) {
    console.error("Error fetching keyword sales graph data:", error.response ? error.response.data : error.message);
    res.status(500).json({ error: "검색 키워드별 구매 데이터를 가져오는 중 오류 발생" });
  }
});



// ========== 실시간 판매 순위 에 대한 데이터 를 가졍괴 ==========
// 1. 카테고리 상품 조회
async function getCategoryProducts(category_no) {
    // URL 주소가 반드시 포함되어야 합니다.
    const url = `https://yogibo.cafe24api.com/api/v2/admin/categories/${category_no}/products`;
    const params = { display_group: 1 };
    try {
        const data = await apiRequest('GET', url, {}, params);
        console.log(`카테고리 ${category_no}의 상품 수:`, data.products.length);
        return data.products;
    } catch (error) {
        console.error('카테고리 상품 조회 오류:', error.message);
        throw error;
    }
}

// 2. 특정 상품들의 판매 데이터 조회
async function getSalesDataForProducts(productNos, start_date, end_date) {
    const url = `https://yogibo.cafe24api.com/api/v2/admin/reports/salesvolume`;
    const params = {
        shop_no: 1,
        start_date,
        end_date,
        product_no: productNos.join(','),
    };
    try {
        const data = await apiRequest('GET', url, {}, params);
        console.log('판매 데이터 조회 완료:', data.salesvolume.length);
        return data.salesvolume;
    } catch (error) {
        console.error('판매 데이터 조회 오류:', error.message);
        throw error;
    }
}

// 3. 판매 순위 계산 및 정렬
function calculateAndSortRanking(categoryProducts, salesData) {
    // 카테고리 상품의 product_no 목록 생성
    const productNosSet = new Set(categoryProducts.map(p => p.product_no));
    // 판매 데이터 중 해당 카테고리 상품에 해당하는 데이터만 필터링
    const filteredSales = salesData.filter(item => productNosSet.has(item.product_no));
    
    // 동일 상품번호의 데이터 합산 (판매 수량, 판매 금액)
    const mergedData = filteredSales.reduce((acc, curr) => {
        const existing = acc.find(item => item.product_no === curr.product_no);
        // product_price를 숫자로 처리 (문자열일 경우 replace 후 파싱, 숫자일 경우 그대로 사용)
        const currPrice = typeof curr.product_price === 'string' 
                          ? parseInt(curr.product_price.replace(/,/g, ''), 10)
                          : curr.product_price;
        if (existing) {
            existing.total_sales += parseInt(curr.total_sales, 10);
            existing.product_price += currPrice;
        } else {
            acc.push({
                ...curr,
                total_sales: parseInt(curr.total_sales, 10),
                product_price: currPrice
            });
        }
        return acc;
    }, []);
    
    // 각 상품별 계산된 총 판매 금액 (판매금액 * 판매수량)
    const rankedData = mergedData.map(item => ({
        ...item,
        calculated_total_price: item.product_price * item.total_sales
    }));
    
    // 내림차순 정렬 및 순위 번호 부여
    rankedData.sort((a, b) => b.calculated_total_price - a.calculated_total_price);
    rankedData.forEach((item, index) => {
        item.rank = index + 1;
    });
    
    return rankedData;
}
async function getRealTimeSalesRanking(providedDates) {
  let start_date, end_date;
  if (providedDates && providedDates.start_date && providedDates.end_date) {
    start_date = providedDates.start_date;
    end_date = providedDates.end_date;
  } else {
    // 기간 미지정 시: 현재 날짜를 end_date로, 30일 전을 start_date로 설정
    const now = new Date();
    end_date = now.toISOString().split('T')[0];
    const pastDate = new Date(now);
    pastDate.setDate(now.getDate() - 30);
    start_date = pastDate.toISOString().split('T')[0];
  }

  try {
    console.log(`실시간 판매 순위 데이터 수집 시작: ${start_date} ~ ${end_date}`);
    
    // 1. 카테고리 상품 조회 (URL 주소 그대로 유지)
    const categoryProducts = await getCategoryProducts(CATEGORY_NO);
    if (!categoryProducts || categoryProducts.length === 0) {
      return "해당 카테고리에는 상품이 없습니다.";
    }
    const productNos = categoryProducts.map(p => p.product_no);
    console.log("카테고리 상품 번호:", productNos);

    // 2. 판매 데이터 조회 (URL 주소 그대로 유지)
    const salesData = await getSalesDataForProducts(productNos, start_date, end_date);
    if (!salesData || salesData.length === 0) {
      return "판매 데이터가 없습니다.";
    }

    // 3. 판매 순위 계산 및 정렬
    const rankedData = calculateAndSortRanking(categoryProducts, salesData);
    console.log('계산된 순위 데이터:', rankedData);

    let finalRankings = rankedData;
    if (typeof compareRankings === 'function') {
      finalRankings = await compareRankings(rankedData);
      console.log('업데이트된 순위 데이터:', finalRankings);
    }

    // 4. 각 상품별 상세 정보를 가져와 상품명과 이미지를 추가 (getProductDetail 참고)
    const finalRankingsWithDetails = await Promise.all(finalRankings.map(async (item) => {
      const detail = await getProductDetail(item.product_no);
      const finalName = detail ? detail.product_name : '상품';
      const listImage = detail ? detail.list_image : "";
      return {
        ...item,
        finalName,
        listImage
      };
    }));

    // 5. 결과 HTML 포맷팅 (상품명과 이미지 포함)
    let output = `<div style="font-weight:bold; margin-bottom:10px;">실시간 판매 순위 (기간: ${start_date} ~ ${end_date}):</div>`;
    finalRankingsWithDetails.forEach(item => {
      output += `<div style="margin-bottom:5px; border-bottom:1px solid #ccc; padding:5px 0;">
        <div style="font-size:16px; font-weight:bold;">순위 ${item.rank}:</div>
        <div class="product-name">${item.finalName}</div>
        <img src="${item.listImage}" alt="이미지" style="max-width:100px; display:block; margin-bottom:5px;"/>
        <div>상품번호: ${item.product_no}</div>
        <div>판매수량: ${item.total_sales}</div>
        <div>총매출액: ${item.calculated_total_price}</div>
      </div>`;
    });

    return output;
  } catch (error) {
    console.error('실시간 판매 순위 데이터 수집 오류:', error.message);
    return "실시간 판매 순위 데이터를 가져오는 중 오류가 발생했습니다.";
  }
}



// ========== [16] 채팅 엔드포인트 (/chat) ==========
app.post("/chat", async (req, res) => {
  await refreshAccessToken();
  const userInput = req.body.message;
  const memberId = req.body.memberId;
  const providedDates = {
    start_date: req.body.start_date,
    end_date: req.body.end_date
  };

  if (!userInput) {
    return res.status(400).json({ error: "Message is required" });
  }

  try {
    // 기존 조건 처리 (장바구니, 페이지뷰, 결제금액, 키워드 순위, 광고별 판매 순위, 광고별 유입수, 일별 방문자, 상세페이지 접속 순위)
    if (userInput.includes("기간별 장바구니 순위")) {
      const topProducts = await getTop10ProductsByAddCart(providedDates);
      const productListText = topProducts.map(prod => prod.displayText).join("<br>");
      return res.json({ text: productListText });
    }

    if (userInput.includes("기간별 페이지뷰 순위") || userInput.includes("페이지 뷰")) {
      const topPages = await getTop10PagesByView(providedDates);
      const pageListText = topPages.map(page => page.displayText).join("<br>");
      return res.json({ text: pageListText });
    }

    if (userInput.includes("시간대별 결제 금액 추이")) {
      const salesRanking = await getSalesTimesRanking(providedDates);
      const rankingText = salesRanking.map(item => item.displayText).join("<br>");
      return res.json({ text: "시간대별 결제금액 순위입니다.<br>" + rankingText });
    }

    if (userInput.includes("검색 키워드별 구매 순위") || userInput.includes("키워드 순위")) {
      const keywordSales = await getTop10AdKeywordSales(providedDates);
      const keywordListText = keywordSales.map(item => item.displayText).join("<br>");
      return res.json({ text: keywordListText });
    }

    if (userInput.includes("광고별 판매 순위") && userInput.includes("순위")) {
      const adSales = await getTop10AdSales(providedDates);
      const adSalesText = adSales.map(item => item.displayText).join("<br>");
      return res.json({ text: adSalesText });
    }

    if (userInput.includes("광고별 자사몰 유입수")) {
      const adInflow = await getTop10AdInflow(providedDates);
      const adInflowText = adInflow.map(item => item.displayText).join("<br>");
      return res.json({ text: "광고별 유입수 순위 TOP 10 입니다.<br>" + adInflowText });
    }

    if (userInput.includes("일별 방문자 확인")) {
      const visitorStats = await getDailyVisitorStats(providedDates);
      const visitorText = visitorStats.join("<br>");
      return res.json({ text: "조회 기간 동안의 일별 실제 방문자 순위입니다.<br>" + visitorText });
    }

    if (userInput.includes("상세페이지 접속 순위")) {
      const productViews = await getTop10ProductViews(providedDates);
      const productViewsText = productViews.map(prod => prod.displayText).join("<br>");
      return res.json({ text: productViewsText });
    }

    if (userInput.includes("실시간 판매순위")) {
      const realTimeRanking = await getRealTimeSalesRanking(providedDates);
      return res.json({ text: realTimeRanking });
    }

    // 프롬프트 기능: 집계된 데이터를 기반으로 질문하는 경우 추가 컨텍스트 제공
    let aggregatedData = "";
    if (userInput.includes("최근 캠페인 데이터") || userInput.includes("데이터 분석")) {
      // 예시: 집계 데이터를 요약한 문자열 (실제 데이터에 맞게 수정 필요)
      aggregatedData = "The latest campaign data shows a 12% increase in engagement and a 15% increase in conversions compared to the previous quarter. 📈💡";
    }

    // 위 조건에 해당하지 않으면 GPT 프롬프트를 사용하여 일반 응답 생성
    const gptResponse = await getGPT3TurboResponse(userInput, aggregatedData);
    return res.json({ text: gptResponse });
  } catch (error) {
    console.error("Error in /chat endpoint:", error.response ? error.response.data : error.message);
    return res.status(500).json({ text: "메시지를 처리하는 중 오류가 발생했습니다." });
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
