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

const YOGIBO_SYSTEM_PROMPT = `
"너는 요기보 기업의 마케터로 빈백/소파 브랜드 전문 마케터로 데이터 분석및 차트 분석 다양한 데이터를 가지고 있어 또한 다양한 이벤트들을 기획단계부터 마케팅 광고에 까지
전문적인 지식을 가지고 있는 사람이야 대화에서 다양한 이모티콘을 활용하여 쉽고 친숙하게 대화를 이끌어줘"
`;
async function getGPT3TurboResponse(userInput, chatHistory) {
  try {
    // 시스템 프롬프트 추가
    const messages = [
      { role: "system", content: YOGIBO_SYSTEM_PROMPT }
    ];
    // 클라이언트에서 전달받은 이전 대화 내역(chatHistory)이 있으면 메시지 배열에 추가
    if (chatHistory && Array.isArray(chatHistory) && chatHistory.length > 0) {
      messages.push(...chatHistory);
    }
    // 최신 사용자 입력 추가
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
      console.log(`Product detail for ${product_no}:`, product.product_name, product.list_image, product.price,product.summary_description);
      // product_name, list_image, 그리고 price를 함께 반환
      return { 
        product_name: product.product_name, 
        list_image: product.list_image,
        price: product.price,
        summary:product.summary_description,
        productNo:product.product_no
      };
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
    limit: 500,
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
    
    // 배열 추출: data.view, data.count, 그 외 배열이면 그대로 사용
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
    
    // 조회수(count) 기준 내림차순 정렬 후 상위 10개 선택
    products.sort((a, b) => b.count - a.count);
    const top10 = products.slice(0, 10);
    
    // 각 항목에 대해 product_no를 활용해 상세 API 호출, 상세의 product_name 사용
    const updatedProducts = await Promise.all(
      top10.map(async (item, index) => {
        const detail = await getProductDetail(item.product_no);
        // detail이 존재하면 detail.product_name, 없으면 item.product_name(문자열 그대로) 사용
        const finalName = (detail && detail.product_name) || item.product_name || '상품';
        // detail이 있으면 이미지 URL, 없으면 빈 문자열
        const listImage = (detail && detail.list_image) || "";
        
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
        calculated_total_price: item.product_price
    }));
    
    // 내림차순 정렬 및 순위 번호 부여
    rankedData.sort((a, b) => b.calculated_total_price - a.calculated_total_price);
    rankedData.forEach((item, index) => {
        item.rank = index + 1;
    });
    
    return rankedData;
}
async function getRealTimeSalesRanking(categoryNo, providedDates) {
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
    console.log(`실시간 판매 순위 데이터 수집 시작 (카테고리 ${categoryNo}): ${start_date} ~ ${end_date}`);
    
    // 1. 카테고리 상품 조회 (categoryNo 사용)
    const categoryProducts = await getCategoryProducts(categoryNo);
    if (!categoryProducts || categoryProducts.length === 0) {
      return "해당 카테고리에는 상품이 없습니다.";
    }
    const productNos = categoryProducts.map(p => p.product_no);
    console.log("카테고리 상품 번호:", productNos);

    // 2. 판매 데이터 조회
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

    // 4. 각 상품별 상세 정보를 가져와 상품명과 이미지를 추가
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

    // 판매수량이 0인 항목은 필터링
    const filteredRankings = finalRankingsWithDetails.filter(item => item.total_sales > 0);
    if (filteredRankings.length === 0) {
      return "해당 기간 내에 판매된 상품이 없습니다.";
    }

    // 5. 결과 HTML 포맷팅 (상품명, 이미지, 총매출액은 원화로 표시)
    let output = `<div style="font-weight:bold; margin-bottom:10px;">판매 순위 (기간: ${start_date} ~ ${end_date})</div>`;
    filteredRankings.forEach(item => {
      output += `<div class="product-ranking" style="margin-bottom:10px; border-bottom:1px solid #ccc; padding:5px 0;">
        <div class="rank"> ${item.rank}</div>
        <div class="image">
          <img src="${item.listImage}" alt="이미지" style="max-width:100px;"/>
        </div>     
        <div class="details">
          <div class="product-name">${item.finalName}</div>
          <div>판매수량: ${item.total_sales}</div>
          <div>총매출액: ${formatCurrency(item.calculated_total_price)}</div>
        </div>
      </div>`;
    });
    return output;
  } catch (error) {
    console.error('실시간 판매 순위 데이터 수집 오류:', error.message);
    return "실시간 판매 순위 데이터를 가져오는 중 오류가 발생했습니다.";
  }
}


// ========== [12] 전체 상세페이지 접속 순위 조회 함수 (getView) ==========
async function getView(providedDates) {
  const { start_date, end_date } = getLastTwoWeeksDates(providedDates);
  const url = 'https://ca-api.cafe24data.com/products/view';
  const params = {
    mall_id: 'yogibo',
    start_date,
    end_date,
    limit:1000,
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
    
    let data = response.data;
    if (typeof data === "string") {
      try {
        data = JSON.parse(data);
      } catch (e) {
        console.error("응답 데이터를 JSON으로 파싱하는 데 실패:", e);
        throw new Error("응답 데이터가 유효한 JSON이 아닙니다.");
      }
    }
    
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
    
    products = products.filter(item => item.product_no && typeof item.count === "number");
    return products;
  } catch (error) {
    console.error("Error fetching product view data:", error.response ? error.response.data : error.message);
    throw error;
  }
}

// ====27272727722727====== [12] 이벤트 페이지 클릭률 (카테고리 상세페이지 접속 순위) ==========
async function getCategoryProductViewRanking(category_no, providedDates) {
  try {
    // 1. 카테고리 내 상품 목록 조회
    const categoryProducts = await getCategoryProducts(category_no);
    if (!categoryProducts || categoryProducts.length === 0) {
      console.log(`카테고리 ${category_no}에는 등록된 상품이 없습니다.`);
      return [];
    }
    // 카테고리 상품의 product_no 목록 Set 생성
    const categoryProductNos = new Set(categoryProducts.map(product => product.product_no));
    console.log(`카테고리 ${category_no}의 product_no 목록:`, Array.from(categoryProductNos));
    
    // 2. 전체 상세페이지 접속 순위 데이터 조회 (getView 함수 사용)
    const allViewData = await getView(providedDates);
    if (!allViewData || allViewData.length === 0) {
      console.log("전체 상세페이지 접속 순위 데이터가 없습니다.");
      return [];
    }
    
    // 3. 카테고리 상품의 product_no에 해당하는 항목만 필터링
    const filteredViewData = allViewData.filter(item => categoryProductNos.has(item.product_no));
    if (filteredViewData.length === 0) {
      console.log("해당 카테고리의 상세페이지 접속 순위 데이터가 없습니다.");
      return [];
    }
    
    // 4. 조회수(count) 기준 내림차순 정렬 후 순위(rank) 부여
    filteredViewData.sort((a, b) => b.count - a.count);
    filteredViewData.forEach((item, index) => {
      item.rank = index + 1;
    });
    
    // 5. 각 항목의 product_no를 활용해 상세 정보(product_name, list_image) 불러오기
    const finalData = await Promise.all(
      filteredViewData.map(async (item) => {
        const detail = await getProductDetail(item.product_no);
        return {
          ...item,
          product_name: detail ? detail.product_name : item.product_name,
          list_image: detail ? detail.list_image : ""
        };
      })
    );
    
    console.log("최종 필터링된 상세페이지 접속 순위 데이터:", finalData);
    return finalData;
  } catch (error) {
    console.error("카테고리 상세페이지 접속 순위 데이터 조회 오류:", error.response ? error.response.data : error.message);
    throw error;
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
  const chatHistory = req.body.chatHistory || [];
  if (!userInput) {
    return res.status(400).json({ error: "Message is required" });
  }

  try {
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

    if (userInput.includes("상세페이지 접속 순위") && !userInput.includes("클릭률")) {
      const productViews = await getTop10ProductViews(providedDates);
      const productViewsText = productViews.map(prod => prod.displayText).join("<br>");
      return res.json({ text: productViewsText });
    }

    if (userInput.includes("소파 실시간 판매순위")) {
      const realTimeRanking = await getRealTimeSalesRanking(858, providedDates);
      return res.json({ text: realTimeRanking });
    }
    
    if (userInput.includes("바디필로우 실시간 판매순위")) {
      const realTimeRanking = await getRealTimeSalesRanking(876, providedDates);
      return res.json({ text: realTimeRanking });
    }

    //실시간 클릴률
    const clickRateMatch = userInput.match(/^(\d+)\s*클릭률/);
      if (clickRateMatch) {
        const categoryNo = parseInt(clickRateMatch[1], 10);
        const filteredViewData = await getCategoryProductViewRanking(categoryNo, providedDates);
        if (!filteredViewData || filteredViewData.length === 0) {
          return res.json({ text: "해당 카테고리의 클릭률 데이터를 찾을 수 없습니다." });
        }
        const displayText = filteredViewData.map(item => {
          return `
          <div class="product-ranking" style="margin-bottom:10px; border-bottom:1px solid #ccc; padding:5px 0;">
            <div class="rank">${item.rank}</div>
            <div class="image">
              <img src="${item.list_image}" alt="${item.product_name}" style="max-width:100px;"/>
            </div>
              <div class="details">
                <div class="product-name">${item.product_name}</div>
                <div class="product-count" style="font-size:14px;color:#ff0000;">클릭률: ${item.count}</div>
              </div>
            </div>
          `;
        }).join("<br>");
        return res.json({ text: displayText });
      }

      if (userInput.includes("이벤트 상품 클릭률")) {
        // 두 개의 카테고리 번호를 배열로 정의
        const categories = [632, 629,630,899,901,994,995];
        
        // Promise.all을 사용해 각 카테고리의 데이터를 병렬로 호출
        const results = await Promise.all(
          categories.map(async (categoryNo) => {
            const data = await getCategoryProductViewRanking(categoryNo, providedDates);
            return { categoryNo, data };
          })
        );
      
        // 두 카테고리의 결과를 하나의 배열로 합침
        const mergedData = results.reduce((acc, cur) => {
          if (cur.data && cur.data.length > 0) {
            return acc.concat(cur.data);
          }
          return acc;
        }, []);
      
        if (!mergedData || mergedData.length === 0) {
          return res.json({ text: "해당 카테고리(960, 961번)의 클릭률 데이터를 찾을 수 없습니다." });
        }
      
        // 최종 데이터를 정렬하거나 별도로 가공할 수 있음 (예: rank 재설정 등)
        mergedData.sort((a, b) => b.count - a.count);
        mergedData.forEach((item, index) => {
          item.rank = index + 1;
        });
      
        const displayText = mergedData.map(item => {
          return `
          <div class="product-ranking" style="margin-bottom:10px; border-bottom:1px solid #ccc; padding:5px 0;">
            <div class="rank">${item.rank}</div>
            <div class="image">
              <img src="${item.list_image}" alt="${item.product_name}" style="max-width:100px;"/>
            </div>
              <div class="details">
                <div class="product-name">${item.product_name}</div>
                <div class="product-count" style="font-size:14px;color:#ff0000;">클릭률: ${item.count}회</div>
              </div>
            </div>
          `;
        }).join("<br>");
        
        return res.json({ text: displayText });
      }
      

    // 집계 데이터: 클라이언트에서 chatHistory 배열을 전달한다고 가정
    let aggregatedData = "";
    if (userInput.includes("데이터 분석") || userInput.includes("이젠 데이터 분석")) {
      aggregatedData = (chatHistory && Array.isArray(chatHistory) && chatHistory.length > 0)
        ? chatHistory.map(msg => msg.content).join("\n")
        : "집계 데이터가 준비되지 않았습니다.";
    }
    
    const gptResponse = await getGPT3TurboResponse(userInput, chatHistory);
    return res.json({ text: gptResponse });
  } catch (error) {
    console.error("Error in /chat endpoint:", error.response ? error.response.data : error.message);
    return res.status(500).json({ text: "메시지를 처리하는 중 오류가 발생했습니다." });
  }
});

app.get("/api/v2/admin/products/search", async (req, res) => {
  const dataValue = req.query.dataValue;
  console.log("Received dataValue from client:", dataValue);
  if (!dataValue) {
    return res.status(400).json({ error: "dataValue query parameter is required" });
  }

  const mallid = process.env.CAFE24_MALLID || "yogibo";
  const url = `https://${mallid}.cafe24api.com/api/v2/admin/products?fields=product_name,product_no&product_name=${encodeURIComponent(dataValue)}&limit=50`;
  console.log("Constructed URL:", url);

  try {
    const response = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
        'X-Cafe24-Api-Version': CAFE24_API_VERSION,
      },
    });
    const products = response.data.products || [];
    console.log("API 응답 상품 개수:", products.length);

    const exactMatches = products.filter(product => product.product_name === dataValue);
    console.log("정확히 일치하는 상품 개수:", exactMatches.length);

    if (exactMatches.length === 1) {
      const product_no = exactMatches[0].product_no;
      const detail = await getProductDetail(product_no);
      return res.json(detail);
    } else {
      return res.json(exactMatches);
    }
  } catch (error) {
    // 401 에러인 경우, MongoDB에서 토큰 정보를 갱신
    if (error.response && error.response.status === 401) {
      try {
        // db 변수 대신 getTokensFromDB() 함수를 호출하여 최신 토큰을 불러옵니다.
        await getTokensFromDB();
        console.log("토큰 갱신 완료. New tokens:", accessToken, refreshToken);

        // 새로운 토큰을 사용하여 재요청
        const retryResponse = await axios.get(url, {
          headers: {
            Authorization: `Bearer ${accessToken}`,
            'Content-Type': 'application/json',
            'X-Cafe24-Api-Version': CAFE24_API_VERSION,
          },
        });
        const products = retryResponse.data.products || [];
        console.log("API 응답 상품 개수 (retry):", products.length);

        const exactMatches = products.filter(product => product.product_name === dataValue);
        console.log("정확히 일치하는 상품 개수 (retry):", exactMatches.length);

        if (exactMatches.length === 1) {
          const product_no = exactMatches[0].product_no;
          const detail = await getProductDetail(product_no);
          return res.json(detail);
        } else {
          return res.json(exactMatches);
        }
      } catch (mongoError) {
        console.error("MongoDB 토큰 가져오기 오류:", mongoError);
        return res.status(500).json({ error: "MongoDB 토큰 가져오기 오류" });
      }
    } else {
      console.error("Error fetching product:", error.response ? error.response.data : error.message);
      return res.status(500).json({ error: "Error fetching product" });
    }
  }
});

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


    // MongoDB 연결
MongoClient.connect(MONGODB_URI, { useUnifiedTopology: true })
.then(client => {
  db = client.db(DB_NAME);
  participationCollection = db.collection('eventRoll');
  console.log("Connected to MongoDB");

  // MongoDB 연결 후에 서버 시작 (포트 3000 또는 환경변수 PORT 사용)
  const PORT = process.env.PORT || 4000;
  app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
  });
})
.catch(err => {
  console.error("Failed to connect to MongoDB", err);
});




// 쿠폰 세그먼트 정보 (서버에서 관리)
// 브라우저 전용 객체인 segmentImages를 사용하지 않고, 이미지 경로 문자열을 사용합니다.
const segmentsData = [
  { label: '40%', probability: 0 },
  { label: '50%', probability: 0 },
  { label: '60%', probability: 0 },
  { label: '70%', probability: 0 },
  { label: '90%', probability: 0.0001 },
  { label: '80%', probability: 99 }
];

app.get('/api/segments', (req, res) => {
  res.json({ segments: segmentsData });
});

// 각 쿠폰 타입별로 미리 관리되는 쿠폰 번호 데이터 (예시)
const couponDB = {
"40% 쿠폰": [ "40%" ],
"50% 쿠폰": [ "50%" ],
"60% 쿠폰": [ "60%" ],
"70% 쿠폰": [ "70%" ],
"80% 쿠폰": [ "80%" ],
"90% 쿠폰": [ "90%" ]
};

// 쿠폰 번호 발급 API (요청 시 해당 쿠폰 타입의 쿠폰 번호를 할당)
app.get('/api/coupon', (req, res) => {
const couponType = req.query.couponType;
if (!couponType || !couponDB[couponType] || couponDB[couponType].length === 0) {
  return res.status(404).json({ error: "쿠폰이 없습니다." });
}
// DB에서 제거하지 않고 첫 번째 쿠폰 번호를 반환 (제한 없음)
const couponCode = couponDB[couponType][0];
res.json({ couponCode });
});

// 회원 참여 기록 API
app.post('/api/participate', async (req, res) => {
const { memberId } = req.body;
if (!memberId) {
  return res.status(400).json({ error: "회원 아이디가 필요합니다." });
}
try {
  // 이미 참여한 회원인지 체크
  const existing = await participationCollection.findOne({ memberId: memberId });
  if (existing) {
    return res.status(400).json({ error: "" });
  }
  // 참여 기록 저장 (제한 없음)
  await participationCollection.insertOne({ memberId: memberId, participatedAt: new Date() });
  res.json({ success: true });
} catch (err) {
  console.error(err);
  res.status(500).json({ error: "서버 오류" });
}
});



// 1) 이벤트 전용 클릭 저장 API
//    - route: /api/event/click
app.post('/api/event/click', async (req, res) => {
  const { sectionId } = req.body;
  if (!sectionId) {
    return res.status(400).json({ error: 'sectionId is required' });
  }

  const now  = new Date();
  const date = getFormattedDate(now);
  const client = new MongoClient(MONGODB_URI, { useUnifiedTopology: true });

  try {
    await client.connect();
    const db = client.db(DB_NAME);
    // 기존 clickData, clickDataSave 와 혼동 없도록 컬렉션 이름 분리
    const col = db.collection('eventClickData');

    await col.updateOne(
      { sectionId, date },
      { $inc: { count: 1 }, $push: { timestamps: now } },
      { upsert: true }
    );

    res.json({ message: 'Event click recorded', sectionId, date });
  } catch (err) {
    console.error('Error saving event click:', err);
    res.status(500).json({ error: 'DB Error' });
  } finally {
    await client.close();
  }
});


function getFormattedDate(date = new Date()) {
  const seoulString = date.toLocaleString('en-US', { timeZone: 'Asia/Seoul' });
  const seoulDate   = new Date(seoulString);
  return seoulDate.toISOString().slice(0, 10);
}

// 2) 이벤트 전용 날짜별 클릭 통계 조회 API
//    - route: /api/event/click/stats?date=YYYY-MM-DD
app.get('/api/event/click/stats', async (req, res) => {
  const date = req.query.date || getFormattedDate();
  const client = new MongoClient(MONGODB_URI, { useUnifiedTopology: true });

  try {
    await client.connect();
    const db = client.db(DB_NAME);
    const col = db.collection('eventClickData');

    const docs = await col.find({ date }).toArray();
    const result = docs.map(d => ({
      sectionId: d.sectionId,
      clicks:    d.count
    }));

    res.json({ date, result });
  } catch (err) {
    console.error('Error fetching event stats:', err);
    res.status(500).json({ error: 'DB Error' });
  } finally {
    await client.close();
  }
});




let db, eventPartnersCollection;

MongoClient.connect(MONGODB_URI, { useUnifiedTopology: true })
  .then(client => {
    console.log('✅ MongoDB 연결 성공');
    db = client.db(DB_NAME);

    // 이벤트 참여 기록용 컬렉션
    eventPartnersCollection = db.collection('eventPartners');

    // memberId+keyword 조합에 대한 unique 인덱스 생성 (최초 1회)
    eventPartnersCollection.createIndex(
      { memberId: 1, keyword: 1 },
      { unique: true }
    ).then(() => {
      console.log('✅ eventPartners unique index 생성 완료');
    }).catch(err => {
      console.error('❌ eventPartners 인덱스 생성 오류:', err);
    });

    // 서버 시작
    const PORT = process.env.PORT || 3000;
    app.listen(PORT, () => {
      console.log(`🚀 Server is running on port ${PORT}`);
    });
  })
  .catch(err => {
    console.error('❌ MongoDB 연결 실패:', err);
  });

// ── [포인트 적립용 엔드포인트 수정] ──
const KEYWORD_REWARDS = {
  '우파루파': 2000
};

app.post('/api/points', async (req, res) => {
  const { memberId, keyword } = req.body;

  // 1) 파라미터 유효성 검사
  if (!memberId || typeof memberId !== 'string') {
    return res
      .status(400)
      .json({ success: false, message: '잘못된 요청입니다. 다시 시도해주세요.' });
  }

  const amount = KEYWORD_REWARDS[keyword];
  if (!amount) {
    // 틀린 키워드
    return res
      .status(400)
      .json({ success: false, message: '아쉽지만 정답이 아닙니다. 다시 도전해보세요!' });
  }

  try {
    // 2) 중복 참여 확인
    const already = await eventPartnersCollection.findOne({ memberId, keyword });
    if (already) {
      return res
        .status(400)
        .json({ success: false, message: '이미 참여 완료하신 고객입니다.' });
    }

    // 3) Cafe24 API로 포인트 적립
    const payload = {
      shop_no: 1,
      request: {
        member_id: memberId,
        order_id:  null,
        amount,
        type:    'increase',
        reason:  `${keyword} 프로모션 적립금 지급`
      }
    };
    const data = await apiRequest(
      'POST',
      `https://${CAFE24_MALLID}.cafe24api.com/api/v2/admin/points`,
      payload
    );

    // 4) 적립 성공 시 참여 기록 저장
    await eventPartnersCollection.insertOne({
      memberId,
      keyword,
      participatedAt: new Date()
    });

    // 5) 성공 응답
    return res.json({ success: true, data });

  } catch (err) {
    console.error('포인트 지급 오류:', err);

    // unique index 충돌 (동시성 중복 처리)
    if (err.code === 11000) {
      return res
        .status(400)
        .json({ success: false, message: '이미 참여 완료한 이벤트입니다.' });
    }

    // 기타 서버 오류
    return res
      .status(500)
      .json({ success: false, message: '서버 오류가 발생했습니다. 잠시 후 다시 시도해주세요.' });
  }
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
// (3) 적립금 지급 함수 (자사몰용)
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
// (6) 매장용 참여 내역 엑셀 다운로드
app.get('/api/event/marketing-consent-export', async (req, res) => {
  const client = new MongoClient(MONGODB_URI);
  try {
    await client.connect();
    const coll = client.db(DB_NAME).collection('marketingConsentEvent');
    const docs = await coll.find({})
      .project({ _id: 0, participatedAt: 1, memberId: 1, store: 1 })
      .toArray();

    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet('매장 참여 내역');

    ws.columns = [
      { header: '참여 날짜', key: 'participatedAt', width: 25 },
      { header: '회원 아이디', key: 'memberId',      width: 20 },
      { header: '참여 매장',  key: 'store',          width: 20 },
    ];

    docs.forEach(d => {
      ws.addRow({
        participatedAt: d.participatedAt.toLocaleString('ko-KR'),
        memberId:       d.memberId,
        store:          d.store
      });
    });

    const storeFilename = '매장_참여_내역.xlsx';
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="store_export.xlsx"; filename*=UTF-8''${encodeURIComponent(storeFilename)}`
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




// ==============================
// 더현대 참여 이벤트 (consent 동시 업데이트 보장형)
// ==============================
app.post('/api/event/marketing-hyundai', async (req, res) => {
  const { memberId, store } = req.body;
  if (!memberId || typeof memberId !== 'string') {
    return res.status(400).json({ success: false, message: 'memberId가 필요합니다.' });
  }

  const client = new MongoClient(MONGODB_URI, { useUnifiedTopology: true });
  try {
    await client.connect();
    const coll = client.db(DB_NAME).collection('mktTheHyundai');

    // 0) 이미 참여했는지 간단 체크 (빠른 응답을 위해)
    // -> 레이스 컨디션 완벽 회피는 insertOne + unique index에 맡김
    if (await coll.findOne({ memberId })) {
      return res.status(409).json({ success: false, message: '이미 참여하셨습니다.' });
    }

    // 1) 개인정보 수집·이용(선택) 업데이트
    try {
      await updatePrivacyConsent(memberId);
      // updatePrivacyConsent는 'No API found'일 경우 내부에서 패스하도록 구현되어 있음
    } catch (err) {
      // privacy API에서 기술적 에러(예: 500 등)가 나면 중단
      console.error('updatePrivacyConsent 실패:', err);
      return res.status(500).json({ success: false, message: '개인정보 동의 업데이트 중 오류가 발생했습니다.' });
    }

    // 2) SMS 수신동의 업데이트 (재시도 로직 포함)
    const maxAttempts = 3;
    let updatedMarketing = false;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        await updateMarketingConsent(memberId);
        updatedMarketing = true;
        break;
      } catch (err) {
        console.warn(`updateMarketingConsent 실패 (attempt ${attempt}):`, err && err.message ? err.message : err);
        // 만약 응답에 "No API found" 같은 비지원 표시가 있으면 중단(대체 정책 필요)
        if (err.response?.data?.error?.message?.includes('No API found')) {
          console.warn('updateMarketingConsent 엔드포인트 미지원으로 간주 — 패스');
          updatedMarketing = false; // 혹은 true로 간주할지 비즈니스 결정 필요
          break;
        }
        // 재시도: 짧은 backoff
        if (attempt < maxAttempts) {
          await new Promise(r => setTimeout(r, 300 * attempt));
          continue;
        } else {
          // 마지막 시도 실패하면 오류 반환
          console.error('updateMarketingConsent 최종 실패:', err);
          return res.status(500).json({ success: false, message: 'SMS 수신동의 업데이트 중 오류가 발생했습니다.' });
        }
      }
    }

    // NOTE: 위에서 updatedMarketing이 false인 경우는 "No API found" 등으로 처리했을 때입니다.
    // 비즈니스 요구상 반드시 성공해야 한다면 위에서 failure시 500을 리턴하도록 바꿔야 합니다.

    // 3) 참여 기록 저장 (insertOne -> unique index가 있어 중복 방지)
    const seoulNow = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
    try {
      await coll.insertOne({ memberId: String(memberId), store: store || null, participatedAt: seoulNow });
    } catch (err) {
      // unique index 충돌
      if (err.code === 11000) {
        return res.status(409).json({ success: false, message: '이미 참여하셨습니다.' });
      }
      console.error('참여 기록 저장 오류:', err);
      return res.status(500).json({ success: false, message: '참여 처리 중 서버 오류가 발생했습니다.' });
    }

    // 4) (선택) 포인트 지급 등 추가 작업: 필요하면 여기서 giveRewardPoints 호출 (비동기 후처리 권장)
    // await giveRewardPoints(memberId, amount, '더현대 이벤트 보상');

    return res.json({ success: true, message: '참여 완료!' });
  } catch (err) {
    console.error('더현대 이벤트 처리 오류:', err);
    return res.status(500).json({ success: false, message: '서버 오류가 발생했습니다.' });
  } finally {
    await client.close();
  }
});


// ==============================
// 더현대 참여 내역 엑셀 다운로드
// ==============================
app.get('/api/event/marketing-consent-hyundai', async (req, res) => {
  const client = new MongoClient(MONGODB_URI, { useUnifiedTopology: true });
  try {
    await client.connect();
    const coll = client.db(DB_NAME).collection('mktTheHyundai');

    const docs = await coll.find({})
      .project({ _id: 0, participatedAt: 1, memberId: 1 }) // 필요한 필드만
      .sort({ participatedAt: -1 })
      .toArray();

    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet('더현대_참여내역');

    ws.columns = [
      { header: '참여 날짜', key: 'participatedAt', width: 25 },
      { header: '회원 아이디', key: 'memberId', width: 20 },
    ];

    docs.forEach(d => {
      ws.addRow({
        participatedAt: d.participatedAt ? new Date(d.participatedAt).toLocaleString('ko-KR') : '',
        memberId: d.memberId || ''
      });
    });

    const filename = '더현대_참여내역.xlsx';
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="hyundai_participants.xlsx"; filename*=UTF-8''${encodeURIComponent(filename)}`
    );
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');

    await wb.xlsx.write(res);
    res.end();
  } catch (err) {
    console.error('더현대 엑셀 생성 오류:', err);
    res.status(500).send('엑셀 생성 중 오류가 발생했습니다.');
  } finally {
    await client.close();
  }
});

// ==============================
// (9) 추석 적립금 지급 이벤트
// ==============================

// [추가] 참여 여부 확인 API
// GET /api/event/yogi-event-entry?memberId=회원ID
app.get('/api/event/yogi-event-entry', async (req, res) => {
  // 1. 쿼리에서 회원 ID를 받습니다.
  const { memberId } = req.query;
  if (!memberId) {
      return res.status(400).json({ success: false, message: '회원 ID가 필요합니다.' });
  }

  const client = new MongoClient(process.env.MONGODB_URI);
  try {
      await client.connect();
      const collection = client.db("yogibo").collection('yogiEventParticipants');

      // 2. DB에서 해당 회원 ID를 찾습니다.
      const participant = await collection.findOne({ memberId: String(memberId) });

      // 3. 참여 기록에 따라 다른 응답을 보냅니다.
      if (participant) {
          // 참여 기록이 있으면, 참여했다는 사실과 선택했던 옵션을 응답합니다.
          return res.status(200).json({
              hasParticipated: true,
              selectedOption: participant.selectedOption
          });
      } else {
          // 참여 기록이 없으면, 참여하지 않았다고 응답합니다.
          return res.status(200).json({
              hasParticipated: false
          });
      }
  } catch (error) {
      console.error('참여 여부 확인 중 오류:', error);
      return res.status(500).json({ success: false, message: '서버 처리 중 오류가 발생했습니다.' });
  } finally {
      await client.close();
  }
});


// (9) 나만의 맞춤 제안 이벤트 (참여자 정보 수집용)
// POST /api/event/yogi-event-entry
app.post('/api/event/yogi-event-entry', async (req, res) => {
  const { memberId, selectedOption } = req.body;
  if (!memberId || !selectedOption) {
      return res.status(400).json({ success: false, message: '필수 정보가 누락되었습니다.' });
  }

  const client = new MongoClient(process.env.MONGODB_URI);
  try {
      await client.connect();
      const collection = client.db("yogibo").collection('yogiEventParticipants');

      const existingParticipant = await collection.findOne({ memberId: memberId });
      
      // ▼▼▼ [수정] 중복 참여자일 경우, 기존 선택 옵션을 함께 반환합니다. ▼▼▼
      if (existingParticipant) {
          return res.status(409).json({
              success: false,
              message: '이미 참여한 이벤트입니다.',
              selectedOption: existingParticipant.selectedOption // 기존 선택 옵션 추가!
          });
      }
      // ▲▲▲ [수정] ▲▲▲

      const participationRecord = {
          memberId: String(memberId),
          selectedOption: selectedOption,
          participatedAt: new Date()
      };
      await collection.insertOne(participationRecord);

      return res.status(200).json({ success: true, message: '이벤트 참여가 완료되었습니다.' });

  } catch (error) {
      if (error.code === 11000) {
          // ▼▼▼ [수정] DB 에러 발생 시에도 기존 정보를 조회해서 반환 시도 ▼▼▼
          const existingParticipant = await client.db("yogibo").collection('yogiEventParticipants').findOne({ memberId: memberId });
          return res.status(409).json({
              success: false,
              message: '이미 참여한 이벤트입니다.',
              selectedOption: existingParticipant ? existingParticipant.selectedOption : null
          });
          // ▲▲▲ [수정] ▲▲▲
      }
      console.error('이벤트 참여 처리 중 오류:', error);
      return res.status(500).json({ success: false, message: error.message || '서버 처리 중 오류가 발생했습니다.' });
  } finally {
      await client.close();
  }
});


// 참여자 리스트 다운로드 코드 (수정 없음)
app.get('/api/event/yogi-event-export', async (req, res) => {
  const client = new MongoClient(process.env.MONGODB_URI);
  try {
      await client.connect();
      const collection = client.db("yogibo").collection('yogiEventParticipants');

      const participants = await collection.find({}).sort({ participatedAt: -1 }).toArray();

      const workbook = new ExcelJS.Workbook();
      const worksheet = workbook.addWorksheet('이벤트 참여자 명단');

      worksheet.columns = [
          { header: '참여 날짜', key: 'participatedAt', width: 25 },
          { header: '회원 아이디', key: 'memberId', width: 30 },
          { header: '선택 옵션', key: 'selectedOption', width: 15 } // 엑셀에 선택 옵션도 추가하면 좋을 것 같아 추가했습니다.
      ];

      participants.forEach(p => {
          worksheet.addRow({
              participatedAt: new Date(p.participatedAt).toLocaleString('ko-KR'),
              memberId: p.memberId,
              selectedOption: p.selectedOption
          });
      });

      const filename = '요기보_이벤트_참여내역.xlsx';
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(filename)}`);

      await workbook.xlsx.write(res);
      res.end();

  } catch (error) {
      console.error('엑셀 다운로드 생성 중 오류:', error);
      res.status(500).send('엑셀 파일 생성 중 오류가 발생했습니다.');
  } finally {
      await client.close();
  }
});


//쿠폰 데이터 저장

let userCouponsCollection;

async function initCouponDb() {
  if (!userCouponsCollection) {
    mongoClient = new MongoClient(MONGODB_URI, { useUnifiedTopology: true });
    await mongoClient.connect();
    db = mongoClient.db(DB_NAME);
    userCouponsCollection = db.collection('userCoupons');
    // memberId + couponId 조합 유니크 인덱스
    await userCouponsCollection.createIndex(
      { memberId: 1, couponId: 1 },
      { unique: true }
    );
    console.log('✅ userCoupons collection ready with unique index');
  }
}

// 서울 기준 날짜 문자열 "YYYY-MM-DD" 반환
function getSeoulYMD(date = new Date()) {
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Seoul',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  return fmt.format(date); // 예: "2025-08-01"
}

// 오늘 기준 오픈 가능한 쿠폰 ID 계산 (1부터 시작, 최대 15)
function computeAvailableCouponId() {
  const todayStr = getSeoulYMD(); // ex "2025-08-01"
  const eventStartStr = '2025-08-01'; // 이벤트 시작일 (KST 기준)
  const [yT, mT, dT] = todayStr.split('-').map(Number);
  const [yS, mS, dS] = eventStartStr.split('-').map(Number);
  // UTC로 맞춰서 날짜 객체 생성 (날짜 차이는 동일하게 계산됨)
  const todayDate = new Date(Date.UTC(yT, mT - 1, dT));
  const startDate = new Date(Date.UTC(yS, mS - 1, dS));
  const diffMs = todayDate - startDate;
  const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
  const couponId = diffDays + 1;
  if (couponId < 1) return 0;
  if (couponId > 15) return 15;
  return couponId;
}

// ====== 조회: 이미 클레임한 쿠폰 목록 ======
app.get('/api/coupon/claimed', async (req, res) => {
  const memberId = req.query.memberId;
  if (!memberId) {
    return res.status(400).json({ error: 'memberId 쿼리 파라미터가 필요합니다.' });
  }
  try {
    await initCouponDb();
    const docs = await userCouponsCollection
      .find({ memberId: String(memberId) })
      .project({ _id: 0, couponId: 1 })
      .toArray();
    const claimed = docs.map(d => String(d.couponId));
    return res.json({ claimed });
  } catch (err) {
    console.error('[쿠폰 이벤트] claimed 조회 오류:', err);
    return res.status(500).json({ error: 'DB 조회 중 오류가 발생했습니다.' });
  }
});

// ====== 클레임 엔드포인트 ======
app.post('/api/coupon/claim', async (req, res) => {
  const { memberId, couponId } = req.body;
  if (!memberId || couponId === undefined || couponId === null) {
    return res.status(400).json({ error: 'memberId와 couponId가 필요합니다.' });
  }

  // couponId 유효성 검사 (1~15)
  const cid = parseInt(couponId, 10);
  if (isNaN(cid) || cid < 1 || cid > 15) {
    return res.status(400).json({ error: '유효하지 않은 couponId입니다.' });
  }

  // 오픈 여부 체크 (서버 기준: KST 날짜 기준)
  const availableCouponId = computeAvailableCouponId();
  if (cid > availableCouponId) {
    return res.status(400).json({
      error: `쿠폰 ${cid}은 아직 오픈되지 않았습니다. (현재 오픈가능 쿠폰: ${availableCouponId})`,
    });
  }

  try {
    await initCouponDb();
    const result = await userCouponsCollection.updateOne(
      { memberId: String(memberId), couponId: String(cid) },
      {
        $setOnInsert: {
          memberId: String(memberId),
          couponId: String(cid),
          claimedAt: new Date(),
        },
      },
      { upsert: true }
    );

    if (result.upsertedId) {
      // 새로 클레임된 경우
      return res.json({ success: true, message: '쿠폰 클레임 완료되었습니다.' });
    } else {
      // 이미 존재해서 upsert 없이 끝난 경우
      return res.status(409).json({ error: '이미 받은 쿠폰입니다.' });
    }
  } catch (err) {
    if (err.code === 11000) {
      return res.status(409).json({ error: '이미 받은 쿠폰입니다.' });
    }
    console.error('[쿠폰 이벤트] 클레임 처리 오류:', err);
    return res.status(500).json({ error: '서버 내부 오류' });
  }
});



// ========== [추가] 오프라인 매출 관련 설정 ==========
const offlineSalesCollectionName = "dailyOfflineSales"; // MongoDB 컬렉션 이름

// 서버 메모리에 오늘 오프라인 매출 상태 저장 (DB 부하 감소 목적)
let todayOfflineState = {
    date: null,
    target: 0,
    accumulated: 0,
    startTime: null,
    endTime: null,
    isComplete: true
};
/**
 * [백엔드 로직] 15초마다 실행되어 accumulatedOfflineSales를 점진적으로 업데이트
 */
async function updateAccumulatedOfflineSales() {
  // 업데이트할 필요 없으면 종료 (완료되었거나, 시작/종료 시간 없음)
  if (todayOfflineState.isComplete || !todayOfflineState.startTime || !todayOfflineState.endTime) {
      return; 
  }

  const now = new Date(); // 현재 시간

  // 목표 시간 도달 시 최종 처리
  if (now >= todayOfflineState.endTime) {
      // 아직 최종 값으로 업데이트되지 않았다면 업데이트
      if (todayOfflineState.accumulated !== todayOfflineState.target) {
          todayOfflineState.accumulated = todayOfflineState.target;
          console.log(`[오프라인 매출] ${todayOfflineState.date} 목표 시간 도달. 최종 값 업데이트: ${todayOfflineState.target.toLocaleString()}원`);
      }
      // 아직 완료 처리되지 않았다면 완료 처리
      if (!todayOfflineState.isComplete) {
           todayOfflineState.isComplete = true;
           console.log(`[오프라인 매출] ${todayOfflineState.date} 점진적 업데이트 완료.`);
      }

      // DB에도 최종 상태 저장 (완료 시 한 번만)
      try {
          const client = new MongoClient(MONGODB_URI);
          await client.connect();
          const db = client.db(DB_NAME);
          const collection = db.collection(offlineSalesCollectionName);
          // isComplete 플래그와 최종 누적 금액, 업데이트 시간 저장
          await collection.updateOne(
              { date: todayOfflineState.date },
              { $set: { accumulatedOfflineSales: todayOfflineState.target, isComplete: true, updatedAt: new Date() } }
          );
          await client.close();
      } catch(err) { 
          console.error("DB 최종 오프라인 매출 저장 오류:", err); 
      }

  } 
  // 시작 시간 이후 & 목표 시간 이전일 때 진행
  else if (now >= todayOfflineState.startTime) { 
      // 경과 시간에 비례하여 현재 누적되어야 할 금액 계산
      const totalDuration = todayOfflineState.endTime.getTime() - todayOfflineState.startTime.getTime();
      const elapsedDuration = now.getTime() - todayOfflineState.startTime.getTime();
      // 진행률 계산 (0 ~ 1)
      const progress = Math.min(elapsedDuration / totalDuration, 1); 
      // 현재 시점의 누적 금액 계산
      const newAccumulated = Math.floor(progress * todayOfflineState.target); 

      // 계산된 누적 금액이 현재 메모리의 누적 금액보다 클 때만 업데이트 (감소 방지)
      if (newAccumulated > todayOfflineState.accumulated) {
          todayOfflineState.accumulated = newAccumulated;
          // 15초마다 로그 찍는 것은 부하를 유발할 수 있어 주석 처리
          // console.log(`[오프라인 매출] ${todayOfflineState.date} 진행 중: ${newAccumulated.toLocaleString()}원 / ${todayOfflineState.target.toLocaleString()}원`);

          // 💡 FIX: 중간 상태 DB 업데이트 로직 제거 완료
      }
  }
}

/**
* [백엔드 로직] 서버 시작 시 오늘 날짜의 오프라인 매출 상태 로드
*/
async function loadTodayOfflineState() {
  // 한국 시간 기준 오늘 날짜 (YYYY-MM-DD)
  const todayYMD = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' })).toISOString().slice(0, 10);

  try {
      const client = new MongoClient(MONGODB_URI);
      await client.connect();
      const db = client.db(DB_NAME);
      const collection = db.collection(offlineSalesCollectionName);
      // 오늘 날짜로 저장된 문서 조회
      const doc = await collection.findOne({ date: todayYMD });
      await client.close();

      // 문서가 있으면 해당 데이터로 메모리 상태 업데이트
      if (doc) {
          todayOfflineState = {
              date: doc.date,
              target: doc.targetOfflineSales || 0,
              accumulated: doc.accumulatedOfflineSales || 0,
              startTime: doc.startTime ? new Date(doc.startTime) : null,
              endTime: doc.endTime ? new Date(doc.endTime) : null,
              isComplete: doc.isComplete === true
          };
          // 로그에는 필요한 정보만 간략하게 출력
          console.log(`[오프라인 매출] ${todayYMD} 데이터 로드 완료:`, {
              target: todayOfflineState.target,
              accumulated: todayOfflineState.accumulated,
              isComplete: todayOfflineState.isComplete
           });
      } else {
           // 문서가 없으면 오늘 데이터 없음을 알리고 초기 상태로 설정
           console.log(`[오프라인 매출] ${todayYMD} 데이터 없음.`);
           todayOfflineState = { date: todayYMD, target: 0, accumulated: 0, startTime: null, endTime: null, isComplete: true };
      }
  } catch (err) {
      // DB 조회 중 오류 발생 시 에러 로그 출력 및 초기 상태로 설정
      console.error("오늘 오프라인 매출 상태 로드 오류:", err);
      todayOfflineState = { date: todayYMD, target: 0, accumulated: 0, startTime: null, endTime: null, isComplete: true };
  }
}

// 서버 시작 시 오늘 상태 로드
loadTodayOfflineState();
// 15초마다 오프라인 매출 점진적 업데이트 함수 실행
setInterval(updateAccumulatedOfflineSales, 15 * 1000);


// ========== [신규] 오프라인 매출 입력을 위한 API 엔드포인트 ==========
app.post("/api/offline-sales", async (req, res) => {
  // 요청 본문에서 날짜(date)와 금액(amount) 추출
  const { date, amount } = req.body; 

  // 날짜 형식(YYYY-MM-DD) 및 금액 유효성 검사
  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date) || typeof amount !== 'number' || amount < 0) {
      return res.status(400).json({ error: "날짜(YYYY-MM-DD)와 0 이상의 매출 금액을 입력해주세요." });
  }

  // 오늘 날짜 확인 (한국 시간 기준)
  const todayYMD = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' })).toISOString().slice(0, 10);
  // 입력된 날짜가 오늘이 아니면 경고 로그 출력 (처리는 계속)
  if (date !== todayYMD) {
       console.warn(`[오프라인 매출] 오늘(${todayYMD})이 아닌 날짜(${date}) 데이터가 입력되었습니다. 처리는 계속합니다.`);
  }

  try {
      const startTime = new Date(); // KST 기준 현재 시간 (점진적 반영 시작 시간)
      
      // 종료 시간: 다음 날 오전 9시 (KST 기준)
      const endTime = new Date(startTime);
      endTime.setDate(startTime.getDate() + 1); // 날짜를 다음 날로 변경
      const endTimeYMD = endTime.toISOString().slice(0, 10); // 다음 날 날짜 (YYYY-MM-DD)
      // KST 오전 9시로 정확히 설정 (+09:00 오프셋 명시)
      const endTimeKST = new Date(`${endTimeYMD}T09:00:00+09:00`); 

      // DB에 저장할 데이터 객체 생성
      const salesData = {
          date: date,                        // 입력받은 날짜
          targetOfflineSales: amount,        // 입력받은 목표 금액
          accumulatedOfflineSales: 0,        // 누적 금액은 0으로 초기화
          startTime: startTime,              // 반영 시작 시간 (현재)
          endTime: endTimeKST,               // 반영 종료 시간 (다음날 9시 KST)
          isComplete: false,                 // 점진적 반영 시작 플래그
          updatedAt: new Date()              // 문서 업데이트 시간 기록
      };

      // MongoDB 연결 및 데이터 업데이트/삽입 (upsert)
      const client = new MongoClient(MONGODB_URI);
      await client.connect();
      const db = client.db(DB_NAME);
      const collection = db.collection(offlineSalesCollectionName);
      
      await collection.updateOne(
          { date: date }, // 해당 날짜를 기준으로
          { $set: salesData }, // salesData 객체 내용으로 덮어쓰기
          { upsert: true } // 문서가 없으면 새로 생성
      );
      await client.close();

      // 서버 메모리 상태(todayOfflineState)도 즉시 업데이트
      todayOfflineState = {
          date: salesData.date,
          target: salesData.targetOfflineSales,
          accumulated: salesData.accumulatedOfflineSales,
          startTime: salesData.startTime,
          endTime: salesData.endTime,
          isComplete: salesData.isComplete
      };

      // 성공 로그 출력 및 응답 전송
      console.log(`[오프라인 매출] ${date} 목표 ${amount.toLocaleString()}원 설정 완료. 반영 시작 시간: ${startTime.toLocaleString('ko-KR')}, 종료 시간: ${endTimeKST.toLocaleString('ko-KR')}`);
      res.json({ message: `${date} 오프라인 매출 ${amount.toLocaleString()}원 목표 설정 완료. 점진적 반영 시작.` });

  } catch (error) {
      // 오류 발생 시 에러 로그 출력 및 500 응답 전송
      console.error("오프라인 매출 저장 오류:", error);
      res.status(500).json({ error: "오프라인 매출 처리 중 서버 오류 발생" });
  }
});


// ========== [수정] 총 매출액 조회를 위한 API 엔드포인트 (오프라인 합산) ==========
app.get("/api/total-sales", async (req, res) => {
  // 프론트엔드에서 전달받을 수 있는 날짜 쿼리 파라미터
  const providedDates = {
      start_dateText: req.query.start_dateText,
      end_dateText: req.query.end_dateText
  };

  try {
      // API 요청 전 최신 Access Token 로드
      await getTokensFromDB(); 
      
      // 1. 온라인 매출 조회 (지정된 기간 또는 기본 기간)
      const onlineSales = await getTotalSales(providedDates);
      
      // 2. 현재 시점까지 반영된 오프라인 매출 가져오기 (메모리 값 사용)
      const currentOfflinePortion = todayOfflineState.accumulated;
      
      // 3. 온라인 매출 + 오프라인 매출 합산
      const combinedTotalSales = onlineSales + currentOfflinePortion;

      // 응답에 포함될 최종 조회 기간 확인
      // providedDates에 값이 있으면 사용, 없으면 getTotalSales 내부 로직과 동일하게 기본값 설정
      const finalStartDate = providedDates.start_dateText || '2025-01-01'; 
      const finalEndDate = providedDates.end_dateText || '2025-12-31';

      // 합산 결과 로그 출력
      console.log(`[API 응답] 온라인(${onlineSales.toLocaleString()}) + 오프라인(${currentOfflinePortion.toLocaleString()}) = ${combinedTotalSales.toLocaleString()}`);

      // 프론트엔드에 최종 합산 결과 응답
      res.json({
          startDate: finalStartDate,
          endDate: finalEndDate,
          totalSales: combinedTotalSales // 합산된 금액 반환
      });
  } catch (error) {
      // 오류 발생 시 500 응답
      res.status(500).json({ error: "총 매출액을 가져오는 중 오류가 발생했습니다." });
  }
});

// ==========================================================
// [API 1] 로그 수집 (수정됨: 정확한 재방문/세션 로직 적용)
// ==========================================================
app.post('/api/trace/log', async (req, res) => {
  try {
      // 1. IP 확인 및 차단 필터
      let userIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '';
      if (userIp.includes(',')) userIp = userIp.split(',')[0].trim();

      // 1. 차단할 공용 IP 리스트
      const BLOCKED_IPS = ['127.0.0.1', '61.99.75.10']; 
      
      // 2. 프론트에서 보낸 '나 개발자야(isDev)' 신호 받기
      const { isDev } = req.body; 

      // ★ [핵심 수정] IP가 차단 목록에 있어도, isDev가 true면 통과시킴
      if (BLOCKED_IPS.includes(userIp) && !isDev) {
          return res.json({ success: true, msg: 'IP Filtered' });
      }

      let { eventTag, visitorId, currentUrl, prevUrl, utmData, deviceType } = req.body;

      // [디버깅] 요청 로깅
      console.log('[LOG] 요청:', { 
          visitorId, 
          currentUrl: currentUrl?.substring(0, 50), 
          userIp 
      });

      const isRealMember = visitorId && !/guest_/i.test(visitorId) && visitorId !== 'null';

      // ==========================================================
      // [1] 회원 로그인 시: 5분 이내 게스트 병합
      // ==========================================================
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

      // ==========================================================
      // [2] 게스트일 경우: 30분 이내 같은 IP의 기존 게스트 ID 사용
      // ==========================================================
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
              // console.log(`[GUEST] 기존 ID 재사용: ${visitorId}`);
          }
      }

      // ==========================================================
      // [3] ★ 중복 / 세션 / 재방문(Retention) 체크 (로직 개선됨)
      // ==========================================================
      let isNewSession = true;
      let skipReason = null;
      let isRevisit = false; // 기본값

      if (visitorId) {
          // 해당 유저의 '가장 최근 로그' 하나만 가져옴
          const lastLog = await db.collection('visit_logs1Event').findOne(
              { visitorId: visitorId },
              { sort: { createdAt: -1 } }
          );

          if (lastLog) {
              const timeDiff = Date.now() - new Date(lastLog.createdAt).getTime();
              const SESSION_TIMEOUT = 30 * 60 * 1000; // 30분 (세션 기준)

              // 3-1. 중복 클릭 방지 (2분 이내 + 같은 URL)
              if (timeDiff < 2 * 60 * 1000 && lastLog.currentUrl === currentUrl) {
                  skipReason = 'Duplicate (same URL within 2min)';
              }

              // 3-2. 세션 판별 및 재방문 로직
              if (timeDiff < SESSION_TIMEOUT) {
                  // [CASE A] 30분 이내 방문 (세션 유지 중)
                  // -> 페이지 이동이나 새로고침입니다.
                  // -> 절대 재방문 여부를 새로 계산하지 말고, 직전 상태를 그대로 물려받습니다.
                  isNewSession = false;
                  isRevisit = lastLog.isRevisit || false; 
              } else {
                  // [CASE B] 30분 지남 (새로운 세션 시작)
                  // -> 이때만 "과거(24시간 전)에 온 적 있나?"를 체크합니다.
                  isNewSession = true;
                  
                  // "지금으로부터 24시간보다 더 이전에" 작성된 로그가 하나라도 있는지 체크
                  const pastLog = await db.collection('visit_logs1Event').findOne({
                      visitorId: visitorId,
                      createdAt: { $lt: new Date(Date.now() - 24 * 60 * 60 * 1000) } 
                  });

                  if (pastLog) {
                      isRevisit = true; // 24시간 전 기록이 있으므로 재방문 유저!
                      console.log(`[REVISIT] 24시간 경과 후 재방문 확인: ${visitorId}`);
                  } else {
                      isRevisit = false; // 24시간 전 기록 없음 (신규 혹은 하루 내 재접속)
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

      // ==========================================================
      // [4] 진입점 체크
      // ==========================================================
      const hasPromoVisit = await db.collection('visit_logs1Event').findOne({
          $or: [ { visitorId: visitorId }, { userIp: userIp } ],
          currentUrl: { $regex: '1_promotion.html' }
      });

      if (isNewSession && !hasPromoVisit) {
          if (currentUrl && !currentUrl.includes('1_promotion.html')) {
              // console.log(`[BLOCK] 진입점 아님: ${currentUrl}`);
              return res.json({ success: true, msg: 'Not entry point' });
          }
      }

      if (currentUrl && currentUrl.includes('skin-skin')) {
          return res.json({ success: true, msg: 'Skin Ignored' });
      }

      // ==========================================================
      // [5] 로그 저장
      // ==========================================================
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
          isRevisit: isRevisit, // 결정된 재방문 값 저장
          createdAt: new Date()
      };

      const result = await db.collection('visit_logs1Event').insertOne(log);
      
      const prefix = isRevisit ? '[SAVE-Revisit]' : '[SAVE-New]';
      console.log(`${prefix} ${visitorId} (Session: ${isNewSession ? 'New' : 'Cont'})`);
      
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
// [API 11] 특정 채널로 유입된 방문자 목록 조회 (수정: 대용량 처리 옵션 추가)
// ==========================================================
app.get('/api/trace/visitors/by-channel', async (req, res) => {
  try {
      const { channelName, startDate, endDate } = req.query;
      
      let dateFilter = {};
      if (startDate || endDate) {
          dateFilter = {};
          if (startDate) dateFilter.$gte = new Date(startDate + "T00:00:00.000Z");
          if (endDate) dateFilter.$lte = new Date(endDate + "T23:59:59.999Z");
      }

      const pipeline = [
          { $match: { createdAt: dateFilter } },
          {
              $project: {
                  visitorId: 1,
                  userIp: 1,
                  createdAt: 1,
                  isMember: 1,
                  // ★ API 5번과 동일한 매핑 로직
                  computedChannel: {
                      $switch: {
                          branches: [
                              // 1. 네이버
                              { case: { $eq: ["$utmData.campaign", "home_main"] },  then: "브검 : 홈페이지 메인" },
                              { case: { $eq: ["$utmData.campaign", "naver_main"] }, then: "브검 : 1월 말할 수 없는 편안함(메인)" },
                              { case: { $eq: ["$utmData.campaign", "naver_sub1"] }, then: "브검 : 1월 말할 수 없는 편안함(서브1)_10%" },
                              { case: { $eq: ["$utmData.campaign", "naver_sub2"] }, then: "브검 : 1월 말할 수 없는 편안함(서브2)_20%" },
                              { case: { $eq: ["$utmData.campaign", "naver_sub3"] }, then: "브검 : 1월 말할 수 없는 편안함(서브3)_갓생" },
                              { case: { $eq: ["$utmData.campaign", "naver_sub4"] }, then: "브검 : 1월 말할 수 없는 편안함(서브4)_무료배송" },
                              { case: { $eq: ["$utmData.campaign", "naver_sub5"] }, then: "브검 : 1월 말할 수 없는 편안함(서브5)_가까운매장" },
                              // 2. 메타
                              { case: { $eq: ["$utmData.content", "employee_discount"] }, then: "메타 : 1월 말할 수 없는 편안함(직원 할인 찬스)" },
                              { case: { $eq: ["$utmData.content", "areading_group1"] },   then: "메타 : 1월 말할 수 없는 편안함(sky독서소파)" },
                              { case: { $eq: ["$utmData.content", "areading_group2"] },   then: "메타 : 1월 말할 수 없는 편안함(sky독서소파2)" },
                              { case: { $eq: ["$utmData.content", "special_price1"] },    then: "메타 : 1월 말할 수 없는 편안함(신년특가1)" },
                              { case: { $eq: ["$utmData.content", "special_price2"] },    then: "메타 : 1월 말할 수 없는 편안함(신년특가2)" },
                              { case: { $eq: ["$utmData.content", "horse"] },             then: "메타 : 1월 말할 수 없는 편안함(말 ai아님)" },
                              // 3. 카카오
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
          // 2. 요청받은 채널명과 일치하는 것만 필터링
          { $match: { computedChannel: channelName } },
          // 3. 최신순 정렬
          { $sort: { createdAt: -1 } },
          // 4. 그룹핑 (중복 제거)
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
                  lastAction: { $first: "$createdAt" },
                  userIp: { $first: "$userIp" },
                  count: { $sum: 1 }
              }
          },
          // 5. 프론트엔드용 필드 정리
          {
              $project: {
                  _id: 0,
                  searchId: "$_id",
                  visitorId: 1,
                  isMember: 1,
                  lastAction: 1,
                  userIp: 1,
                  count: 1
              }
          },
          { $sort: { lastAction: -1 } },
          { $limit: 100 }
      ];

      // ★ [중요] allowDiskUse: true 옵션 추가 (데이터가 많을 때 메모리 초과 방지)
      const visitors = await db.collection('visit_logs1Event').aggregate(pipeline, { allowDiskUse: true }).toArray();
      
      res.json({ success: true, visitors });

  } catch (err) {
      console.error('API 11 Error:', err);
      res.status(500).json({ msg: 'Server Error', error: err.toString() });
  }
});


// ==========================================
//2026년 1월 응모이벤트
// [API 1] 응모하기 (옵션 검증 로직 추가)
// URL: POST /api/raffle/entryEvents
// ==========================================

const EVENT_COLLECTION_NAME = 'event_2026_01Promotion';
const EVENT_PERIOD_START = '2026-01-01'; 
const EVENT_PERIOD_END = '2026-01-31'; 
const VALID_OPTIONS = ['유아독서대', '무드등', '홈 오피스'];


app.post('/api/raffle/entryEvents', async (req, res) => {
  try {
      const { userId, optionName } = req.body;
      
      // 1. 필수값 체크
      if (!userId || userId === 'GUEST' || userId === 'null') {
          return res.status(401).json({ success: false, message: '회원 로그인 후 참여 가능합니다.' });
      }
      if (!optionName) {
          return res.status(400).json({ success: false, message: '옵션(경품)을 선택해주세요.' });
      }

      // 2. 유효한 옵션인지 검증
      if (!VALID_OPTIONS.includes(optionName)) {
          return res.status(400).json({ success: false, message: '존재하지 않는 경품 옵션입니다.' });
      }

      // 3. 기간 체크
      const now = moment().tz('Asia/Seoul');
      const todayStr = now.format('YYYY-MM-DD');

      if (todayStr < EVENT_PERIOD_START || todayStr > EVENT_PERIOD_END) {
           return res.status(403).json({ success: false, message: '이벤트 진행 기간이 아닙니다.' });
      }

      const collection = db.collection(EVENT_COLLECTION_NAME);

      // 4. 중복 참여 체크
      const existingEntry = await collection.findOne({ userId: userId });

      if (existingEntry) {
          return res.json({ // 200 OK로 보내되 success: false 처리
              success: false, 
              code: 'ALREADY_ENTERED', 
              message: `이미 [${existingEntry.optionName}] 경품에 응모하셨습니다.` 
          });
      }

      // 5. DB 저장
      const newEntry = {
          userId: userId,
          optionName: optionName,
          entryDate: todayStr,
          createdAt: new Date(),
      };

      const result = await collection.insertOne(newEntry);

      res.json({
          success: true,
          message: `[${optionName}] 응모가 완료되었습니다!`,
          entryId: result.insertedId,
      });

  } catch (error) {
      console.error('이벤트 응모 오류:', error);
      res.status(500).json({ success: false, message: '서버 오류 발생' });
  }
});


// ==========================================
// [API 2] 응모 현황 조회 (중복 체크용)
// URL: GET /api/raffle/status
// ==========================================
app.get('/api/raffle/status', async (req, res) => {
  try {
      const { userId } = req.query;

      if (!userId || userId === 'GUEST' || userId === 'null') {
          return res.json({ success: true, isEntered: false, message: '로그인 필요' });
      }

      const collection = db.collection(EVENT_COLLECTION_NAME);
      const existingEntry = await collection.findOne({ userId: userId });
      
      if (existingEntry) {
          return res.json({ 
              success: true, 
              isEntered: true, 
              optionName: existingEntry.optionName,
              message: `이미 참여하셨습니다.`
          });
      } else {
           return res.json({ success: true, isEntered: false });
      }

  } catch (error) {
      console.error('상태 조회 오류:', error);
      res.status(500).json({ success: false, message: '서버 오류' });
  }
});


// ==========================================
// [API 3] 엑셀 다운로드
// URL: GET /api/raffle/excel (URL 변경 제안)
// ==========================================
app.get('/api/raffle/excel', async (req, res) => {
try {
    const collection = db.collection(EVENT_COLLECTION_NAME);
    const entries = await collection.find({}).sort({ createdAt: -1 }).toArray();

    if (!entries.length) {
        return res.status(404).json({ success: false, message: '데이터가 없습니다.' });
    }

    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('2026_Event_Entries');

    worksheet.columns = [
        { header: 'No', key: 'index', width: 8 },
        { header: '회원 ID', key: 'userId', width: 20 },
        { header: '응모 날짜', key: 'entryDate', width: 20 },
        { header: '선택 옵션', key: 'optionName', width: 30 },
        { header: '등록 시간', key: 'createdAt', width: 25 },
    ];

    entries.forEach((entry, index) => {
        worksheet.addRow({
            index: index + 1,
            userId: entry.userId || 'N/A',
            entryDate: entry.entryDate || 'N/A',
            optionName: entry.optionName || 'N/A',
            createdAt: entry.createdAt ? moment(entry.createdAt).tz('Asia/Seoul').format('YYYY-MM-DD HH:mm:ss') : 'N/A',
        });
    });

    const filename = `Event_2026_${moment().tz('Asia/Seoul').format('YYYYMMDD_HHmmss')}.xlsx`;
    
    // 버퍼로 바로 전송 (파일 생성 X)
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename=${filename}`);

    await workbook.xlsx.write(res);
    res.end();

} catch (err) {
    console.error('엑셀 오류:', err);
    res.status(500).json({ success: false, message: '엑셀 생성 실패' });
}
});


app.get('/api/config/kakao', (req, res) => {
  res.json({ 
      success: true, 
      key: process.env.KAKAO_JS_KEY // .env에서 가져온 키
  });
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