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
          return res.status(409).json({ message: '이미 참여하셨습니다.' });
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
  const PORT = process.env.PORT || 3000;
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
    return res.status(400).json({ error: "이미 참여하셨습니다." });
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
  '요기보다': 1
};

app.post('/api/points', async (req, res) => {
  const { memberId, keyword } = req.body;

  // 1) 파라미터 유효성 검사
  if (!memberId || typeof memberId !== 'string') {
    return res.status(400).json({ success: false, error: 'memberId는 문자열입니다.' });
  }
  const amount = KEYWORD_REWARDS[keyword];
  if (!amount) {
    return res.status(400).json({ success: false, error: '유효하지 않은 키워드입니다.' });
  }

  try {
    // 2) 중복 참여 확인 (MongoDB 조회)
    const already = await eventPartnersCollection.findOne({ memberId, keyword });
    if (already) {
      return res
        .status(400)
        .json({ success: false, error: '이미 참여 완료한 이벤트입니다.' });
    }

    // 3) Cafe24 API로 포인트 적립
    const payload = {
      shop_no: 1,
      request: {
        member_id: memberId,
        order_id:  '',
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

    // 5) 응답
    return res.json({ success: true, data });

  } catch (err) {
    console.error('포인트 지급 오류:', err);

    // 동시성 등으로 인해 unique index 위반시에도 중복 처리
    if (err.code === 11000) {
      return res
        .status(400)
        .json({ success: false, error: '이미 참여 완료한 이벤트입니다.' });
    }

    const status = err.response?.status || 500;
    return res
      .status(status)
      .json({ success: false, error: err.response?.data || err.message });
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
