/**
 * onOrder 서버에서 옮겨 온 API (2026-10 클라우드타입 서버 통합)
 *
 * · 원본: YogiboKorea/onOrder index.js — 온라인 판매현황 조회(/api/online/*), 팝업스토어 현장판매(/api/popup/*)
 * · 데이터는 그대로 on DB(orders · events · system_metadata · popup_sales · popup_inventory)에 있다.
 *   adminChat과 같은 클러스터라 getDb().client로 on DB를 연다.
 * · 팝업 화면(public/popup.html, popup_dashboard.html)도 같이 옮겨 와서 상대 경로로 이 API를 부른다.
 * · 옮기지 않은 것: 서버가 켜질 때마다 file/onOrderData.xlsx(3월분)를 on.orders에 다시 넣던 일.
 *   적재는 PC의 Desktop/온라인/excel 스크립트가 한다.
 */
'use strict';

const { ObjectId } = require('mongodb');

const ONLINE_DB_NAME = 'on';

function mount(app, deps) {
  const getDb = deps.getDb;
  const dbOnline = () => getDb().client.db(ONLINE_DB_NAME);

  // ── 온라인 대시보드 데이터 ─────────────────────────────

  // 월별 목록
  app.get('/api/online/months', async (req, res) => {
    try {
      const months = await dbOnline().collection('orders').distinct('month');
      res.status(200).json({ success: true, months: months });
    } catch (err) {
      console.error('월 목록 조회 에러:', err);
      res.status(500).json({ success: false, message: '서버 에러' });
    }
  });

  // 주문 데이터 (월별 & 판매처별)
  app.get('/api/online/orders', async (req, res) => {
    try {
      const { month, store } = req.query;
      const query = {};
      if (month) query.month = month;
      if (store && store !== 'all') query.store = store;

      const orders = await dbOnline().collection('orders').find(query).toArray();
      res.status(200).json({ success: true, orders: orders });
    } catch (err) {
      console.error('주문 데이터 조회 에러:', err);
      res.status(500).json({ success: false, message: '서버 에러' });
    }
  });

  // 이벤트 캘린더
  app.get('/api/online/events', async (req, res) => {
    try {
      const events = await dbOnline().collection('events').find({}).toArray();
      res.status(200).json({ success: true, events: events });
    } catch (err) {
      res.status(500).json({ success: false, message: '서버 에러' });
    }
  });

  // 마지막 업데이트 시간
  app.get('/api/online/system/last-update', async (req, res) => {
    try {
      const meta = await dbOnline().collection('system_metadata').findOne({ key: 'last_update_time' });
      res.status(200).json({ success: true, timestamp: meta ? meta.timestamp : null });
    } catch (err) {
      res.status(500).json({ success: false, message: '서버 에러' });
    }
  });

  // ── 팝업 스토어 ────────────────────────────────────────

  app.post('/api/popup/sales', async (req, res) => {
    try {
      const { items, customerName, customerPhone, customerAddress, memo } = req.body;
      const popupCol = dbOnline().collection('popup_sales');

      if (!items || !items.length) {
        return res.status(400).json({ success: false, message: '상품이 없습니다.' });
      }

      const now = new Date();
      const year = now.getFullYear();
      const month = String(now.getMonth() + 1).padStart(2, '0');
      const day = String(now.getDate()).padStart(2, '0');
      const isoDate = `${year}-${month}-${day}`;

      const newSales = items.map(item => {
        let price = 69000;
        if (item.product === '마인드 필로우') price = 59000;
        else if (item.product === '마인드 바디필로우') price = 65000;

        return {
          date: isoDate,
          timestamp: now,
          product: item.product,
          color: item.color,
          qty: Number(item.qty),
          price,
          totalAmount: price * Number(item.qty),
          customerName,
          customerPhone,
          customerAddress,
          memo,
          status: 'SALE'
        };
      });

      const result = await popupCol.insertMany(newSales);
      res.status(200).json({ success: true, message: '판매가 등록되었습니다.', count: result.insertedCount });
    } catch (err) {
      console.error('팝업 판매 등록 에러:', err);
      res.status(500).json({ success: false, message: '서버 에러 발생' });
    }
  });

  app.post('/api/popup/cancel', async (req, res) => {
    try {
      const { id } = req.body;
      if (!id) return res.status(400).json({ success: false, message: 'ID가 필요합니다.' });

      const result = await dbOnline().collection('popup_sales').updateOne(
        { _id: new ObjectId(id) },
        { $set: { status: 'CANCEL', cancelDate: new Date() } }
      );

      if (result.matchedCount === 0) {
        return res.status(404).json({ success: false, message: '대상을 찾을 수 없습니다.' });
      }
      res.status(200).json({ success: true, message: '취소되었습니다.' });
    } catch (err) {
      console.error('팝업 취소 에러:', err);
      res.status(500).json({ success: false, message: '서버 에러 발생' });
    }
  });

  app.get('/api/popup/data', async (req, res) => {
    try {
      const popupCol = dbOnline().collection('popup_sales');
      const invCol = dbOnline().collection('popup_inventory');

      const start = req.query.start;
      const end = req.query.end;

      const allSales = await popupCol.find({}).sort({ timestamp: -1 }).toArray();

      let inventoryData = await invCol.find({}).toArray();
      if (inventoryData.length === 0) {
        const initialSetup = [
          { _id: '마인드 필로우_화이트 미스트', total: 50 },
          { _id: '마인드 필로우_블루문', total: 30 },
          { _id: '마인드 필로우_핑크샌드', total: 50 },
          { _id: '마인드 바디필로우_화이트 미스트', total: 20 },
          { _id: '마인드 바디필로우_블루문', total: 20 },
          { _id: '마인드 바디필로우_핑크샌드', total: 20 }
        ];
        await invCol.insertMany(initialSetup);
        inventoryData = initialSetup;
      }

      const inventory = {};
      inventoryData.forEach(item => {
        inventory[item._id] = { total: item.total, sold: 0 };
      });

      allSales.forEach(sale => {
        const invKey = `${sale.product}_${sale.color}`;
        if (sale.status === 'SALE') {
          if (inventory[invKey]) {
            inventory[invKey].sold += sale.qty;
          }
        }
      });

      let sales = allSales;
      if (start || end) {
        sales = allSales.filter(s => {
          const sDate = s.date || new Date(s.timestamp).toISOString().split('T')[0];
          if (start && sDate < start) return false;
          if (end && sDate > end) return false;
          return true;
        });
      }

      const dailyData = {};
      let totalRevenue = 0;

      sales.forEach(sale => {
        const date = sale.date || new Date(sale.timestamp).toISOString().split('T')[0];

        if (!dailyData[date]) {
          dailyData[date] = { qty: 0, revenue: 0, cancelQty: 0, cancelRevenue: 0 };
        }

        if (sale.status === 'SALE') {
          dailyData[date].qty += sale.qty;
          dailyData[date].revenue += sale.totalAmount;
          totalRevenue += sale.totalAmount;
        } else if (sale.status === 'CANCEL') {
          dailyData[date].cancelQty += sale.qty;
          dailyData[date].cancelRevenue += sale.totalAmount;
        }
      });

      res.status(200).json({ success: true, inventory, dailyData, totalRevenue, sales });
    } catch (err) {
      console.error('팝업 데이터 조회 에러:', err);
      res.status(500).json({ success: false, message: '서버 에러 발생' });
    }
  });

  // 원본은 xlsx(SheetJS)로 만들었다. 이 서버에 이미 있는 exceljs로 같은 시트·같은 열을 만든다
  app.get('/api/popup/excel', async (req, res) => {
    try {
      const start = req.query.start;
      const end = req.query.end;

      const allSales = await dbOnline().collection('popup_sales').find({}).sort({ timestamp: -1 }).toArray();

      let sales = allSales;
      if (start || end) {
        sales = allSales.filter(s => {
          const sDate = s.date || new Date(s.timestamp).toISOString().split('T')[0];
          if (start && sDate < start) return false;
          if (end && sDate > end) return false;
          return true;
        });
      }

      if (sales.length === 0) {
        return res.status(404).send('다운로드할 데이터가 없습니다.');
      }

      const dataForExcel = sales.map((sale, index) => ({
        'No': index + 1,
        '판매일자': sale.date,
        '판매시간': new Date(sale.timestamp).toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' }),
        '구분': sale.status === 'SALE' ? '판매' : '취소입고',
        '상품명': sale.product,
        '색상': sale.color,
        '수량': sale.qty,
        '단위금액': sale.price,
        '총액': sale.totalAmount,
        '고객명': sale.customerName || '',
        '연락처': sale.customerPhone || '',
        '주소': sale.customerAddress || '',
        '취소일시': sale.cancelDate ? new Date(sale.cancelDate).toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' }) : '',
        '메모': sale.memo || ''
      }));

      const ExcelJS = require('exceljs');
      const wb = new ExcelJS.Workbook();
      const ws = wb.addWorksheet('현장판매내역');
      ws.columns = Object.keys(dataForExcel[0]).map(key => ({ header: key, key }));
      // xlsx(SheetJS)처럼 빈 값은 칸을 만들지 않는다
      dataForExcel.forEach(row => ws.addRow(Object.fromEntries(Object.entries(row).map(([k, v]) => [k, v === '' ? null : v]))));

      const fileBuffer = Buffer.from(await wb.xlsx.writeBuffer());

      res.setHeader('Content-Disposition', 'attachment; filename="popup_sales_data.xlsx"');
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.send(fileBuffer);
    } catch (err) {
      console.error('팝업 엑셀 다운로드 에러:', err);
      res.status(500).json({ success: false, message: '서버 에러 발생' });
    }
  });
}

module.exports = { mount };
