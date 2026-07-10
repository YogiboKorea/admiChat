// ================================================================
// ERP(이카운트) 판매현황 엑셀 적재 모듈
//  - data.xlsx (이카운트 매크로 다운로드본)을 파싱하여
//    정규화된 판매 라인(품목 단위) 배열로 변환하고 MongoDB에 upsert
//  - 매칭 키: 고객연락처(휴대폰) 정규화값
// ================================================================
const ExcelJS = require('exceljs');
const crypto = require('crypto');

const ERP_COLLECTION = 'erp_sales';

// 엑셀 매크로가 내려받는 고정 경로 (필요 시 .env ERP_FILE_PATH 로 덮어쓰기)
const DEFAULT_ERP_FILE = process.env.ERP_FILE_PATH
  || 'C:/Users/Yogibo Design/Desktop/mdPoint/file/data.xlsx';

// --- 셀 값 정리 (수식/리치텍스트 객체 → 문자열) ---
function cellText(v) {
  if (v === null || v === undefined) return '';
  if (typeof v === 'object') {
    if (v.result !== undefined) return cellText(v.result);
    if (v.text !== undefined) return cellText(v.text);
    if (v.richText) return v.richText.map(t => t.text).join('');
    if (v instanceof Date) return v.toISOString();
  }
  return String(v).trim();
}

// --- 휴대폰번호 정규화 & 분류 ---
// 숫자만 남기고, 매칭 가능한 유형인지 판별한다.
function normalizePhone(raw) {
  let digits = String(raw || '').replace(/[^0-9]/g, '');
  // +82 / 82 국가코드로 시작하는 010 → 0으로 치환
  if (/^82(10\d{8})$/.test(digits)) digits = '0' + digits.slice(2);
  let type = 'empty';
  if (!digits) type = 'empty';
  else if (/^010\d{8}$/.test(digits)) type = 'mobile';   // 매칭 가능
  else if (/^050/.test(digits)) type = 'safe';           // 안심번호(050) → 매칭 불가
  else type = 'other';                                    // 유선/불완전 번호
  return { digits, type };
}

// --- 일자 파싱: "2026/07/01 -3" → { date: Date, dateStr:'2026-07-01', seq:'3' } ---
function parseSaleDate(raw) {
  const s = cellText(raw);
  const m = s.match(/(\d{4})[\/\-.](\d{1,2})[\/\-.](\d{1,2})/);
  if (!m) return { date: null, dateStr: '', seq: '' };
  const [, y, mo, d] = m;
  const dateStr = `${y}-${String(mo).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
  // 시간대 이슈 없이 자정(KST) 기준으로 저장
  const date = new Date(`${dateStr}T00:00:00+09:00`);
  const seqMatch = s.match(/-\s*(\d+)\s*$/);
  return { date, dateStr, seq: seqMatch ? seqMatch[1] : '' };
}

// --- 숫자 파싱 (콤마 제거) ---
function parseNum(raw) {
  const s = cellText(raw).replace(/,/g, '');
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

// 헤더명 → 표준 필드 매핑 (이카운트 컬럼명 기준, 공백 무시 비교)
const HEADER_MAP = {
  '일자': 'date',
  '관리항목명': 'manageItem',
  '예약출고일': 'reserveDate',
  '거래처명': 'channel',
  '창고명': 'warehouse',
  '품목코드': 'productCode',
  '품목명': 'productName',
  '규격': 'spec',
  '수량': 'qty',
  '합계': 'amount',
  '프로모션1': 'promo1',
  '프로모션2': 'promo2',
  '고객명': 'customerName',
  '고객연락처': 'phone',
  '고객주소': 'address',
  '마케팅정보수신동의여부': 'marketingConsent',
  '특이사항': 'note',
};
const squash = (s) => cellText(s).replace(/\s+/g, '');

/**
 * data.xlsx 를 읽어 정규화된 판매 라인 배열과 요약 통계를 반환한다.
 * @returns {{ rows: object[], stats: object, headerRow: number }}
 */
async function parseErpWorkbook(filePath = DEFAULT_ERP_FILE) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.readFile(filePath);

  // '고객연락처' 헤더가 있는 시트를 선택 (없으면 첫 시트)
  let ws = null, headerRow = -1, colOf = {};
  wb.eachSheet((sheet) => {
    if (ws) return;
    for (let r = 1; r <= Math.min(6, sheet.rowCount); r++) {
      const map = {};
      for (let c = 1; c <= sheet.columnCount; c++) {
        const key = HEADER_MAP[squash(sheet.getRow(r).getCell(c).value)];
        if (key) map[key] = c;
      }
      if (map.phone && map.productCode) { ws = sheet; headerRow = r; colOf = map; return; }
    }
  });
  if (!ws) throw new Error('판매현황 헤더(고객연락처/품목코드)를 찾을 수 없습니다.');

  const get = (row, key) => (colOf[key] ? cellText(row.getCell(colOf[key]).value) : '');

  const rows = [];
  const stats = {
    totalRows: 0, kept: 0, skipped: 0,
    phoneMobile: 0, phoneSafe: 0, phoneOther: 0, phoneEmpty: 0,
    uniquePhones: new Set(), uniqueMatchable: new Set(),
  };

  for (let r = headerRow + 1; r <= ws.rowCount; r++) {
    const row = ws.getRow(r);
    const customerName = get(row, 'customerName');
    const rawPhone = get(row, 'phone');
    const productCode = get(row, 'productCode');
    const rawDate = get(row, 'date');

    // 합계행/빈행 스킵: 고객·연락처·품목코드가 모두 없으면 데이터가 아님
    if (!customerName && !rawPhone && !productCode) { continue; }
    stats.totalRows++;
    if (!rawDate || !productCode) { stats.skipped++; continue; }

    const { digits: phone, type: phoneType } = normalizePhone(rawPhone);
    const { date, dateStr, seq } = parseSaleDate(rawDate);

    const rec = {
      saleDate: date,
      saleDateStr: dateStr,
      orderSeq: seq,
      orderKey: dateStr && seq ? `${dateStr}#${seq}` : '',
      channel: get(row, 'channel'),
      warehouse: get(row, 'warehouse'),
      manageItem: get(row, 'manageItem'),
      productCode,
      productName: get(row, 'productName'),
      spec: get(row, 'spec'),
      qty: parseNum(get(row, 'qty')),
      amount: parseNum(get(row, 'amount')),
      promo1: get(row, 'promo1'),
      promo2: get(row, 'promo2'),
      customerName,
      phone,               // ★ 매칭 키 (숫자만)
      phoneRaw: rawPhone,
      phoneType,           // mobile | safe | other | empty
      matchable: phoneType === 'mobile',
      address: get(row, 'address'),
      marketingConsent: get(row, 'marketingConsent'),
      note: get(row, 'note'),
    };

    // 멱등 재적재용 해시 (동일 라인은 항상 같은 rowHash)
    rec.rowHash = crypto.createHash('sha1').update([
      rec.saleDateStr, rec.orderSeq, rec.channel, rec.productCode,
      rec.spec, rec.qty, rec.amount, rec.customerName, rec.phone,
    ].join('|')).digest('hex');

    // 통계
    stats[`phone${phoneType[0].toUpperCase()}${phoneType.slice(1)}`]++;
    if (phone) stats.uniquePhones.add(phone);
    if (rec.matchable) stats.uniqueMatchable.add(phone);
    stats.kept++;
    rows.push(rec);
  }

  const finalStats = {
    totalRows: stats.totalRows,
    kept: stats.kept,
    skipped: stats.skipped,
    phoneMobile: stats.phoneMobile,
    phoneSafe: stats.phoneSafe,
    phoneOther: stats.phoneOther,
    phoneEmpty: stats.phoneEmpty,
    uniquePhones: stats.uniquePhones.size,
    uniqueMatchablePhones: stats.uniqueMatchable.size,
  };
  return { rows, stats: finalStats, headerRow };
}

/**
 * 정규화된 라인들을 MongoDB erp_sales 컬렉션에 upsert (rowHash 기준 멱등).
 * @param {import('mongodb').Db} db
 * @param {object[]} rows
 */
async function ingestErpSales(db, rows) {
  const col = db.collection(ERP_COLLECTION);
  await col.createIndex({ rowHash: 1 }, { unique: true });
  await col.createIndex({ phone: 1 });
  await col.createIndex({ saleDate: 1 });

  if (!rows.length) return { upserted: 0, modified: 0, matched: 0 };

  const now = new Date();
  const ops = rows.map((r) => ({
    updateOne: {
      filter: { rowHash: r.rowHash },
      update: { $set: { ...r, updatedAt: now }, $setOnInsert: { ingestedAt: now } },
      upsert: true,
    },
  }));

  // 대량 처리: 500개 단위로 분할
  let upserted = 0, modified = 0, matched = 0;
  for (let i = 0; i < ops.length; i += 500) {
    const res = await col.bulkWrite(ops.slice(i, i + 500), { ordered: false });
    upserted += res.upsertedCount || 0;
    modified += res.modifiedCount || 0;
    matched += res.matchedCount || 0;
  }
  return { upserted, modified, matched, total: rows.length };
}

module.exports = {
  ERP_COLLECTION,
  DEFAULT_ERP_FILE,
  cellText,
  normalizePhone,
  parseSaleDate,
  parseErpWorkbook,
  ingestErpSales,
};
