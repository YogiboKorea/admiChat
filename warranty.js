// ================================================================
// 정품인증 / 보증기간 계산 모듈
//  - 매칭키: 휴대폰번호 (Cafe24 회원 cellphone ↔ erp_sales.phone)
//  - 보증기간 = 구매일 + 12개월(기본) / 18개월(정품인증 시점이 MD 프로모션 기간 내)
// ================================================================
const crypto = require('crypto');

const PROMO_COLLECTION = 'warranty_promotions'; // MD 지정 보증연장 기간
const WARRANTY_COLLECTION = 'warranties';        // 확정된 정품인증/보증 기록

const DEFAULT_MONTHS = 12;
const PROMO_MONTHS = 18; // 1년 6개월

// 인증서 난수번호 발급 (혼동문자 0/1/I/O 제외, YGB-XXXXX-XXXXX)
const CERT_ALPHABET = '23456789ABCDEFGHJKLMNPQRSTUVWXYZ';
function genCertNo() {
  const bytes = crypto.randomBytes(10);
  let s = '';
  for (let i = 0; i < 10; i++) s += CERT_ALPHABET[bytes[i] % CERT_ALPHABET.length];
  return `YGB-${s.slice(0, 5)}-${s.slice(5, 10)}`;
}

/**
 * 정품인증 1건 저장(제품=rowHash 당 1회). 신규면 난수번호 발급, 이미 있으면 기존 반환.
 * certNo 충돌 시 재발급 재시도.
 */
async function insertWarranty(db, doc) {
  const col = db.collection(WARRANTY_COLLECTION);
  for (let attempt = 0; attempt < 6; attempt++) {
    const certNo = genCertNo();
    try {
      await col.insertOne({ ...doc, certNo, asStatus: null, asHistory: [] });
      return { certNo, inserted: true };
    } catch (e) {
      if (e.code === 11000) {
        if (e.keyPattern && e.keyPattern.rowHash) {
          const ex = await col.findOne({ rowHash: doc.rowHash });
          return { certNo: ex.certNo, inserted: false, existing: ex };
        }
        continue; // certNo 충돌 → 재발급
      }
      throw e;
    }
  }
  throw new Error('인증번호 생성 실패(재시도 초과)');
}

// 개월 수 더하기 (말일 보정 포함)
function addMonths(date, months) {
  const d = new Date(date);
  const day = d.getDate();
  d.setMonth(d.getMonth() + months);
  if (d.getDate() < day) d.setDate(0); // 예: 1/31 +1개월 → 2/28
  return d;
}

const ymd = (d) => (d ? new Date(d).toISOString().slice(0, 10) : null);

/**
 * 지정 시점(atDate)에 활성인 MD 프로모션(보증연장) 조회.
 * warranty_promotions 문서: { name, startDate, endDate, months, active }
 */
async function getActivePromotion(db, atDate = new Date()) {
  const at = new Date(atDate);
  return db.collection(PROMO_COLLECTION).findOne({
    active: { $ne: false },
    startDate: { $lte: at },
    endDate: { $gte: at },
  });
}

/**
 * 구매일 + (인증시점 프로모션 여부)로 보증기간 산출.
 * @returns {{ months, endDate, promotion }} promotion=적용된 프로모션(없으면 null)
 */
async function computeWarranty(db, saleDate, authDate = new Date()) {
  const promo = await getActivePromotion(db, authDate);
  const months = promo ? (promo.months || PROMO_MONTHS) : DEFAULT_MONTHS;
  return {
    months,
    endDate: addMonths(saleDate, months),
    promotion: promo ? { name: promo.name, months } : null,
  };
}

module.exports = {
  PROMO_COLLECTION,
  WARRANTY_COLLECTION,
  DEFAULT_MONTHS,
  PROMO_MONTHS,
  addMonths,
  ymd,
  genCertNo,
  insertWarranty,
  getActivePromotion,
  computeWarranty,
};
