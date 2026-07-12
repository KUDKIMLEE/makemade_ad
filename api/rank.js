// 네이버 검색광고 API로 키워드별 평균노출순위(avgRnk)·추이를 조회하는 서버리스 함수.
// 비밀키는 이 서버 안에서만 서명에 쓰이고 브라우저로 나가지 않습니다.
//
// 핵심: 일별 추이(timeIncrement=1)는 "id(단수)" 로만 허용됨. ids(복수)+timeIncrement 는 11001로 거부됨.
//       → 추이는 키워드 하나씩 id로 조회(호출 수 = 키워드 수, 기간 길이와 무관).
//       → 단일일 스냅샷은 ids(복수) 배치로 빠르게.
// 견고성: 잘못된 요청(400/11001)은 해당 키워드만 skip, 일시 오류(429/5xx)는 재시도.

const crypto = require('crypto');
const BASE = 'https://api.naver.com';

function env(name) {
  const v = process.env[name];
  if (!v) throw new Error(`환경변수 ${name}가 설정되지 않았습니다. Vercel 프로젝트 설정에서 등록해 주세요.`);
  return v;
}
function buildHeaders(method, path) {
  const ts = Date.now().toString();
  const signature = crypto.createHmac('sha256', env('NAVER_SECRET_KEY')).update(`${ts}.${method}.${path}`).digest('base64');
  return { 'X-Timestamp': ts, 'X-API-KEY': env('NAVER_API_KEY'), 'X-Customer': env('NAVER_CUSTOMER_ID'), 'X-Signature': signature, 'Content-Type': 'application/json; charset=UTF-8' };
}
async function napi(method, path, query) {
  let url = BASE + path;
  if (query) url += '?' + new URLSearchParams(query).toString();
  const res = await fetch(url, { method, headers: buildHeaders(method, path) });
  const text = await res.text();
  if (!res.ok) { const e = new Error(`네이버 API ${res.status} (${path}) → ${text}`); e.status = res.status; throw e; }
  try { return JSON.parse(text); } catch { return text; }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function yesterday() { const d = new Date(); d.setDate(d.getDate() - 1); return d.toISOString().slice(0, 10); }
function dateRange(since, until) {
  const out = []; const d = new Date(since + 'T00:00:00'); const end = new Date(until + 'T00:00:00'); let g = 0;
  while (d <= end && g++ < 92) { out.push(d.toISOString().slice(0, 10)); d.setDate(d.getDate() + 1); }
  return out;
}
const STAT_FIELDS = ['impCnt', 'clkCnt', 'ctr', 'cpc', 'avgRnk'];

function detectDate(row) {
  for (const k of ['dt', 'statDt', 'date', 'dateStart', 'day', 'baseDt']) { if (row[k]) { const n = normDate(row[k]); if (n) return n; } }
  for (const v of Object.values(row)) { const n = normDate(v); if (n) return n; }
  return null;
}
function normDate(v) {
  if (typeof v !== 'string' && typeof v !== 'number') return null;
  const m = String(v).match(/(20\d{2})[-/.]?(\d{2})[-/.]?(\d{2})/);
  return m ? `${m[1]}-${m[2]}-${m[3]}` : null;
}
function mapStat(r) {
  return { avgRnk: r.avgRnk && r.avgRnk > 0 ? r.avgRnk : null, impCnt: r.impCnt ?? 0, clkCnt: r.clkCnt ?? 0, ctr: r.ctr ?? 0, cpc: r.cpc ?? 0 };
}
function emptyPoint(date) { return { date, avgRnk: null, impCnt: 0, clkCnt: 0, ctr: 0, cpc: 0 }; }

// 일시 오류는 재시도, 400(형식 오류)은 즉시 실패
async function withRetry(fn, tries = 3) {
  for (let i = 0; i < tries; i++) {
    try { return await fn(); }
    catch (e) { if (e.status === 400 || i === tries - 1) throw e; await sleep(400 * (i + 1)); }
  }
}

// 단일 키워드의 일별 추이 (id 단수 + timeIncrement=1)
async function statsDailyForKeyword(id, since, until) {
  const data = await napi('GET', '/stats', {
    id, fields: JSON.stringify(STAT_FIELDS),
    timeRange: JSON.stringify({ since, until }), timeIncrement: '1',
  });
  return Array.isArray(data) ? data : (data.data || []);
}

// 단일일 다건 스냅샷 (ids 복수, timeIncrement 없음)
async function statsSnapshot(ids, date) {
  const data = await napi('GET', '/stats', {
    ids: JSON.stringify(ids), fields: JSON.stringify(STAT_FIELDS),
    timeRange: JSON.stringify({ since: date, until: date }),
  });
  return Array.isArray(data) ? data : (data.data || []);
}

async function runPool(items, worker, concurrency = 6) {
  const out = new Array(items.length); let i = 0;
  async function next() { while (i < items.length) { const idx = i++; out[idx] = await worker(items[idx], idx); } }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, next));
  return out;
}

async function collectAllIds() {
  const campaigns = await napi('GET', '/ncc/campaigns'); const ids = [];
  for (const c of campaigns) {
    const adgroups = await napi('GET', '/ncc/adgroups', { nccCampaignId: c.nccCampaignId }); await sleep(120);
    for (const g of adgroups) {
      const keywords = await napi('GET', '/ncc/keywords', { nccAdgroupId: g.nccAdgroupId }); await sleep(120);
      for (const k of keywords) ids.push(k.nccKeywordId);
    }
  }
  return ids;
}

async function buildSeries(ids, dates, skipped) {
  const since = dates[0], until = dates[dates.length - 1];
  const idx = Object.fromEntries(dates.map((d, i) => [d, i]));
  const series = {}; ids.forEach((id) => { series[id] = dates.map(emptyPoint); });

  await runPool(ids, async (kwId) => {
    try {
      const rows = await withRetry(() => statsDailyForKeyword(kwId, since, until));
      if (!rows.length) { await sleep(30); return; }
      const dated = rows.map((r) => ({ d: detectDate(r), r }));
      const allDated = dated.every((x) => x.d);
      if (allDated) {
        for (const { d, r } of dated) if (idx[d] != null) series[kwId][idx[d]] = { date: d, ...mapStat(r) };
      } else if (rows.length === dates.length) {
        // 날짜 필드 인식 실패 + 개수 일치 → 시간순 가정하고 인덱스 매핑
        rows.forEach((r, i) => { series[kwId][i] = { date: dates[i], ...mapStat(r) }; });
      } else {
        // 부분: 인식된 날짜만 반영 (best effort)
        for (const { d, r } of dated) if (d && idx[d] != null) series[kwId][idx[d]] = { date: d, ...mapStat(r) };
      }
      await sleep(30);
    } catch (e) {
      skipped.add(kwId); // 400/11001 등 → 해당 키워드만 건너뜀
    }
  }, 6);
  return series;
}

function parseBody(req) { if (req.body && typeof req.body === 'object') return req.body; try { return JSON.parse(req.body || '{}'); } catch { return {}; } }

module.exports = async (req, res) => {
  if (req.method !== 'POST') { res.status(405).json({ error: 'POST 요청만 지원합니다.' }); return; }
  try {
    const { ids, since, until, daily } = parseBody(req);
    const to = until || yesterday(); const from = since || to;
    const keywordIds = Array.isArray(ids) && ids.length ? ids : await collectAllIds();
    if (!keywordIds.length) { res.status(200).json({ from, to, stats: {}, series: {}, dates: [], skipped: [] }); return; }

    const skipped = new Set();
    if (daily) {
      const dates = dateRange(from, to);
      const series = await buildSeries(keywordIds, dates, skipped);
      res.status(200).json({ from, to, dates, series, skipped: [...skipped] });
      return;
    }
    // 단일일 스냅샷 (ids 배치)
    const map = {};
    for (let i = 0; i < keywordIds.length; i += 100) {
      try { const rows = await statsSnapshot(keywordIds.slice(i, i + 100), to); for (const r of rows) map[r.id] = mapStat(r); }
      catch (e) { keywordIds.slice(i, i + 100).forEach((id) => skipped.add(id)); }
      await sleep(80);
    }
    res.status(200).json({ from, to, stats: map, skipped: [...skipped] });
  } catch (e) {
    res.status(500).json({ error: String((e && e.message) || e) });
  }
};
