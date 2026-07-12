// 네이버 검색광고 API로 키워드별 평균노출순위(avgRnk)·추이를 조회하는 서버리스 함수.
// 비밀키는 이 서버 안에서만 서명에 쓰이고 브라우저로 나가지 않습니다.
//
// 속도: 추이는 timeIncrement=1 로 기간 전체를 "한 번에 일별"로 받아옵니다(호출 수 대폭 감소).
//       응답에서 날짜를 자동 인식하며, 인식 실패 시 날짜별 개별조회로 자동 폴백(정확성 보장).
// 견고성: /stats 가 많은 ID를 거부(11001)하면 절반씩 쪼개 재시도, 끝까지 거부되는 ID만 건너뜀.

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

// 응답 행에서 날짜(YYYY-MM-DD) 추출 (필드명이 dt/statDt/date 등 무엇이든 대응)
function detectDate(row) {
  for (const k of ['dt', 'statDt', 'date', 'dateStart', 'day', 'baseDt']) {
    if (row[k]) { const n = normDate(row[k]); if (n) return n; }
  }
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

async function callStats(ids, since, until, daily) {
  const params = { ids: JSON.stringify(ids), fields: JSON.stringify(STAT_FIELDS), timeRange: JSON.stringify({ since, until }) };
  if (daily) params.timeIncrement = '1';
  const data = await napi('GET', '/stats', params);
  return Array.isArray(data) ? data : (data.data || []);
}

// 견고 조회: 실패하면 절반씩 쪼개 재시도, 개별 ID까지 실패하면 skipped에 담고 건너뜀
async function fetchRows(ids, since, until, daily, onRows, skipped) {
  if (!ids.length) return;
  try { onRows(await callStats(ids, since, until, daily)); await sleep(50); }
  catch (e) {
    if (ids.length === 1) { skipped.add(ids[0]); return; }
    const mid = Math.floor(ids.length / 2);
    await fetchRows(ids.slice(0, mid), since, until, daily, onRows, skipped);
    await fetchRows(ids.slice(mid), since, until, daily, onRows, skipped);
  }
}
async function runPool(items, worker, concurrency = 4) {
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

function emptyPoint(date) { return { date, avgRnk: null, impCnt: 0, clkCnt: 0, ctr: 0, cpc: 0 }; }

// 추이: 우선 timeIncrement로 한 번에, 날짜 인식 실패하면 날짜별 개별조회로 폴백
async function buildSeries(ids, dates, skipped) {
  const since = dates[0], until = dates[dates.length - 1];
  const idx = Object.fromEntries(dates.map((d, i) => [d, i]));
  const series = {}; ids.forEach((id) => { series[id] = dates.map(emptyPoint); });

  let sawDated = false, sawUndated = false;
  const onRows = (rows) => {
    for (const r of rows) {
      if (!series[r.id]) continue;
      const d = detectDate(r);
      if (d && idx[d] != null) { sawDated = true; series[r.id][idx[d]] = { date: d, ...mapStat(r) }; }
      else sawUndated = true;
    }
  };

  // 1) 빠른 경로: timeIncrement로 배치 조회 (배치는 병렬)
  const batches = [];
  for (let i = 0; i < ids.length; i += 30) batches.push(ids.slice(i, i + 30));
  await runPool(batches, (b) => fetchRows(b, since, until, true, onRows, skipped), 4);

  // 2) 폴백: 일별로 행이 오긴 했는데 날짜를 못 읽었다면 → 날짜별 개별조회로 재구성
  if (sawUndated && !sawDated) {
    ids.forEach((id) => { series[id] = dates.map(emptyPoint); });
    skipped.clear();
    const perDay = await runPool(dates, async (date) => {
      const map = {};
      await fetchRows(ids, date, date, false, (rows) => { for (const r of rows) if (series[r.id]) map[r.id] = mapStat(r); }, skipped);
      return map;
    }, 5);
    dates.forEach((date, di) => { const m = perDay[di] || {}; ids.forEach((id) => { if (m[id]) series[id][di] = { date, ...m[id] }; }); });
  }
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
    // 단일일
    const map = {};
    await fetchRows(keywordIds, to, to, false, (rows) => { for (const r of rows) map[r.id] = mapStat(r); }, skipped);
    res.status(200).json({ from, to, stats: map, skipped: [...skipped] });
  } catch (e) {
    res.status(500).json({ error: String((e && e.message) || e) });
  }
};
