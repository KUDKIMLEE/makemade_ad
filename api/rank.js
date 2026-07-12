// 네이버 검색광고 API로 키워드별 평균노출순위(avgRnk)를 조회하는 서버리스 함수.
// 비밀키는 이 서버 안에서만 서명에 쓰이고 브라우저로 나가지 않습니다.
//
//  · 단일일  : { ids, since, until }            → { from, to, stats, skipped }
//  · 추이(일별): { ids, since, until, daily:true } → { from, to, dates, series, skipped }
//
// /stats 가 한 번에 많은 ID를 거부(11001)하는 경우가 있어, 배치가 실패하면
// 절반씩 쪼개 재시도하고 끝까지 거부되는 개별 ID만 건너뜁니다(자기치유).

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
  return {
    'X-Timestamp': ts,
    'X-API-KEY': env('NAVER_API_KEY'),
    'X-Customer': env('NAVER_CUSTOMER_ID'),
    'X-Signature': signature,
    'Content-Type': 'application/json; charset=UTF-8',
  };
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

async function callStats(ids, date) {
  const data = await napi('GET', '/stats', {
    ids: JSON.stringify(ids),
    fields: JSON.stringify(STAT_FIELDS),
    timeRange: JSON.stringify({ since: date, until: date }),
  });
  return Array.isArray(data) ? data : (data.data || []);
}

// 배치 실패 시 절반씩 쪼개 재시도, 개별 ID까지 실패하면 skipped에 담고 건너뜀
async function fetchInto(ids, date, map, skipped) {
  if (!ids.length) return;
  try {
    const rows = await callStats(ids, date);
    for (const r of rows) {
      map[r.id] = {
        avgRnk: r.avgRnk && r.avgRnk > 0 ? r.avgRnk : null,
        impCnt: r.impCnt ?? 0, clkCnt: r.clkCnt ?? 0, ctr: r.ctr ?? 0, cpc: r.cpc ?? 0,
      };
    }
    await sleep(80);
  } catch (e) {
    if (ids.length === 1) { skipped.add(ids[0]); return; }
    const mid = Math.floor(ids.length / 2);
    await fetchInto(ids.slice(0, mid), date, map, skipped);
    await fetchInto(ids.slice(mid), date, map, skipped);
  }
}

async function statsForDate(ids, date, skipped) {
  const map = {};
  // 처음엔 20개씩 시도(대량 거부 회피), 실패하면 fetchInto가 더 쪼갬
  for (let i = 0; i < ids.length; i += 20) {
    await fetchInto(ids.slice(i, i + 20), date, map, skipped);
  }
  return map;
}

async function runPool(items, worker, concurrency = 4) {
  const results = new Array(items.length); let i = 0;
  async function next() { while (i < items.length) { const idx = i++; results[idx] = await worker(items[idx], idx); } }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, next));
  return results;
}

async function collectAllIds() {
  const campaigns = await napi('GET', '/ncc/campaigns');
  const ids = [];
  for (const c of campaigns) {
    const adgroups = await napi('GET', '/ncc/adgroups', { nccCampaignId: c.nccCampaignId });
    await sleep(120);
    for (const g of adgroups) {
      const keywords = await napi('GET', '/ncc/keywords', { nccAdgroupId: g.nccAdgroupId });
      await sleep(120);
      for (const k of keywords) ids.push(k.nccKeywordId);
    }
  }
  return ids;
}

function parseBody(req) { if (req.body && typeof req.body === 'object') return req.body; try { return JSON.parse(req.body || '{}'); } catch { return {}; } }

module.exports = async (req, res) => {
  if (req.method !== 'POST') { res.status(405).json({ error: 'POST 요청만 지원합니다.' }); return; }
  try {
    const { ids, since, until, daily } = parseBody(req);
    const to = until || yesterday();
    const from = since || to;
    const keywordIds = Array.isArray(ids) && ids.length ? ids : await collectAllIds();
    if (!keywordIds.length) { res.status(200).json({ from, to, stats: {}, series: {}, dates: [], skipped: [] }); return; }

    const skipped = new Set();

    if (daily) {
      const dates = dateRange(from, to);
      const perDay = await runPool(dates, (d) => statsForDate(keywordIds, d, skipped), 4);
      const series = {};
      keywordIds.forEach((id) => { series[id] = []; });
      dates.forEach((d, di) => {
        const m = perDay[di] || {};
        keywordIds.forEach((id) => {
          const s = m[id] || { avgRnk: null, impCnt: 0, clkCnt: 0, ctr: 0, cpc: 0 };
          series[id].push({ date: d, ...s });
        });
      });
      res.status(200).json({ from, to, dates, series, skipped: [...skipped] });
      return;
    }

    const stats = await statsForDate(keywordIds, to, skipped);
    res.status(200).json({ from, to, stats, skipped: [...skipped] });
  } catch (e) {
    res.status(500).json({ error: String((e && e.message) || e) });
  }
};
