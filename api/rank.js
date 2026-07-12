// 네이버 검색광고 API로 키워드별 평균노출순위(avgRnk)를 조회하는 서버리스 함수.
// 비밀키는 이 서버 안에서만 서명에 쓰이고 브라우저로 나가지 않습니다.
//
// 두 가지 모드:
//  · 단일일  : { ids, since, until }            → { from, to, stats: { id: {...} } }
//  · 추이(일별): { ids, since, until, daily:true } → { from, to, dates:[...], series: { id: [{date,...}] } }
//
// 추이는 날짜별로 각각 조회해 날짜를 직접 태깅합니다(날짜 축이 정확). 요청은 소량 동시 실행.

const crypto = require('crypto');
const BASE = 'https://api.naver.com';

function env(name) {
  const v = process.env[name];
  if (!v) throw new Error(`환경변수 ${name}가 설정되지 않았습니다. Vercel 프로젝트 설정에서 등록해 주세요.`);
  return v;
}

function buildHeaders(method, path) {
  const ts = Date.now().toString();
  const signature = crypto
    .createHmac('sha256', env('NAVER_SECRET_KEY'))
    .update(`${ts}.${method}.${path}`)
    .digest('base64');
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
  if (!res.ok) throw new Error(`네이버 API ${res.status} (${path}) → ${await res.text()}`);
  return res.json();
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function chunk(a, n) { const o = []; for (let i = 0; i < a.length; i += n) o.push(a.slice(i, i + n)); return o; }
function yesterday() { const d = new Date(); d.setDate(d.getDate() - 1); return d.toISOString().slice(0, 10); }

function dateRange(since, until) {
  const out = [];
  const d = new Date(since + 'T00:00:00');
  const end = new Date(until + 'T00:00:00');
  let guard = 0;
  while (d <= end && guard++ < 92) { out.push(d.toISOString().slice(0, 10)); d.setDate(d.getDate() + 1); }
  return out;
}

// 특정 하루(aggregated)에 대한 키워드별 지표
async function statsForDate(ids, date) {
  const fields = ['impCnt', 'clkCnt', 'ctr', 'cpc', 'avgRnk'];
  const map = {};
  for (const group of chunk(ids, 100)) {
    const data = await napi('GET', '/stats', {
      ids: JSON.stringify(group),
      fields: JSON.stringify(fields),
      timeRange: JSON.stringify({ since: date, until: date }),
    });
    const rows = Array.isArray(data) ? data : data.data || [];
    for (const r of rows) {
      map[r.id] = {
        avgRnk: r.avgRnk && r.avgRnk > 0 ? r.avgRnk : null,
        impCnt: r.impCnt ?? 0,
        clkCnt: r.clkCnt ?? 0,
        ctr: r.ctr ?? 0,
        cpc: r.cpc ?? 0,
      };
    }
    await sleep(100);
  }
  return map;
}

// 동시 실행 풀 (rate limit 여유)
async function runPool(items, worker, concurrency = 4) {
  const results = new Array(items.length);
  let i = 0;
  async function next() {
    while (i < items.length) {
      const idx = i++;
      results[idx] = await worker(items[idx], idx);
    }
  }
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

function parseBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body || '{}'); } catch { return {}; }
}

module.exports = async (req, res) => {
  if (req.method !== 'POST') { res.status(405).json({ error: 'POST 요청만 지원합니다.' }); return; }
  try {
    const { ids, since, until, daily } = parseBody(req);
    const to = until || yesterday();
    const from = since || to;

    const keywordIds = Array.isArray(ids) && ids.length ? ids : await collectAllIds();
    if (!keywordIds.length) { res.status(200).json({ from, to, stats: {}, series: {}, dates: [] }); return; }

    if (daily) {
      const dates = dateRange(from, to);
      const perDay = await runPool(dates, (d) => statsForDate(keywordIds, d), 4);
      const series = {};
      keywordIds.forEach((id) => { series[id] = []; });
      dates.forEach((d, di) => {
        const m = perDay[di] || {};
        keywordIds.forEach((id) => {
          const s = m[id] || { avgRnk: null, impCnt: 0, clkCnt: 0, ctr: 0, cpc: 0 };
          series[id].push({ date: d, ...s });
        });
      });
      res.status(200).json({ from, to, dates, series });
      return;
    }

    const stats = await statsForDate(keywordIds, to);
    res.status(200).json({ from, to, stats });
  } catch (e) {
    res.status(500).json({ error: String((e && e.message) || e) });
  }
};
