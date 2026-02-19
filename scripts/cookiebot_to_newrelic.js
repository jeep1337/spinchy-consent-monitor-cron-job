const https = require('https');

function assertEnv(name) {
  const v = process.env[name];
  if (!v || String(v).trim() === '') {
    throw new Error(`Missing required env var: ${name}`);
  }
  return String(v).trim();
}

function yyyymmdd(date) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, '0');
  const d = String(date.getUTCDate()).padStart(2, '0');
  return `${y}${m}${d}`;
}

function parseDateYyyymmdd(value) {
  if (!value) return null;
  const s = String(value).trim();
  if (!/^\d{8}$/.test(s)) return null;
  const y = Number(s.slice(0, 4));
  const m = Number(s.slice(4, 6)) - 1;
  const d = Number(s.slice(6, 8));
  return new Date(Date.UTC(y, m, d));
}

function httpRequest(method, url, headers = {}, body = null, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const req = https.request(url, { method, headers, timeout: timeoutMs }, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => resolve({ statusCode: res.statusCode || 0, body: data, headers: res.headers }));
    });
    req.on('error', reject);
    req.on('timeout', () => req.destroy(new Error(`Request timeout: ${method} ${url}`)));
    if (body) req.write(body);
    req.end();
  });
}

async function withRetry(fn, attempts = 3, baseDelayMs = 1200) {
  let lastErr;
  for (let i = 1; i <= attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (i < attempts) {
        const delay = baseDelayMs * i;
        console.log(`[retry] attempt ${i} failed, waiting ${delay}ms...`);
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }
  throw lastErr;
}

function toNumberOrNull(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function normalizeRows(raw) {
  if (!raw) return [];
  if (Array.isArray(raw)) return raw;
  if (Array.isArray(raw.data)) return raw.data;
  if (Array.isArray(raw.stats)) return raw.stats;
  if (raw.consentstat && Array.isArray(raw.consentstat.consentday)) return raw.consentstat.consentday;
  if (raw.consentstat && raw.consentstat.consentday && typeof raw.consentstat.consentday === 'object') {
    return [raw.consentstat.consentday];
  }
  if (raw.ConsentStat && Array.isArray(raw.ConsentStat.ConsentDay)) return raw.ConsentStat.ConsentDay;
  if (raw.ConsentStat && raw.ConsentStat.ConsentDay && typeof raw.ConsentStat.ConsentDay === 'object') {
    return [raw.ConsentStat.ConsentDay];
  }
  if (raw.result && Array.isArray(raw.result.data)) return raw.result.data;
  if (raw.result && Array.isArray(raw.result.stats)) return raw.result.stats;
  if (raw.payload && Array.isArray(raw.payload.data)) return raw.payload.data;
  if (raw.payload && Array.isArray(raw.payload.stats)) return raw.payload.stats;
  return [];
}

function mapRowToEvent(row, meta) {
  const optIns = toNumberOrNull(row.optIns ?? row.optins ?? row.OptIns ?? row.OptIn);
  const optOuts = toNumberOrNull(row.optOuts ?? row.optouts ?? row.OptOuts ?? row.OptOut);
  const optInImplied = toNumberOrNull(row.optInImplied ?? row.OptInImplied);

  return {
    eventType: 'SgtmConsentDaily',
    source: 'cookiebot',
    environment: meta.environment,
    domain: meta.domain,
    domainGroupId: meta.domainGroupId,
    date: row.date || row.Date || null,
    countryCode: row.countryCode || row.country || null,
    consents: toNumberOrNull(row.consents ?? row.Consents) ?? (optIns !== null && optOuts !== null ? optIns + optOuts : optIns),
    optIns: optIns,
    optOuts: optOuts,
    optInImplied: optInImplied,
    necessaryConsents: toNumberOrNull(row.necessaryConsents ?? row.necessary ?? row.strictOptIns ?? row.OptInStrict),
    preferencesConsents: toNumberOrNull(row.preferencesConsents ?? row.preferences ?? row.preferencesOptIns ?? row.TypeOptInPref),
    statisticsConsents: toNumberOrNull(row.statisticsConsents ?? row.statistics ?? row.statisticsOptIns ?? row.TypeOptInStat),
    marketingConsents: toNumberOrNull(row.marketingConsents ?? row.marketing ?? row.marketingOptIns ?? row.TypeOptInMark),
    pulledAt: new Date().toISOString()
  };
}

function getDateRange() {
  const STARTDATE = (process.env.STARTDATE || '').trim();
  const ENDDATE = (process.env.ENDDATE || '').trim();
  const LOOKBACK_DAYS = Number((process.env.LOOKBACK_DAYS || '1').trim());

  if (STARTDATE && ENDDATE) {
    const start = parseDateYyyymmdd(STARTDATE);
    const end = parseDateYyyymmdd(ENDDATE);
    if (!start || !end) throw new Error('STARTDATE/ENDDATE must be YYYYMMDD.');
    if (start > end) throw new Error('STARTDATE cannot be after ENDDATE.');
    return { startdate: STARTDATE, enddate: ENDDATE, mode: 'explicit' };
  }

  const now = new Date();
  const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1));
  const days = Number.isFinite(LOOKBACK_DAYS) && LOOKBACK_DAYS > 0 ? Math.floor(LOOKBACK_DAYS) : 1;
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - days));
  return { startdate: yyyymmdd(start), enddate: yyyymmdd(end), mode: `lookback_${days}d` };
}

async function main() {
  const COOKIEBOT_API_KEY = assertEnv('COOKIEBOT_API_KEY');
  const COOKIEBOT_DOMAIN_GROUP_ID = assertEnv('COOKIEBOT_DOMAIN_GROUP_ID');
  const COOKIEBOT_DOMAIN = assertEnv('COOKIEBOT_DOMAIN');
  const NEW_RELIC_ACCOUNT_ID = assertEnv('NEW_RELIC_ACCOUNT_ID');
  const NEW_RELIC_INGEST_KEY = assertEnv('NEW_RELIC_INGEST_KEY');
  const ENVIRONMENT = (process.env.ENVIRONMENT || 'prod').trim();

  const range = getDateRange();
  const cookiebotUrl =
    `https://consent.cookiebot.com/api/v1/${COOKIEBOT_API_KEY}/json/domaingroup/${COOKIEBOT_DOMAIN_GROUP_ID}` +
    `/domain/${encodeURIComponent(COOKIEBOT_DOMAIN)}/consent/stats?startdate=${range.startdate}&enddate=${range.enddate}`;

  console.log(`[cookiebot] Fetching ${range.startdate}..${range.enddate} for ${COOKIEBOT_DOMAIN} (${range.mode})`);
  const cbResp = await withRetry(() => httpRequest('GET', cookiebotUrl), 3, 1200);
  console.log(`[cookiebot] HTTP ${cbResp.statusCode}`);

  if (cbResp.statusCode < 200 || cbResp.statusCode >= 300) {
    throw new Error(`Cookiebot API failed: ${cbResp.statusCode} ${cbResp.body}`);
  }

  let cbJson;
  try {
    cbJson = JSON.parse(cbResp.body);
  } catch {
    throw new Error(`Cookiebot API returned invalid JSON: ${cbResp.body}`);
  }

  const keys = cbJson && typeof cbJson === 'object' ? Object.keys(cbJson) : [];
  console.log(`[cookiebot] root keys: ${keys.join(', ') || '(none)'}`);
  console.log(`[cookiebot] raw preview: ${JSON.stringify(cbJson).slice(0, 900)}`);

  const rows = normalizeRows(cbJson);
  console.log(`[cookiebot] normalized rows: ${rows.length}`);
  if (!rows.length) {
    console.log('[cookiebot] No rows returned. Exiting cleanly.');
    return;
  }

  const events = rows.map((row) =>
    mapRowToEvent(row, {
      environment: ENVIRONMENT,
      domain: COOKIEBOT_DOMAIN,
      domainGroupId: COOKIEBOT_DOMAIN_GROUP_ID
    })
  );

  const nrUrl = `https://insights-collector.eu01.nr-data.net/v1/accounts/${NEW_RELIC_ACCOUNT_ID}/events`;
  const payload = JSON.stringify(events);
  console.log(`[newrelic] Sending ${events.length} events to account ${NEW_RELIC_ACCOUNT_ID}`);

  const nrResp = await withRetry(
    () =>
      httpRequest(
        'POST',
        nrUrl,
        {
          'Api-Key': NEW_RELIC_INGEST_KEY,
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload)
        },
        payload
      ),
    3,
    1500
  );

  if (nrResp.statusCode < 200 || nrResp.statusCode >= 300) {
    throw new Error(`New Relic ingest failed: ${nrResp.statusCode} ${nrResp.body}`);
  }

  console.log(`[newrelic] Sent ${events.length} SgtmConsentDaily events. Status=${nrResp.statusCode}`);
}

main().catch((err) => {
  console.error(err.stack || err.message || err);
  process.exit(1);
});
