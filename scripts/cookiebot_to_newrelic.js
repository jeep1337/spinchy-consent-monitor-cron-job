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

function httpRequest(method, url, headers = {}, body = null, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const req = https.request(url, { method, headers, timeout: timeoutMs }, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => resolve({ statusCode: res.statusCode || 0, body: data, headers: res.headers }));
    });
    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy(new Error(`Request timeout: ${method} ${url}`));
    });
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
  if (Array.isArray(raw)) return raw;
  if (raw && Array.isArray(raw.data)) return raw.data;
  if (raw && Array.isArray(raw.stats)) return raw.stats;
  return [];
}

function mapRowToEvent(row, meta) {
  return {
    eventType: 'SgtmConsentDaily',
    source: 'cookiebot',
    environment: meta.environment,
    domain: meta.domain,
    domainGroupId: meta.domainGroupId,
    date: row.date || row.Date || null,
    countryCode: row.countryCode || row.country || null,

    // Common consent counters (names vary by API payloads/accounts)
    consents: toNumberOrNull(row.consents ?? row.Consents),
    optIns: toNumberOrNull(row.optIns ?? row.optins ?? row.OptIns),
    optOuts: toNumberOrNull(row.optOuts ?? row.optouts ?? row.OptOuts),

    necessaryConsents: toNumberOrNull(row.necessaryConsents ?? row.necessary ?? row.strictOptIns),
    preferencesConsents: toNumberOrNull(row.preferencesConsents ?? row.preferences ?? row.preferencesOptIns),
    statisticsConsents: toNumberOrNull(row.statisticsConsents ?? row.statistics ?? row.statisticsOptIns),
    marketingConsents: toNumberOrNull(row.marketingConsents ?? row.marketing ?? row.marketingOptIns),

    // Technical metadata
    pulledAt: new Date().toISOString()
  };
}

async function main() {
  const COOKIEBOT_API_KEY = assertEnv('COOKIEBOT_API_KEY');
  const COOKIEBOT_DOMAIN_GROUP_ID = assertEnv('COOKIEBOT_DOMAIN_GROUP_ID');
  const COOKIEBOT_DOMAIN = assertEnv('COOKIEBOT_DOMAIN');
  const NEW_RELIC_ACCOUNT_ID = assertEnv('NEW_RELIC_ACCOUNT_ID');
  const NEW_RELIC_INGEST_KEY = assertEnv('NEW_RELIC_INGEST_KEY');
  const ENVIRONMENT = (process.env.ENVIRONMENT || 'prod').trim();

  // Test mode: pull a 7-day UTC window ending yesterday.
  const now = new Date();
  const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1));
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 7));
  const startdate = yyyymmdd(start);
  const enddate = yyyymmdd(end);

  const cookiebotUrl =
    `https://consent.cookiebot.com/api/v1/${COOKIEBOT_API_KEY}/json/domaingroup/${COOKIEBOT_DOMAIN_GROUP_ID}` +
    `/domain/${encodeURIComponent(COOKIEBOT_DOMAIN)}/consent/stats?startdate=${startdate}&enddate=${enddate}`;

  console.log(`[cookiebot] Fetching ${startdate}..${enddate} for ${COOKIEBOT_DOMAIN}`);

  const cbResp = await withRetry(() => httpRequest('GET', cookiebotUrl), 3, 1200);
  if (cbResp.statusCode < 200 || cbResp.statusCode >= 300) {
    throw new Error(`Cookiebot API failed: ${cbResp.statusCode} ${cbResp.body}`);
  }

  let cbJson;
  try {
    cbJson = JSON.parse(cbResp.body);
  } catch {
    throw new Error(`Cookiebot API returned invalid JSON: ${cbResp.body}`);
  }

  const rows = normalizeRows(cbJson);
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
