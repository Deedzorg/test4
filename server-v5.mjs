import http from 'node:http';
import { spawn } from 'node:child_process';
import { randomUUID, timingSafeEqual } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { hostname } from 'node:os';
import pg from 'pg';
import { WebSocketServer } from 'ws';

const { Pool } = pg;
const VERSION = '1.3.1';
const PORT = Number(process.env.PORT || 3000);
const INTERNAL_PORT = PORT + 137;
const HOST = process.env.HOST || '0.0.0.0';
const RUN_ID = randomUUID();
const INSTANCE_ID = randomUUID();
const STARTED_AT = new Date().toISOString();
const ADMIN_KEY = process.env.GAUNTLET_ADMIN_KEY || '';
const DATABASE_URL = process.env.DATABASE_URL || '';
const RELEASE = { releaseId: 'v1.3-clear-reporting', experiment: 'reproducibility-observability' };
const TEST_CATALOG = JSON.parse(await readFile(new URL('./gauntlet-tests.json', import.meta.url), 'utf8')).tests;
const TESTS = new Map(TEST_CATALOG.map((test) => [test.key, test]));

const LEGACY_ALIASES = new Map(Object.entries({
  'CORE-RUNTIME-001': 'runtime',
  'NET-DYNAMIC-PORT-001': 'dynamic-port',
  'ENV-SENTINEL-001': 'environment-injection',
  'STORAGE-WRITE-001': 'writable-storage',
  'STORAGE-CONTINUITY-001': 'storage-persistence',
  'DB-CONNECT-001': 'database-connection',
  'DB-CONTINUITY-001': 'database-persistence',
  'RESTART-RECOVERY-001': 'restart-recovery',
  'SHUTDOWN-GRACEFUL-001': 'graceful-shutdown',
  'PROXY-HTTPS-001': 'https-proxy',
  'WS-PROXY-ECHO-001': 'websocket-proxy',
  'READINESS-SEPARATION-001': 'readiness-liveness',
  'OBS-LOG-MARKER-001': 'runtime-log-capture'
}));

let pool = null;
let publicServer = null;
let wss = null;
let child = null;
let shuttingDown = false;
let requests = 0;
let wsConnections = 0;
let readinessOverride = { active: false, until: null, correlationId: null };
let correlations = [];

child = spawn(process.execPath, ['server-v3.mjs'], {
  env: { ...process.env, PORT: String(INTERNAL_PORT), HOST: '127.0.0.1' },
  stdio: ['ignore', 'pipe', 'pipe']
});
child.stdout.on('data', (chunk) => process.stdout.write(chunk));
child.stderr.on('data', (chunk) => process.stderr.write(chunk));
child.on('exit', (code, signal) => log('child.exit', { code, signal, level: code === 0 ? 'info' : 'error' }));

await waitForChild();
await initializeDatabase();
await lifecycle('boot', { childPid: child.pid, internalPort: INTERNAL_PORT, node: process.version });
startPublicServer();
setTimeout(() => void recordBootSuite(), 1500).unref();
for (const signal of ['SIGTERM', 'SIGINT']) process.once(signal, () => void gracefulShutdown(signal));

async function waitForChild() {
  const deadline = Date.now() + 20000;
  while (Date.now() < deadline) {
    try { const r = await internalGet('/healthz'); if (r.status === 200) return; } catch {}
    await sleep(250);
  }
  throw new Error('child did not become healthy within 20s');
}

async function initializeDatabase() {
  if (!DATABASE_URL) return;
  pool = new Pool({ connectionString: DATABASE_URL, max: 3, connectionTimeoutMillis: 5000 });
  await pool.query(`CREATE TABLE IF NOT EXISTS gauntlet_v13_lifecycle (
    id BIGSERIAL PRIMARY KEY, event_type TEXT NOT NULL, run_id TEXT NOT NULL, release_id TEXT,
    instance_id TEXT NOT NULL, metadata JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS gauntlet_v13_test_runs (
    id BIGSERIAL PRIMARY KEY, test_id TEXT NOT NULL, run_id TEXT NOT NULL, release_id TEXT,
    instance_id TEXT NOT NULL, verdict TEXT NOT NULL, expected JSONB NOT NULL DEFAULT '{}'::jsonb,
    actual JSONB NOT NULL DEFAULT '{}'::jsonb, evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )`);
  await pool.query(`CREATE INDEX IF NOT EXISTS gauntlet_v13_test_runs_idx ON gauntlet_v13_test_runs(test_id, created_at DESC)`);
}

function startPublicServer() {
  publicServer = http.createServer(async (req, res) => {
    requests++;
    securityHeaders(res);
    const requestId = randomUUID();
    res.setHeader('X-Gauntlet-Request-Id', requestId);
    try { await route(req, res, requestId); }
    catch (error) {
      log('request.failed', { requestId, path: req.url, message: error.message, level: 'error' });
      sendJson(res, 500, { ok: false, error: 'internal_error', requestId, runId: RUN_ID });
    }
  });

  wss = new WebSocketServer({ noServer: true });
  wss.on('connection', (ws) => {
    wsConnections++;
    ws.send(JSON.stringify({ type: 'hello', runId: RUN_ID, instanceId: INSTANCE_ID, releaseId: RELEASE.releaseId, at: now() }));
    ws.on('message', async (raw) => {
      const value = raw.toString().slice(0, 2048);
      ws.send(JSON.stringify({ type: 'echo', value, runId: RUN_ID, instanceId: INSTANCE_ID, releaseId: RELEASE.releaseId, at: now() }));
      await recordOnce('websocket-proxy', 'pass', { echo: true }, { echo: true }, { source: 'browser-via-public-proxy' }).catch(() => {});
    });
  });
  publicServer.on('upgrade', (req, socket, head) => {
    if (new URL(req.url, 'http://local').pathname !== '/ws') return socket.destroy();
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
  });
  publicServer.listen(PORT, HOST, () => log('v13.listening', { port: PORT, host: HOST, childPort: INTERNAL_PORT }));
}

async function route(req, res, requestId) {
  const url = new URL(req.url, 'http://local');
  if (req.method === 'GET' && url.pathname === '/') return sendHtml(res, page());
  if (req.method === 'GET' && url.pathname === '/v13/status') {
    const childStatus = await childJson('/api/status');
    if (forwardedProto(req) === 'https') await recordOnce('https-proxy', 'pass', { https: true }, { https: true }, { forwardedProto: 'https' });
    return sendJson(res, 200, await status(childStatus, req));
  }
  if (req.method === 'GET' && url.pathname === '/v13/evidence') return sendJson(res, 200, await evidence(req));
  if (req.method === 'GET' && url.pathname === '/v13/findings') return sendJson(res, 200, { ok: true, findings: await findings(), recentEvidence: await recentRuns(80) });
  if (req.method === 'GET' && url.pathname === '/v13/confidence') return sendJson(res, 200, { ok: true, findings: await findings(), recentRuns: await recentRuns(80) });
  if (req.method === 'GET' && url.pathname === '/readyz' && readinessActive()) {
    return sendJson(res, 503, { ok: false, readiness: 'forced-not-ready', liveness: true, correlationId: readinessOverride.correlationId, runId: RUN_ID });
  }
  if (req.method === 'GET' && url.pathname === '/v13/access') {
    const httpStatus = allowedStatus(Number(url.searchParams.get('status') || 200));
    const correlationId = String(url.searchParams.get('correlationId') || randomUUID()).slice(0, 120);
    remember({ correlationId, type: 'access-probe', status: httpStatus, at: now() });
    log('access.probe', { requestId, correlationId, status: httpStatus });
    await recordRun('access-logging', 'observation', { visibleInAccessLogs: true }, { emittedStatus: httpStatus }, { correlationId, requestId });
    return sendJson(res, httpStatus, { ok: httpStatus < 400, correlationId, status: httpStatus, runId: RUN_ID });
  }
  if (req.method === 'POST' && url.pathname === '/v13/log-marker') {
    if (!authorized(req)) return sendJson(res, 403, { ok: false, error: 'unauthorized' });
    const body = await readBody(req);
    const correlationId = randomUUID();
    const label = String(body.label || 'observability-check').slice(0, 120);
    const marker = { at: now(), correlationId, label, runId: RUN_ID, instanceId: INSTANCE_ID, releaseId: RELEASE.releaseId };
    console.log(JSON.stringify({ level: 'info', event: 'gauntlet.stdout_marker', ...marker }));
    console.error(JSON.stringify({ level: 'error', event: 'gauntlet.stderr_marker', ...marker }));
    await recordRun('runtime-log-capture', 'observation', { stdout: 'visible', stderr: 'visible' }, { emitted: true }, { correlationId, label });
    remember({ correlationId, type: 'log-marker', label, at: now() });
    return sendJson(res, 201, { ok: true, correlationId, label, runId: RUN_ID });
  }
  if (req.method === 'POST' && url.pathname === '/v13/readiness') {
    if (!authorized(req)) return sendJson(res, 403, { ok: false, error: 'unauthorized' });
    const body = await readBody(req);
    if (body.action === 'stop') {
      readinessOverride = { active: false, until: null, correlationId: null };
      return sendJson(res, 200, { ok: true, readinessOverride });
    }
    const seconds = Math.min(300, Math.max(15, Number(body.seconds || 60)));
    const correlationId = randomUUID();
    readinessOverride = { active: true, until: new Date(Date.now() + seconds * 1000).toISOString(), correlationId };
    await sleep(80);
    const health = await internalPublicGet('/healthz');
    const ready = await internalPublicGet('/readyz');
    const verdict = health.status === 200 && ready.status === 503 ? 'pass' : 'fail';
    await recordRun('readiness-liveness', verdict, { health: 200, ready: 503 }, { health: health.status, ready: ready.status }, { correlationId, seconds, scope: 'endpoint-separation-only' });
    remember({ correlationId, type: 'readiness', health: health.status, ready: ready.status, at: now() });
    return sendJson(res, 201, { ok: true, correlationId, seconds, health: health.status, ready: ready.status, verdict });
  }
  if (req.method === 'POST' && url.pathname === '/v13/observation') {
    const body = await readBody(req);
    const testKey = normalizeTestKey(body.testKey || body.testId);
    const verdict = normalizeVerdict(body.verdict);
    if (!testKey || !verdict) return sendJson(res, 400, { ok: false, error: 'invalid_observation' });
    const row = await recordRun(testKey, verdict, body.expected || {}, body.actual || {}, body.evidence || {});
    return sendJson(res, 201, { ok: true, row });
  }
  return proxyToChild(req, res);
}

async function recordBootSuite() {
  const s = await childJson('/api/status');
  const checks = [
    ['runtime', 'pass', { reachable: true }, { reachable: true, node: s.instance?.node }],
    ['dynamic-port', process.env.PORT ? 'pass' : 'fail', { platformPort: true }, { platformPort: Boolean(process.env.PORT) }],
    ['environment-injection', s.environment?.sentinelPresent ? 'pass' : 'fail', { sentinel: true }, { sentinel: Boolean(s.environment?.sentinelPresent) }],
    ['writable-storage', s.persistence?.storage?.ok ? 'pass' : 'fail', { writable: true }, { writable: Boolean(s.persistence?.storage?.ok), detail: s.persistence?.storage?.detail }],
    ['storage-persistence', Number(s.persistence?.storage?.continuityBoots || 0) > 1 ? 'pass' : 'pending', { continuityBoots: '>=2' }, { identity: s.persistence?.storage?.identity, continuityBoots: s.persistence?.storage?.continuityBoots }],
    ['database-connection', s.database?.ok ? 'pass' : 'fail', { connected: true }, { connected: Boolean(s.database?.ok), identity: s.database?.identity }],
    ['database-persistence', Number(s.database?.bootCount || 0) > 1 ? 'pass' : 'pending', { bootCount: '>=2' }, { bootCount: s.database?.bootCount, identity: s.database?.identity, rows: s.database?.probeRows, endpointFingerprint: s.database?.endpointFingerprint }],
    ['restart-recovery', Number(s.database?.bootCount || 0) > 1 ? 'pass' : 'pending', { observedBoots: '>=2' }, { observedBoots: s.database?.bootCount }],
    ['graceful-shutdown', await gracefulCount() > 0 ? 'pass' : 'pending', { gracefulShutdowns: '>=1' }, { shutdownSignals: await signalCount(), gracefulShutdowns: await gracefulCount() }]
  ];
  for (const [key, verdict, expected, actual] of checks) await recordOnce(key, verdict, expected, actual, { source: 'automatic-boot-suite' });
  log('assurance.boot_suite_recorded', { count: checks.length });
}

async function status(childStatus, req) {
  return {
    ok: true,
    service: 'Infrastry Gauntlet', version: VERSION, release: RELEASE,
    run: { runId: RUN_ID, instanceId: INSTANCE_ID, host: hostname(), pid: process.pid, startedAt: STARTED_AT },
    child: childStatus,
    proxy: { forwardedProto: forwardedProto(req), httpsObserved: forwardedProto(req) === 'https' },
    readinessExperiment: readinessOverride,
    observability: { requests, websocketConnections: wsConnections, activeWebSockets: wss?.clients?.size || 0, correlations: correlations.slice(-12) },
    lifecycle: { shutdownSignals: await signalCount(), gracefulShutdowns: await gracefulCount() },
    findings: await findings()
  };
}

async function evidence(req) {
  const childStatus = await childJson('/api/status');
  return {
    schemaVersion: 2,
    generatedAt: now(),
    service: 'Infrastry Gauntlet',
    version: VERSION,
    release: RELEASE,
    run: { runId: RUN_ID, instanceId: INSTANCE_ID, startedAt: STARTED_AT },
    status: await status(childStatus, req),
    findings: await findings(),
    recentEvidence: await recentRuns(120),
    methodology: {
      confirmationThreshold: 3,
      confirmedPass: '3+ PASS and 0 FAIL',
      confirmedDefect: '3+ FAIL and 0 PASS',
      intermittent: 'at least 1 PASS and 1 FAIL',
      rule: 'Negative findings remain observations until reproduced under controlled conditions.'
    }
  };
}

async function recordOnce(testKey, verdict, expected, actual, evidenceData) {
  if (!pool) return null;
  const existing = await pool.query(`SELECT id FROM gauntlet_v13_test_runs WHERE test_id=$1 AND run_id=$2 LIMIT 1`, [testKey, RUN_ID]);
  if (existing.rowCount) return existing.rows[0];
  return recordRun(testKey, verdict, expected, actual, evidenceData);
}

async function recordRun(testKey, verdict, expected = {}, actual = {}, evidenceData = {}) {
  const key = canonicalKey(testKey);
  const row = { testKey: key, verdict, runId: RUN_ID, releaseId: RELEASE.releaseId, instanceId: INSTANCE_ID, expected, actual, evidence: evidenceData };
  if (!pool) return row;
  const r = await pool.query(`INSERT INTO gauntlet_v13_test_runs(test_id,run_id,release_id,instance_id,verdict,expected,actual,evidence)
    VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8::jsonb) RETURNING *`,
    [key, RUN_ID, RELEASE.releaseId, INSTANCE_ID, verdict, JSON.stringify(expected), JSON.stringify(actual), JSON.stringify(evidenceData)]);
  return r.rows[0];
}

async function findings() {
  if (!pool) return [];
  const r = await pool.query(`SELECT test_id,run_id,release_id,instance_id,verdict,expected,actual,evidence,created_at FROM gauntlet_v13_test_runs ORDER BY id ASC`);
  const groups = new Map();
  for (const row of r.rows) {
    const key = canonicalKey(row.test_id);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }
  const output = [];
  for (const test of TEST_CATALOG) {
    const rows = groups.get(test.key) || [];
    const pass = rows.filter((x) => x.verdict === 'pass').length;
    const fail = rows.filter((x) => x.verdict === 'fail').length;
    const warn = rows.filter((x) => x.verdict === 'warn').length;
    const pending = rows.filter((x) => x.verdict === 'pending').length;
    const observations = rows.filter((x) => x.verdict === 'observation').length;
    const concluded = pass + fail;
    const latest = rows.at(-1) || null;
    output.push({
      key: test.key,
      name: test.name,
      category: test.category,
      question: test.question,
      confidence: confidenceLabel(pass, fail),
      summary: confidenceSummaryText(pass, fail, pending, observations),
      runs: { total: rows.length, concluded, pass, fail, warn, pending, observations },
      coverage: {
        instances: new Set(rows.map((x) => x.instance_id)).size,
        releases: new Set(rows.map((x) => x.release_id)).size
      },
      firstObserved: rows[0]?.created_at || null,
      lastObserved: latest?.created_at || null,
      latestEvidence: latest ? { verdict: latest.verdict, expected: latest.expected, actual: latest.actual, evidence: latest.evidence, runId: latest.run_id, instanceId: latest.instance_id, releaseId: latest.release_id } : null
    });
  }
  return output;
}

function confidenceLabel(pass, fail) {
  if (pass >= 3 && fail === 0) return 'Confirmed pass';
  if (fail >= 3 && pass === 0) return 'Confirmed defect';
  if (pass > 0 && fail > 0) return 'Intermittent';
  if (pass === 2 && fail === 0) return 'Likely pass';
  if (fail === 2 && pass === 0) return 'Likely defect';
  if (pass + fail === 1) return 'Single observation';
  return 'Insufficient evidence';
}

function confidenceSummaryText(pass, fail, pending, observations) {
  const concluded = pass + fail;
  if (pass >= 3 && fail === 0) return `${pass}/${concluded} concluded runs passed`;
  if (fail >= 3 && pass === 0) return `${fail}/${concluded} concluded runs failed`;
  if (pass && fail) return `${pass} passed, ${fail} failed across concluded runs`;
  if (pass === 2) return '2 controlled passes; one more clean pass confirms the finding';
  if (fail === 2) return '2 controlled failures; one more matching failure confirms the finding';
  if (pass === 1) return '1 controlled pass; more reproductions required';
  if (fail === 1) return '1 controlled failure; more reproductions required';
  if (pending) return `${pending} pending observation${pending === 1 ? '' : 's'}; no conclusion yet`;
  if (observations) return `${observations} correlated observation${observations === 1 ? '' : 's'} awaiting platform verification`;
  return 'Not tested yet';
}

async function recentRuns(limit) {
  if (!pool) return [];
  const r = await pool.query(`SELECT id,test_id,run_id,release_id,instance_id,verdict,expected,actual,evidence,created_at FROM gauntlet_v13_test_runs ORDER BY id DESC LIMIT $1`, [Math.min(200, limit)]);
  return r.rows.map((row) => ({
    id: row.id,
    testKey: canonicalKey(row.test_id),
    finding: TESTS.get(canonicalKey(row.test_id))?.name || canonicalKey(row.test_id),
    runId: row.run_id,
    releaseId: row.release_id,
    instanceId: row.instance_id,
    verdict: row.verdict,
    expected: row.expected,
    actual: row.actual,
    evidence: row.evidence,
    createdAt: row.created_at
  }));
}

async function lifecycle(eventType, metadata = {}) {
  if (!pool) return;
  await pool.query(`INSERT INTO gauntlet_v13_lifecycle(event_type,run_id,release_id,instance_id,metadata) VALUES($1,$2,$3,$4,$5::jsonb)`, [eventType, RUN_ID, RELEASE.releaseId, INSTANCE_ID, JSON.stringify(metadata)]);
}
async function gracefulCount() { if (!pool) return 0; const r = await pool.query(`SELECT COUNT(*)::int n FROM gauntlet_v13_lifecycle WHERE event_type='graceful_shutdown'`); return +r.rows[0].n; }
async function signalCount() { if (!pool) return 0; const r = await pool.query(`SELECT COUNT(*)::int n FROM gauntlet_v13_lifecycle WHERE event_type='shutdown_signal_received'`); return +r.rows[0].n; }

async function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  const began = Date.now();
  log('shutdown.signal_received', { signal });
  await lifecycle('shutdown_signal_received', { signal, at: now() }).catch(() => {});
  try { child?.kill('SIGTERM'); } catch {}
  await waitChildExit(6000);
  if (wss) for (const c of wss.clients) c.close(1001, 'shutdown');
  await closeServer(5000);
  const durationMs = Date.now() - began;
  await lifecycle('graceful_shutdown', { signal, durationMs, at: now() }).catch(() => {});
  log('shutdown.graceful_complete', { signal, durationMs });
  if (pool) await pool.end().catch(() => {});
  process.exit(0);
}

function waitChildExit(ms) { return new Promise((resolve) => { if (!child || child.exitCode !== null) return resolve(); const t = setTimeout(resolve, ms); t.unref(); child.once('exit', () => { clearTimeout(t); resolve(); }); }); }
function closeServer(ms) { return new Promise((resolve) => { if (!publicServer) return resolve(); const t = setTimeout(() => { try { publicServer.closeAllConnections?.(); } catch {} resolve(); }, ms); t.unref(); publicServer.close(() => { clearTimeout(t); resolve(); }); }); }
function readinessActive() { if (!readinessOverride.active) return false; if (Date.now() >= Date.parse(readinessOverride.until)) { readinessOverride = { active: false, until: null, correlationId: null }; return false; } return true; }
function authorized(req) { if (!ADMIN_KEY) return false; const a = Buffer.from(String(req.headers['x-gauntlet-key'] || '')); const b = Buffer.from(ADMIN_KEY); return a.length === b.length && timingSafeEqual(a, b); }
function normalizeTestKey(v) { const key = canonicalKey(String(v || '').trim()); return TESTS.has(key) ? key : null; }
function canonicalKey(value) { const raw = String(value || '').trim(); return LEGACY_ALIASES.get(raw.toUpperCase()) || raw.toLowerCase(); }
function normalizeVerdict(v) { const s = String(v || '').toLowerCase(); return ['pass','fail','warn','pending','observation'].includes(s) ? s : null; }
function allowedStatus(v) { return [200,201,204,400,401,403,404,409,418,429,500,503].includes(v) ? v : 200; }
function remember(x) { correlations.push(x); correlations = correlations.slice(-30); }
function forwardedProto(req) { return String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim(); }
function now() { return new Date().toISOString(); }
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
function internalGet(path) { return new Promise((resolve, reject) => { const q = http.get({ host:'127.0.0.1', port:INTERNAL_PORT, path, timeout:1500 }, (r) => { r.resume(); r.on('end', () => resolve({ status:r.statusCode })); }); q.on('timeout', () => q.destroy(new Error('timeout'))); q.on('error', reject); }); }
function internalPublicGet(path) { return new Promise((resolve, reject) => { const q = http.get({ host:'127.0.0.1', port:PORT, path, timeout:1500 }, (r) => { r.resume(); r.on('end', () => resolve({ status:r.statusCode })); }); q.on('timeout', () => q.destroy(new Error('timeout'))); q.on('error', reject); }); }
async function childJson(path) { const r = await childRequest(path); return JSON.parse(r.body); }
function childRequest(path) { return new Promise((resolve, reject) => { const q = http.get({ host:'127.0.0.1', port:INTERNAL_PORT, path, timeout:2500 }, (r) => { const chunks=[]; r.on('data',(c)=>chunks.push(c)); r.on('end',()=>resolve({status:r.statusCode,headers:r.headers,body:Buffer.concat(chunks).toString('utf8')})); }); q.on('timeout',()=>q.destroy(new Error('timeout'))); q.on('error',reject); }); }
function proxyToChild(req, res) { const options = { host:'127.0.0.1', port:INTERNAL_PORT, method:req.method, path:req.url, headers:{...req.headers, host:`127.0.0.1:${INTERNAL_PORT}`} }; const p=http.request(options,(r)=>{res.statusCode=r.statusCode; for(const [k,v] of Object.entries(r.headers)) if(v!==undefined && !['content-security-policy','content-length'].includes(k)) res.setHeader(k,v); r.pipe(res);}); p.on('error',(e)=>sendJson(res,502,{ok:false,error:'child_proxy_failed',detail:e.message})); req.pipe(p); }
async function readBody(req) { const chunks=[]; let n=0; for await(const c of req){n+=c.length;if(n>65536)throw new Error('body_too_large');chunks.push(c);} return chunks.length?JSON.parse(Buffer.concat(chunks).toString('utf8')):{}; }
function securityHeaders(res) { res.setHeader('X-Content-Type-Options','nosniff'); res.setHeader('Referrer-Policy','no-referrer'); res.setHeader('Content-Security-Policy',"default-src 'self'; connect-src 'self' ws: wss:; style-src 'unsafe-inline'; script-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'"); }
function sendJson(res,code,v){res.statusCode=code;res.setHeader('Content-Type','application/json; charset=utf-8');res.setHeader('Cache-Control','no-store');res.end(JSON.stringify(v));}
function sendHtml(res,v){res.statusCode=200;res.setHeader('Content-Type','text/html; charset=utf-8');res.setHeader('Cache-Control','no-store');res.end(v);}
function log(event,data={}){console.log(JSON.stringify({at:now(),level:data.level||'info',event,service:'infrastry-gauntlet-v13',version:VERSION,runId:RUN_ID,instanceId:INSTANCE_ID,releaseId:RELEASE.releaseId,...data}));}

function page() { return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Infrastry Gauntlet</title><style>${css()}</style></head><body><main>
<p class="eyebrow">DEEDZ LABS / PLATFORM ASSURANCE</p><h1>Infrastry <span>Gauntlet</span></h1><p class="lede">Clean, repeatable platform testing with evidence-backed findings. Tests keep one clear name; run counts and confidence are tracked separately.</p>
<div class="actions"><button onclick="refreshAll()">Refresh</button><button onclick="exportEvidence()">Export evidence</button></div><div id="state" class="box">Loading…</div>
<h2>Evidence-backed findings</h2><div id="summary" class="summary"></div><div id="findings"></div>
<details><summary>Run details</summary><div id="identity" class="grid details"></div></details>
<div class="split"><section><h2>Runtime log capture</h2><input id="key" type="password" placeholder="GAUNTLET_ADMIN_KEY"><input id="label" value="observability-check"><button onclick="logMarker()">Emit correlated stdout + stderr</button><pre id="logOut">Not emitted.</pre></section>
<section><h2>Readiness vs liveness</h2><p>Temporarily hold <code>/healthz</code> at 200 while forcing <code>/readyz</code> to 503.</p><button onclick="readiness('start')">Start 60s test</button><button onclick="readiness('stop')">Stop</button><pre id="readyOut">Not running.</pre></section></div>
<div class="split"><section><h2>Access logging</h2><button onclick="probe(200)">Send 200</button><button onclick="probe(404)">Send 404</button><button onclick="probe(500)">Send 500</button><pre id="accessOut">No probes.</pre></section>
<section><h2>WebSocket proxy</h2><p id="wsState">connecting…</p><input id="wsMsg" value="goose-check"><button onclick="sendWs()">Send echo</button><pre id="wsOut">Waiting…</pre></section></div>
<h2>Recent evidence</h2><div id="recent"></div><footer>Evidence first · 3 matching controlled reproductions required for a confirmed conclusion</footer></main><script>${clientJs()}</script></body></html>`; }

function clientJs(){ return `let WS;const $=id=>document.getElementById(id);const esc=v=>String(v??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));async function j(p,o={}){const r=await fetch(p,{cache:'no-store',...o,headers:{'Content-Type':'application/json',...(o.headers||{})}});const t=await r.text();let b={};try{b=t?JSON.parse(t):{}}catch{b={raw:t}}if(!r.ok)throw Object.assign(new Error(r.status+' '+p),{body:b});return b}function cls(x){return String(x||'').toLowerCase().replace(/ /g,'-')}function statusWord(f){if(f.confidence==='Confirmed pass')return 'PASS';if(f.confidence==='Confirmed defect')return 'FAIL';if(f.confidence==='Intermittent')return 'MIXED';if(f.confidence==='Likely pass')return 'PASS?';if(f.confidence==='Likely defect')return 'FAIL?';return 'EVIDENCE'}async function refreshAll(){try{const [s,r]=await Promise.all([j('/v13/status'),j('/v13/findings')]);$('state').textContent='v'+s.version+' · '+s.release.releaseId+' · healthy runtime · '+s.run.startedAt;$('identity').innerHTML=[['Run',s.run.runId],['Supervisor',s.run.instanceId],['Child',s.child.instance.id],['Storage',s.child.persistence.storage.identity||'none'],['Storage boots',s.child.persistence.storage.continuityBoots],['Database',s.child.database.identity||'none'],['Database boots',s.child.database.bootCount],['Graceful shutdowns',s.lifecycle.gracefulShutdowns]].map(x=>'<div class="card"><b>'+esc(x[0])+'</b><small>'+esc(x[1])+'</small></div>').join('');const fs=r.findings||[];const counts={confirmed:fs.filter(x=>x.confidence==='Confirmed pass').length,defects:fs.filter(x=>x.confidence==='Confirmed defect').length,mixed:fs.filter(x=>x.confidence==='Intermittent').length,open:fs.filter(x=>!['Confirmed pass','Confirmed defect','Intermittent'].includes(x.confidence)).length};$('summary').innerHTML=[['Confirmed passes',counts.confirmed],['Confirmed defects',counts.defects],['Intermittent',counts.mixed],['Needs evidence',counts.open]].map(x=>'<div class="card"><b>'+x[0]+'</b><em>'+x[1]+'</em></div>').join('');$('findings').innerHTML=fs.map(f=>'<div class="finding"><div><small class="category">'+esc(f.category)+'</small><h3>'+esc(f.name)+'</h3><p>'+esc(f.question)+'</p><small>'+esc(f.summary)+' · '+f.coverage.instances+' instance'+(f.coverage.instances===1?'':'s')+'</small></div><div class="verdict"><b class="'+cls(f.confidence)+'">'+statusWord(f)+'</b><span>'+esc(f.confidence)+'</span></div></div>').join('');$('recent').innerHTML=(r.recentEvidence||[]).slice(0,20).map(x=>'<div class="evidence"><b>'+esc(x.finding)+'</b><span class="'+esc(x.verdict)+'">'+esc(x.verdict)+'</span><small>'+esc(new Date(x.createdAt).toLocaleString())+'</small></div>').join('')||'<div class="box">No evidence recorded yet.</div>'}catch(e){$('state').textContent='Error: '+e.message}}function h(){return {'x-gauntlet-key':$('key').value||''}}async function logMarker(){try{$('logOut').textContent=JSON.stringify(await j('/v13/log-marker',{method:'POST',headers:h(),body:JSON.stringify({label:$('label').value})}),null,2);refreshAll()}catch(e){$('logOut').textContent=JSON.stringify(e.body||{error:e.message},null,2)}}async function readiness(a){try{$('readyOut').textContent=JSON.stringify(await j('/v13/readiness',{method:'POST',headers:h(),body:JSON.stringify({action:a,seconds:60})}),null,2);refreshAll()}catch(e){$('readyOut').textContent=JSON.stringify(e.body||{error:e.message},null,2)}}async function probe(s){const id=crypto.randomUUID();const r=await fetch('/v13/access?status='+s+'&correlationId='+id,{cache:'no-store'});const b=await r.json();$('accessOut').textContent=JSON.stringify({correlationId:id,observedStatus:r.status,body:b},null,2);refreshAll()}function connect(){const p=location.protocol==='https:'?'wss:':'ws:';WS=new WebSocket(p+'//'+location.host+'/ws');WS.onopen=()=>$('wsState').textContent='connected';WS.onclose=()=>{$('wsState').textContent='disconnected';setTimeout(connect,2500)};WS.onmessage=e=>$('wsOut').textContent=e.data}function sendWs(){if(WS?.readyState===1)WS.send($('wsMsg').value)}async function exportEvidence(){const e=await j('/v13/evidence');const b=new Blob([JSON.stringify(e,null,2)],{type:'application/json'});const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='infrastry-gauntlet-evidence-'+Date.now()+'.json';a.click();URL.revokeObjectURL(a.href)}connect();refreshAll();setInterval(refreshAll,15000);`; }

function css(){return `:root{color-scheme:dark;--p:#111722;--line:#283345;--t:#f4f7fb;--m:#9aa7b8;--a:#9dfc61;--c:#6ae5ff;--w:#ffd166;--b:#ff6b7a}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 20% 0%,#102632,#090b10 35%);color:var(--t);font-family:system-ui,sans-serif}main{width:min(1180px,calc(100% - 28px));margin:auto;padding:44px 0}.eyebrow{color:var(--c);font-size:12px;letter-spacing:.15em;font-weight:800}h1{font-size:clamp(3rem,8vw,6.5rem);line-height:.9;letter-spacing:-.06em;margin:10px 0}h1 span{color:var(--a)}h2{margin-top:30px}.lede{color:var(--m);max-width:900px;font-size:18px;line-height:1.6}.actions{display:flex;gap:8px;margin:24px 0;flex-wrap:wrap}.box,section,.card,.finding,.evidence{background:var(--p);border:1px solid var(--line);border-radius:16px;padding:16px}.summary,.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.summary em{display:block;font-size:2rem;font-style:normal;margin-top:5px}.card b,.card small{display:block}.card small,.finding small{color:var(--m);word-break:break-word}.finding{display:flex;justify-content:space-between;gap:20px;margin:8px 0;align-items:center}.finding h3{margin:4px 0;font-size:1.15rem}.finding p{margin:4px 0 8px;color:var(--m)}.category{color:var(--c)!important;text-transform:uppercase;letter-spacing:.08em}.verdict{text-align:right;min-width:150px}.verdict b,.verdict span{display:block}.confirmed-pass,.likely-pass{color:var(--a)}.confirmed-defect,.likely-defect{color:var(--b)}.intermittent{color:#ff9d66}.single-observation,.insufficient-evidence{color:var(--w)}.split{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin:14px 0}input,button{font:inherit;border-radius:10px;border:1px solid var(--line);background:#111722;color:var(--t);padding:10px 12px}input{width:100%;margin:5px 0 10px}button{cursor:pointer;font-weight:750}.evidence{display:grid;grid-template-columns:1fr auto auto;gap:14px;margin:6px 0;align-items:center}.evidence small{color:var(--m)}.pass{color:var(--a)}.fail{color:var(--b)}.pending,.observation{color:var(--w)}details{margin-top:18px}summary{cursor:pointer;color:var(--m);padding:8px 0}.details{margin-top:10px}pre{white-space:pre-wrap;word-break:break-word;background:#080b10;padding:12px;border-radius:10px;color:#bcc8d6;max-height:360px;overflow:auto}footer{color:var(--m);padding:30px 0}@media(max-width:850px){.summary,.grid{grid-template-columns:1fr 1fr}.split{grid-template-columns:1fr}.finding{align-items:flex-start}.verdict{min-width:110px}.evidence{grid-template-columns:1fr auto}}@media(max-width:500px){.summary,.grid{grid-template-columns:1fr}.finding{display:block}.verdict{text-align:left;margin-top:10px}.evidence{display:block}.evidence span,.evidence small{display:block;margin-top:4px}}`;}
