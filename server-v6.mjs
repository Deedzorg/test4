import http from 'node:http';
import { spawn } from 'node:child_process';
import { randomUUID, timingSafeEqual } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { hostname } from 'node:os';
import pg from 'pg';
import { WebSocketServer } from 'ws';

const { Pool } = pg;
const VERSION = '1.3.2';
const PORT = Number(process.env.PORT || 3000);
const INTERNAL_PORT = PORT + 137;
const HOST = process.env.HOST || '0.0.0.0';
const RUN_ID = randomUUID();
const INSTANCE_ID = randomUUID();
const STARTED_AT = new Date().toISOString();
const ADMIN_KEY = process.env.GAUNTLET_ADMIN_KEY || '';
const DATABASE_URL = process.env.DATABASE_URL || '';
const RELEASE = { releaseId: 'v1.3-final-assurance', experiment: 'reproducibility-observability' };
const CATALOG = JSON.parse(await readFile(new URL('./gauntlet-tests.json', import.meta.url), 'utf8')).tests;
const TESTS = new Map(CATALOG.map((test) => [test.key, test]));
const CONFIRMATION_TARGET = 3;

const LEGACY_ALIASES = new Map(Object.entries({
  'CORE-RUNTIME-001': 'runtime', 'NET-DYNAMIC-PORT-001': 'dynamic-port',
  'ENV-SENTINEL-001': 'environment-injection', 'STORAGE-WRITE-001': 'writable-storage',
  'STORAGE-CONTINUITY-001': 'storage-persistence', 'DB-CONNECT-001': 'database-connection',
  'DB-CONTINUITY-001': 'database-persistence', 'RESTART-RECOVERY-001': 'process-replacement',
  'SHUTDOWN-GRACEFUL-001': 'graceful-shutdown', 'PROXY-HTTPS-001': 'https-proxy',
  'WS-PROXY-ECHO-001': 'websocket-proxy', 'READINESS-SEPARATION-001': 'readiness-endpoints',
  'OBS-LOG-MARKER-001': 'runtime-log-capture', 'restart-recovery': 'process-replacement',
  'readiness-liveness': 'readiness-endpoints'
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
setTimeout(() => void recordBootSuite(), 1600).unref();
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
    instance_id TEXT NOT NULL, metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
  publicServer.listen(PORT, HOST, () => log('assurance.listening', { port: PORT, host: HOST, childPort: INTERNAL_PORT }));
}

async function route(req, res, requestId) {
  const url = new URL(req.url, 'http://local');
  if (req.method === 'GET' && url.pathname === '/') return sendHtml(res, page());
  if (req.method === 'GET' && url.pathname === '/v13/status') {
    const childStatus = await childJson('/api/status');
    if (forwardedProto(req) === 'https') await recordOnce('https-proxy', 'pass', { https: true }, { https: true }, { forwardedProto: 'https' });
    return sendJson(res, 200, await status(childStatus, req));
  }
  if (req.method === 'GET' && url.pathname === '/v13/findings') {
    const f = await findings();
    return sendJson(res, 200, { ok: true, summary: findingCounts(f), findings: f, recentEvidence: await recentRuns(100) });
  }
  if (req.method === 'GET' && url.pathname === '/v13/evidence') return sendJson(res, 200, await evidence(req));
  if (req.method === 'GET' && url.pathname === '/readyz' && readinessActive()) {
    return sendJson(res, 503, { ok: false, readiness: 'forced-not-ready', liveness: true, correlationId: readinessOverride.correlationId, runId: RUN_ID });
  }

  if (req.method === 'POST' && url.pathname === '/v13/log-marker') {
    if (!authorized(req)) return unauthorized(res);
    const body = await readBody(req);
    const correlationId = randomUUID();
    const label = String(body.label || 'observability-check').slice(0, 120);
    const marker = { at: now(), correlationId, label, runId: RUN_ID, instanceId: INSTANCE_ID, releaseId: RELEASE.releaseId };
    console.log(JSON.stringify({ level: 'info', event: 'gauntlet.stdout_marker', ...marker }));
    console.error(JSON.stringify({ level: 'error', event: 'gauntlet.stderr_marker', ...marker }));
    await recordRun('runtime-log-capture', 'observation', { stdout: 'visible', stderr: 'visible' }, { emitted: true }, { correlationId, label, requestId });
    remember({ correlationId, type: 'runtime-log-capture', label, at: now() });
    return sendJson(res, 201, { ok: true, finding: 'Runtime log capture', correlationId, label, next: 'Find this correlation ID in Infrastry Runtime logs, then verify the result.' });
  }

  if (req.method === 'POST' && url.pathname === '/v13/access') {
    if (!authorized(req)) return unauthorized(res);
    const body = await readBody(req);
    const httpStatus = allowedStatus(Number(body.status || 200));
    const correlationId = randomUUID();
    remember({ correlationId, type: 'access-logging', status: httpStatus, at: now() });
    log('access.probe', { requestId, correlationId, status: httpStatus });
    await recordRun('access-logging', 'observation', { visibleInAccessLogs: true, status: httpStatus }, { emittedStatus: httpStatus }, { correlationId, requestId });
    return sendJson(res, httpStatus, { ok: httpStatus < 400, correlationId, status: httpStatus, next: 'Find this correlation ID in Infrastry Access logs, then verify the result.' });
  }

  if (req.method === 'POST' && url.pathname === '/v13/readiness') {
    if (!authorized(req)) return unauthorized(res);
    const body = await readBody(req);
    if (body.action === 'stop') {
      readinessOverride = { active: false, until: null, correlationId: null };
      return sendJson(res, 200, { ok: true, readinessOverride });
    }
    const seconds = Math.min(300, Math.max(15, Number(body.seconds || 60)));
    const correlationId = randomUUID();
    readinessOverride = { active: true, until: new Date(Date.now() + seconds * 1000).toISOString(), correlationId };
    await sleep(100);
    const health = await internalPublicGet('/healthz');
    const ready = await internalPublicGet('/readyz');
    const endpointVerdict = health.status === 200 && ready.status === 503 ? 'pass' : 'fail';
    await recordRun('readiness-endpoints', endpointVerdict, { health: 200, ready: 503 }, { health: health.status, ready: ready.status }, { correlationId, seconds });
    await recordRun('readiness-platform', 'observation', { platformReaction: 'verify' }, { health: health.status, ready: ready.status }, { correlationId, seconds });
    remember({ correlationId, type: 'readiness-platform', health: health.status, ready: ready.status, at: now() });
    return sendJson(res, 201, { ok: true, correlationId, seconds, health: health.status, ready: ready.status, endpointVerdict, next: 'Inspect Infrastry Platform/Monitoring logs during this window, then verify the platform response.' });
  }

  if (req.method === 'POST' && url.pathname === '/v13/verify') {
    if (!authorized(req)) return unauthorized(res);
    const body = await readBody(req);
    const testKey = normalizeTestKey(body.testKey);
    const verdict = normalizeVerdict(body.verdict);
    const correlationId = String(body.correlationId || '').trim().slice(0, 120);
    if (!testKey || !['pass','fail','warn'].includes(verdict) || !correlationId) {
      return sendJson(res, 400, { ok: false, error: 'verification_requires_test_verdict_and_correlation' });
    }
    const row = await recordRun(testKey, verdict, body.expected || {}, body.actual || {}, {
      correlationId, source: 'human-verified-platform-evidence', note: String(body.note || '').slice(0, 500)
    });
    return sendJson(res, 201, { ok: true, finding: TESTS.get(testKey)?.name, verdict, correlationId, rowId: row?.id || null });
  }

  if (req.method === 'POST' && url.pathname === '/v13/observation') {
    if (!authorized(req)) return unauthorized(res);
    const body = await readBody(req);
    const testKey = normalizeTestKey(body.testKey || body.testId);
    const verdict = normalizeVerdict(body.verdict);
    if (!testKey || !verdict) return sendJson(res, 400, { ok: false, error: 'invalid_observation' });
    return sendJson(res, 201, { ok: true, row: await recordRun(testKey, verdict, body.expected || {}, body.actual || {}, body.evidence || {}) });
  }

  return proxyToChild(req, res);
}

async function recordBootSuite() {
  const s = await childJson('/api/status');
  const priorStorage = await previousConcludedActual('storage-persistence');
  const storageIdentity = s.persistence?.storage?.identity || null;
  let storageVerdict = 'pending';
  if (priorStorage?.identity && storageIdentity) storageVerdict = priorStorage.identity === storageIdentity ? 'pass' : 'fail';

  const priorDb = await previousConcludedActual('database-persistence');
  const dbNow = {
    identity: s.database?.identity || null,
    endpointFingerprint: s.database?.endpointFingerprint || null,
    rows: Number(s.database?.probeRows || 0),
    bootCount: Number(s.database?.bootCount || 0)
  };
  let dbVerdict = 'pending';
  if (priorDb?.identity && dbNow.identity) {
    dbVerdict = priorDb.identity === dbNow.identity &&
      (!priorDb.endpointFingerprint || priorDb.endpointFingerprint === dbNow.endpointFingerprint) &&
      dbNow.rows >= Number(priorDb.rows || 0) && dbNow.bootCount > Number(priorDb.bootCount || 0) ? 'pass' : 'fail';
  }

  const priorRuntime = await previousDistinctInstance('runtime');
  const checks = [
    ['runtime', 'pass', { reachable: true }, { reachable: true, node: s.instance?.node, childInstance: s.instance?.id }],
    ['dynamic-port', process.env.PORT ? 'pass' : 'fail', { platformPort: true }, { platformPort: Boolean(process.env.PORT) }],
    ['environment-injection', s.environment?.sentinelPresent ? 'pass' : 'fail', { sentinel: true }, { sentinel: Boolean(s.environment?.sentinelPresent) }],
    ['writable-storage', s.persistence?.storage?.ok ? 'pass' : 'fail', { writable: true }, { writable: Boolean(s.persistence?.storage?.ok), detail: s.persistence?.storage?.detail }],
    ['storage-persistence', storageVerdict, { compareAcrossReplacement: true }, { identity: storageIdentity, previousIdentity: priorStorage?.identity || null, continuityBoots: s.persistence?.storage?.continuityBoots }],
    ['database-connection', s.database?.ok ? 'pass' : 'fail', { connected: true }, { connected: Boolean(s.database?.ok), identity: dbNow.identity }],
    ['database-persistence', dbVerdict, { preserveIdentityAndData: true }, { ...dbNow, previous: priorDb || null }],
    ['process-replacement', priorRuntime ? 'pass' : 'pending', { independentInstances: '>=2' }, { currentInstance: INSTANCE_ID, previousInstance: priorRuntime?.instanceId || null }],
    ['graceful-shutdown', await gracefulCount() > 0 ? 'pass' : 'pending', { durableReceipt: '>=1' }, { shutdownSignals: await signalCount(), gracefulShutdowns: await gracefulCount() }]
  ];
  for (const [key, verdict, expected, actual] of checks) await recordOnce(key, verdict, expected, actual, { source: 'automatic-boot-suite' });
  log('assurance.boot_suite_recorded', { count: checks.length });
}

async function status(childStatus, req) {
  return {
    ok: true, service: 'Infrastry Gauntlet', version: VERSION, release: RELEASE,
    run: { runId: RUN_ID, instanceId: INSTANCE_ID, host: hostname(), pid: process.pid, startedAt: STARTED_AT },
    child: childStatus,
    proxy: { forwardedProto: forwardedProto(req), httpsObserved: forwardedProto(req) === 'https' },
    evidenceStore: { connected: Boolean(pool) },
    readinessExperiment: readinessOverride,
    observability: { requests, websocketConnections: wsConnections, activeWebSockets: wss?.clients?.size || 0, correlations: correlations.slice(-12) },
    lifecycle: { shutdownSignals: await signalCount(), gracefulShutdowns: await gracefulCount() }
  };
}

async function evidence(req) {
  const childStatus = await childJson('/api/status');
  const f = await findings();
  return {
    schemaVersion: 3,
    generatedAt: now(),
    service: 'Infrastry Gauntlet', version: VERSION, release: RELEASE,
    run: { runId: RUN_ID, instanceId: INSTANCE_ID, startedAt: STARTED_AT },
    summary: findingCounts(f),
    findings: f,
    runtimeStatus: await status(childStatus, req),
    recentEvidence: await recentRuns(160),
    methodology: {
      confirmationTarget: CONFIRMATION_TARGET,
      independence: 'Confidence counts independent trials, not raw database rows.',
      instanceTests: 'Deployment/lifecycle findings deduplicate by runtime instance.',
      correlationTests: 'Observability findings deduplicate by verified correlation ID.',
      neutralCharacteristics: 'Architectural characteristics such as application storage are reported as persistent/ephemeral, not automatically as defects.',
      negativeFindingRule: 'A platform defect is not called confirmed until at least 3 independent matching failures exist with no contradictory pass.',
      auditability: 'Raw expected, actual, timestamps, releases, runtime instances and correlation IDs remain in the evidence export.'
    }
  };
}

async function findings() {
  if (!pool) return CATALOG.map((test) => emptyFinding(test));
  const rows = await allRows();
  const groups = new Map();
  for (const row of rows) {
    const key = canonicalKey(row.test_id);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }

  return CATALOG.map((test) => buildFinding(test, groups.get(test.key) || []))
    .sort((a, b) => rankFinding(a) - rankFinding(b) || a.category.localeCompare(b.category) || a.name.localeCompare(b.name));
}

function buildFinding(test, rows) {
  if (!rows.length) return emptyFinding(test);
  const trialMap = new Map();
  for (const row of rows) {
    const trialKey = trialIdentity(test, row);
    if (!trialMap.has(trialKey)) trialMap.set(trialKey, []);
    trialMap.get(trialKey).push(row);
  }
  const trials = [...trialMap.entries()].map(([key, rs]) => summarizeTrial(key, rs));
  const pass = trials.filter((x) => x.verdict === 'pass').length;
  const fail = trials.filter((x) => x.verdict === 'fail').length;
  const mixed = trials.filter((x) => x.verdict === 'mixed').length;
  const pending = trials.filter((x) => x.verdict === 'pending').length;
  const observations = trials.filter((x) => x.verdict === 'observation').length;
  const warn = trials.filter((x) => x.verdict === 'warn').length;
  const result = resultLabel(test, { pass, fail, mixed, pending, observations, warn });
  const latest = rows.at(-1);
  const directionCount = Math.max(pass, fail);
  return {
    key: test.key, name: test.name, category: test.category, question: test.question,
    result: result.label, tone: result.tone,
    evidenceSummary: evidenceSummary(test, { pass, fail, mixed, pending, observations, warn, trials }),
    progress: { current: Math.min(CONFIRMATION_TARGET, directionCount), target: CONFIRMATION_TARGET, unit: test.confirmationUnit === 'correlation' ? 'verified correlations' : 'independent runtimes' },
    nextTest: nextTest(test, result.label, directionCount),
    counts: { independentTrials: trials.length, pass, fail, mixed, warn, pending, observations, rawRecords: rows.length },
    coverage: {
      instances: new Set(rows.map((x) => x.instance_id).filter(Boolean)).size,
      releases: new Set(rows.map((x) => x.release_id).filter(Boolean)).size,
      correlations: new Set(rows.map((x) => x.evidence?.correlationId).filter(Boolean)).size
    },
    firstObserved: rows[0]?.created_at || null,
    lastObserved: latest?.created_at || null,
    latestEvidence: latest ? compactEvidence(latest) : null,
    audit: trials.slice(-6).map((trial) => ({ trial: trial.key, verdict: trial.verdict, at: trial.at, releaseId: trial.releaseId, instanceId: trial.instanceId, correlationId: trial.correlationId }))
  };
}

function emptyFinding(test) {
  return {
    key: test.key, name: test.name, category: test.category, question: test.question,
    result: 'Not tested', tone: 'open', evidenceSummary: 'No controlled evidence has been recorded yet.',
    progress: { current: 0, target: CONFIRMATION_TARGET, unit: test.confirmationUnit === 'correlation' ? 'verified correlations' : 'independent runtimes' },
    nextTest: test.nextTest, counts: { independentTrials: 0, pass: 0, fail: 0, mixed: 0, warn: 0, pending: 0, observations: 0, rawRecords: 0 },
    coverage: { instances: 0, releases: 0, correlations: 0 }, firstObserved: null, lastObserved: null, latestEvidence: null, audit: []
  };
}

function trialIdentity(test, row) {
  if (test.confirmationUnit === 'correlation') return row.evidence?.correlationId || `unverified:${row.run_id}:${row.id}`;
  if (test.confirmationUnit === 'run') return row.run_id || `row:${row.id}`;
  return row.instance_id || row.run_id || `row:${row.id}`;
}

function summarizeTrial(key, rows) {
  const hasPass = rows.some((x) => x.verdict === 'pass');
  const hasFail = rows.some((x) => x.verdict === 'fail');
  let verdict = 'observation';
  if (hasPass && hasFail) verdict = 'mixed';
  else if (hasFail) verdict = 'fail';
  else if (hasPass) verdict = 'pass';
  else if (rows.some((x) => x.verdict === 'warn')) verdict = 'warn';
  else if (rows.some((x) => x.verdict === 'pending')) verdict = 'pending';
  const latest = rows.at(-1);
  return {
    key, verdict, at: latest?.created_at || null, releaseId: latest?.release_id || null,
    instanceId: latest?.instance_id || null, correlationId: latest?.evidence?.correlationId || null
  };
}

function resultLabel(test, c) {
  if (c.mixed > 0 || (c.pass > 0 && c.fail > 0)) return { label: 'Intermittent', tone: 'mixed' };
  if (test.mode === 'characteristic') {
    if (c.pass >= 3) return { label: 'Confirmed persistent', tone: 'neutral' };
    if (c.fail >= 3) return { label: 'Confirmed ephemeral', tone: 'neutral' };
    if (c.pass === 2) return { label: 'Likely persistent', tone: 'neutral' };
    if (c.fail === 2) return { label: 'Likely ephemeral', tone: 'neutral' };
  } else if (test.mode === 'behavior') {
    if (c.pass >= 3) return { label: `Confirmed ${test.positiveLabel || 'positive'}`, tone: 'neutral' };
    if (c.fail >= 3) return { label: `Confirmed ${test.negativeLabel || 'negative'}`, tone: 'neutral' };
    if (c.pass === 2) return { label: `Likely ${test.positiveLabel || 'positive'}`, tone: 'neutral' };
    if (c.fail === 2) return { label: `Likely ${test.negativeLabel || 'negative'}`, tone: 'neutral' };
  } else {
    if (c.pass >= 3) return { label: 'Confirmed pass', tone: 'good' };
    if (c.fail >= 3) return { label: 'Confirmed defect', tone: 'bad' };
    if (c.pass === 2) return { label: 'Likely pass', tone: 'good' };
    if (c.fail === 2) return { label: 'Likely concern', tone: 'bad' };
  }
  if (c.pass + c.fail > 0 || c.pending + c.observations + c.warn > 0) return { label: 'Needs evidence', tone: c.fail ? 'bad' : 'open' };
  return { label: 'Not tested', tone: 'open' };
}

function evidenceSummary(test, c) {
  const unit = test.confirmationUnit === 'correlation' ? 'verified correlation' : 'independent runtime';
  if (c.mixed) return `${c.mixed} ${unit}${c.mixed === 1 ? '' : 's'} contain contradictory evidence; review before concluding.`;
  if (test.mode === 'characteristic') {
    if (c.fail) return `${c.fail} ${unit}${c.fail === 1 ? '' : 's'} observed a changed storage identity; ${c.pass} observed continuity.`;
    if (c.pass) return `${c.pass} ${unit}${c.pass === 1 ? '' : 's'} observed the same storage identity across replacement.`;
  }
  if (c.pass || c.fail) return `${c.pass} ${unit}${c.pass === 1 ? '' : 's'} passed; ${c.fail} failed.`;
  if (c.observations) return `${c.observations} correlated observation${c.observations === 1 ? '' : 's'} recorded; platform verification is still required.`;
  if (c.pending) return `${c.pending} pending observation${c.pending === 1 ? '' : 's'} recorded; the comparison condition has not been met yet.`;
  return 'Evidence exists but is not yet conclusive.';
}

function nextTest(test, label, directionCount) {
  if (label.startsWith('Confirmed')) return 'Recheck after a meaningful platform or harness change; no immediate reproduction is required.';
  if (label.startsWith('Likely') && directionCount === 2) return `One more matching ${test.confirmationUnit === 'correlation' ? 'verified correlation' : 'fresh runtime'} will confirm this result.`;
  return test.nextTest;
}

function findingCounts(findingsList) {
  return {
    confirmed: findingsList.filter((f) => f.result.startsWith('Confirmed')).length,
    likely: findingsList.filter((f) => f.result.startsWith('Likely')).length,
    intermittent: findingsList.filter((f) => f.result === 'Intermittent').length,
    needsEvidence: findingsList.filter((f) => f.result === 'Needs evidence').length,
    notTested: findingsList.filter((f) => f.result === 'Not tested').length
  };
}

function rankFinding(f) {
  if (f.result.startsWith('Confirmed')) return 0;
  if (f.result.startsWith('Likely')) return 1;
  if (f.result === 'Intermittent') return 2;
  if (f.result === 'Needs evidence') return 3;
  return 4;
}

async function allRows() {
  const r = await pool.query(`SELECT id,test_id,run_id,release_id,instance_id,verdict,expected,actual,evidence,created_at FROM gauntlet_v13_test_runs ORDER BY id ASC`);
  return r.rows;
}

async function previousConcludedActual(testKey) {
  if (!pool) return null;
  const rows = await allRows();
  for (let i = rows.length - 1; i >= 0; i--) {
    const row = rows[i];
    if (canonicalKey(row.test_id) === testKey && row.instance_id !== INSTANCE_ID && ['pass','fail'].includes(row.verdict)) return row.actual || null;
  }
  return null;
}

async function previousDistinctInstance(testKey) {
  if (!pool) return null;
  const rows = await allRows();
  for (let i = rows.length - 1; i >= 0; i--) {
    const row = rows[i];
    if (canonicalKey(row.test_id) === testKey && row.instance_id && row.instance_id !== INSTANCE_ID) return { instanceId: row.instance_id, releaseId: row.release_id, at: row.created_at };
  }
  return null;
}

async function recentRuns(limit) {
  if (!pool) return [];
  const r = await pool.query(`SELECT id,test_id,run_id,release_id,instance_id,verdict,expected,actual,evidence,created_at FROM gauntlet_v13_test_runs ORDER BY id DESC LIMIT $1`, [Math.min(250, limit)]);
  return r.rows.map((row) => ({
    id: row.id, testKey: canonicalKey(row.test_id), finding: TESTS.get(canonicalKey(row.test_id))?.name || canonicalKey(row.test_id),
    runId: row.run_id, releaseId: row.release_id, instanceId: row.instance_id, verdict: row.verdict,
    expected: row.expected, actual: row.actual, evidence: row.evidence, createdAt: row.created_at
  }));
}

async function recordOnce(testKey, verdict, expected, actual, evidenceData) {
  if (!pool) return null;
  const key = canonicalKey(testKey);
  const existing = await pool.query(`SELECT id FROM gauntlet_v13_test_runs WHERE test_id=$1 AND run_id=$2 LIMIT 1`, [key, RUN_ID]);
  if (existing.rowCount) return existing.rows[0];
  return recordRun(key, verdict, expected, actual, evidenceData);
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

function compactEvidence(row) {
  return { verdict: row.verdict, expected: row.expected, actual: row.actual, evidence: row.evidence, runId: row.run_id, instanceId: row.instance_id, releaseId: row.release_id, at: row.created_at };
}
function canonicalKey(value) { const raw = String(value || '').trim(); return LEGACY_ALIASES.get(raw.toUpperCase()) || LEGACY_ALIASES.get(raw.toLowerCase()) || raw.toLowerCase(); }
function normalizeTestKey(v) { const key = canonicalKey(v); return TESTS.has(key) ? key : null; }
function normalizeVerdict(v) { const s = String(v || '').toLowerCase(); return ['pass','fail','warn','pending','observation'].includes(s) ? s : null; }
function authorized(req) { if (!ADMIN_KEY) return false; const a = Buffer.from(String(req.headers['x-gauntlet-key'] || '')); const b = Buffer.from(ADMIN_KEY); return a.length === b.length && timingSafeEqual(a, b); }
function unauthorized(res) { return sendJson(res, 403, { ok: false, error: 'authorized_experiment_control_required' }); }
function allowedStatus(v) { return [200,201,204,400,401,403,404,409,418,429,500,503].includes(v) ? v : 200; }
function readinessActive() { if (!readinessOverride.active) return false; if (Date.now() >= Date.parse(readinessOverride.until)) { readinessOverride = { active: false, until: null, correlationId: null }; return false; } return true; }
function remember(x) { correlations.push(x); correlations = correlations.slice(-40); }
function forwardedProto(req) { return String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim(); }
function now() { return new Date().toISOString(); }
function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }

function waitChildExit(ms) { return new Promise((resolve) => { if (!child || child.exitCode !== null) return resolve(); const t = setTimeout(resolve, ms); t.unref(); child.once('exit', () => { clearTimeout(t); resolve(); }); }); }
function closeServer(ms) { return new Promise((resolve) => { if (!publicServer) return resolve(); const t = setTimeout(() => { try { publicServer.closeAllConnections?.(); } catch {} resolve(); }, ms); t.unref(); publicServer.close(() => { clearTimeout(t); resolve(); }); }); }
function internalGet(path) { return new Promise((resolve, reject) => { const q = http.get({ host:'127.0.0.1', port:INTERNAL_PORT, path, timeout:1500 }, (r) => { r.resume(); r.on('end', () => resolve({ status:r.statusCode })); }); q.on('timeout', () => q.destroy(new Error('timeout'))); q.on('error', reject); }); }
function internalPublicGet(path) { return new Promise((resolve, reject) => { const q = http.get({ host:'127.0.0.1', port:PORT, path, timeout:1500 }, (r) => { r.resume(); r.on('end', () => resolve({ status:r.statusCode })); }); q.on('timeout', () => q.destroy(new Error('timeout'))); q.on('error', reject); }); }
async function childJson(path) { const r = await childRequest(path); return JSON.parse(r.body); }
function childRequest(path) { return new Promise((resolve, reject) => { const q = http.get({ host:'127.0.0.1', port:INTERNAL_PORT, path, timeout:2500 }, (r) => { const chunks=[]; r.on('data',(c)=>chunks.push(c)); r.on('end',()=>resolve({status:r.statusCode,headers:r.headers,body:Buffer.concat(chunks).toString('utf8')})); }); q.on('timeout',()=>q.destroy(new Error('timeout'))); q.on('error',reject); }); }
function proxyToChild(req, res) { const options = { host:'127.0.0.1', port:INTERNAL_PORT, method:req.method, path:req.url, headers:{...req.headers, host:`127.0.0.1:${INTERNAL_PORT}`} }; const p=http.request(options,(r)=>{res.statusCode=r.statusCode; for(const [k,v] of Object.entries(r.headers)) if(v!==undefined && !['content-security-policy','content-length'].includes(k)) res.setHeader(k,v); r.pipe(res);}); p.on('error',(e)=>sendJson(res,502,{ok:false,error:'child_proxy_failed',detail:e.message})); req.pipe(p); }
async function readBody(req) { const chunks=[]; let n=0; for await(const c of req){n+=c.length;if(n>65536)throw new Error('body_too_large');chunks.push(c);} return chunks.length?JSON.parse(Buffer.concat(chunks).toString('utf8')):{}; }
function securityHeaders(res) { res.setHeader('X-Content-Type-Options','nosniff'); res.setHeader('Referrer-Policy','no-referrer'); res.setHeader('Content-Security-Policy',"default-src 'self'; connect-src 'self' ws: wss:; style-src 'unsafe-inline'; script-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'"); }
function sendJson(res, code, value) { res.statusCode=code; res.setHeader('Content-Type','application/json; charset=utf-8'); res.setHeader('Cache-Control','no-store'); res.end(JSON.stringify(value)); }
function sendHtml(res, value) { res.statusCode=200; res.setHeader('Content-Type','text/html; charset=utf-8'); res.setHeader('Cache-Control','no-store'); res.end(value); }
function log(event, data={}) { console.log(JSON.stringify({ at:now(), level:data.level||'info', event, service:'infrastry-gauntlet', version:VERSION, runId:RUN_ID, instanceId:INSTANCE_ID, releaseId:RELEASE.releaseId, ...data })); }

function page() { return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Infrastry Gauntlet</title><style>${css()}</style></head><body><main>
<header><p class="eyebrow">DEEDZ LABS / PLATFORM ASSURANCE</p><h1>Infrastry <span>Gauntlet</span></h1><p class="lede">Evidence-backed platform testing. Each finding has one clear name, independent reproduction counts, raw audit evidence and an explicit next test.</p><div class="actions"><button onclick="refreshAll()">Refresh</button><button onclick="exportEvidence()">Export evidence</button></div><div id="state" class="state">Loading current runtime…</div></header>
<section class="report"><div class="section-title"><div><p class="eyebrow">ASSURANCE REPORT</p><h2>Evidence-backed findings</h2></div><small>3 independent matching trials required for confirmation</small></div><div id="summary" class="summary"></div><div id="findings"></div></section>
<details class="panel"><summary>Controlled experiments</summary><div class="panel-body"><p class="note">These controls write evidence. Use the configured Gauntlet admin key. Platform-facing results are not counted as pass/fail until you verify them against Infrastry logs or monitoring.</p><input id="key" type="password" placeholder="GAUNTLET_ADMIN_KEY"><div class="experiment-grid">
<section><h3>Runtime log capture</h3><input id="label" value="observability-check"><button onclick="logMarker()">Emit stdout + stderr marker</button><pre id="logOut">Not emitted.</pre></section>
<section><h3>Readiness vs liveness</h3><p>Hold <code>/healthz=200</code> while <code>/readyz=503</code> for 60 seconds.</p><button onclick="readiness('start')">Start 60s window</button> <button onclick="readiness('stop')">Stop</button><pre id="readyOut">Not running.</pre></section>
<section><h3>Access logging</h3><button onclick="probe(200)">Generate 200</button> <button onclick="probe(404)">Generate 404</button> <button onclick="probe(500)">Generate 500</button><pre id="accessOut">No probe.</pre></section>
<section><h3>Platform verification</h3><select id="verifyTest"><option value="runtime-log-capture">Runtime log capture</option><option value="access-logging">Access logging</option><option value="readiness-platform">Platform readiness response</option></select><input id="correlation" placeholder="Correlation ID"><input id="verifyNote" placeholder="Short evidence note"><button onclick="verify('pass')">Record verified pass</button> <button onclick="verify('fail')">Record verified concern</button><pre id="verifyOut">No verification recorded.</pre></section>
</div></div></details>
<details class="panel"><summary>Run and infrastructure details</summary><div id="identity" class="grid panel-body"></div></details>
<details class="panel"><summary>Recent audit trail</summary><div id="recent" class="panel-body"></div></details>
<footer>Evidence first · independent trials, not clicks · negative findings require repeatable proof</footer></main><script>${clientJs()}</script></body></html>`; }

function clientJs() { return `let WS;const $=id=>document.getElementById(id);const esc=v=>String(v??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));async function j(p,o={}){const r=await fetch(p,{cache:'no-store',...o,headers:{'Content-Type':'application/json',...(o.headers||{})}});const t=await r.text();let b={};try{b=t?JSON.parse(t):{}}catch{b={raw:t}}if(!r.ok)throw Object.assign(new Error(r.status+' '+p),{body:b});return b}function h(){return {'x-gauntlet-key':$('key')?.value||''}}function cls(v){return String(v||'').toLowerCase().replace(/[^a-z0-9]+/g,'-')}function short(v){const s=String(v||'');return s.length>18?s.slice(0,8)+'…'+s.slice(-6):s}function progress(f){const p=f.progress||{current:0,target:3};return '<div class="progress"><i style="width:'+Math.min(100,(p.current/p.target)*100)+'%"></i></div><small>'+p.current+' / '+p.target+' '+esc(p.unit)+'</small>'}function evidenceDetails(f){const e=f.latestEvidence;if(!e)return '<p>No raw evidence yet.</p>';return '<div class="egrid"><div><b>Latest verdict</b><small>'+esc(e.verdict)+'</small></div><div><b>Release</b><small>'+esc(e.releaseId||'unknown')+'</small></div><div><b>Instance</b><small>'+esc(short(e.instanceId))+'</small></div><div><b>Observed</b><small>'+esc(new Date(e.at).toLocaleString())+'</small></div></div><pre>'+esc(JSON.stringify({expected:e.expected,actual:e.actual,evidence:e.evidence},null,2))+'</pre>'}async function refreshAll(){try{const [s,r]=await Promise.all([j('/v13/status'),j('/v13/findings')]);$('state').textContent='v'+s.version+' · '+s.release.releaseId+' · runtime healthy · evidence store '+(s.evidenceStore.connected?'connected':'unavailable')+' · '+new Date(s.run.startedAt).toLocaleString();$('identity').innerHTML=[['Run',s.run.runId],['Supervisor',s.run.instanceId],['Child',s.child.instance.id],['Node',s.child.instance.node],['Storage',s.child.persistence.storage.identity||'none'],['Storage boots',s.child.persistence.storage.continuityBoots],['Database',s.child.database.identity||'none'],['Database boots',s.child.database.bootCount],['DB fingerprint',s.child.database.endpointFingerprint||'none'],['Graceful shutdowns',s.lifecycle.gracefulShutdowns]].map(x=>'<div class="card"><b>'+esc(x[0])+'</b><small>'+esc(x[1])+'</small></div>').join('');const c=r.summary||{};$('summary').innerHTML=[['Confirmed',c.confirmed||0],['Likely',c.likely||0],['Intermittent',c.intermittent||0],['Needs evidence',c.needsEvidence||0],['Not tested',c.notTested||0]].map(x=>'<div class="card"><b>'+x[0]+'</b><em>'+x[1]+'</em></div>').join('');$('findings').innerHTML=(r.findings||[]).map(f=>'<article class="finding '+cls(f.tone)+'"><div class="finding-head"><div><small class="category">'+esc(f.category)+'</small><h3>'+esc(f.name)+'</h3></div><strong class="badge '+cls(f.tone)+'">'+esc(f.result)+'</strong></div><p class="question">'+esc(f.question)+'</p><p class="evidence-summary">'+esc(f.evidenceSummary)+'</p>'+progress(f)+'<details><summary>Evidence & next test</summary><div class="next"><b>Next test</b><p>'+esc(f.nextTest)+'</p></div>'+evidenceDetails(f)+'</details></article>').join('');$('recent').innerHTML=(r.recentEvidence||[]).slice(0,30).map(x=>'<div class="audit"><span><b>'+esc(x.finding)+'</b><small>'+esc(new Date(x.createdAt).toLocaleString())+'</small></span><span class="'+cls(x.verdict)+'">'+esc(x.verdict)+'</span><small>'+esc(short(x.instanceId))+'</small></div>').join('')||'<p>No evidence recorded.</p>'}catch(e){$('state').textContent='Error: '+e.message}}async function logMarker(){try{const b=await j('/v13/log-marker',{method:'POST',headers:h(),body:JSON.stringify({label:$('label').value})});$('logOut').textContent=JSON.stringify(b,null,2);$('correlation').value=b.correlationId||'';refreshAll()}catch(e){$('logOut').textContent=JSON.stringify(e.body||{error:e.message},null,2)}}async function readiness(a){try{const b=await j('/v13/readiness',{method:'POST',headers:h(),body:JSON.stringify({action:a,seconds:60})});$('readyOut').textContent=JSON.stringify(b,null,2);if(b.correlationId)$('correlation').value=b.correlationId;refreshAll()}catch(e){$('readyOut').textContent=JSON.stringify(e.body||{error:e.message},null,2)}}async function probe(status){try{const b=await j('/v13/access',{method:'POST',headers:h(),body:JSON.stringify({status})});$('accessOut').textContent=JSON.stringify(b,null,2);$('correlation').value=b.correlationId||'';refreshAll()}catch(e){$('accessOut').textContent=JSON.stringify(e.body||{error:e.message},null,2)}}async function verify(verdict){try{const b=await j('/v13/verify',{method:'POST',headers:h(),body:JSON.stringify({testKey:$('verifyTest').value,verdict,correlationId:$('correlation').value,note:$('verifyNote').value})});$('verifyOut').textContent=JSON.stringify(b,null,2);refreshAll()}catch(e){$('verifyOut').textContent=JSON.stringify(e.body||{error:e.message},null,2)}}function connect(){const p=location.protocol==='https:'?'wss:':'ws:';WS=new WebSocket(p+'//'+location.host+'/ws');WS.onclose=()=>setTimeout(connect,2500)}async function exportEvidence(){const e=await j('/v13/evidence');const b=new Blob([JSON.stringify(e,null,2)],{type:'application/json'});const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='infrastry-gauntlet-evidence-'+Date.now()+'.json';a.click();URL.revokeObjectURL(a.href)}connect();refreshAll();setInterval(refreshAll,15000);`; }

function css() { return `:root{color-scheme:dark;--bg:#080b10;--panel:#111722;--line:#283345;--text:#f4f7fb;--muted:#9aa7b8;--lime:#9dfc61;--cyan:#6ae5ff;--amber:#ffd166;--red:#ff6b7a;--orange:#ff9d66}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 18% 0%,#102632,var(--bg) 36%);color:var(--text);font-family:system-ui,sans-serif}main{width:min(1220px,calc(100% - 28px));margin:auto;padding:42px 0}.eyebrow{color:var(--cyan);font-size:12px;letter-spacing:.15em;font-weight:850;margin:0 0 8px}h1{font-size:clamp(3rem,8vw,6.4rem);line-height:.9;letter-spacing:-.06em;margin:10px 0}h1 span{color:var(--lime)}h2{margin:0;font-size:1.7rem}.lede{color:var(--muted);max-width:920px;font-size:18px;line-height:1.6}.actions{display:flex;gap:8px;margin:22px 0;flex-wrap:wrap}.state,.card,.finding,.panel,.experiment-grid section,.audit{background:var(--panel);border:1px solid var(--line);border-radius:16px}.state{padding:16px}.report{margin-top:38px}.section-title{display:flex;justify-content:space-between;gap:20px;align-items:end}.section-title small{color:var(--muted)}.summary,.grid{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin:18px 0}.card{padding:16px}.card b,.card small{display:block}.card small{color:var(--muted);word-break:break-all}.card em{display:block;font-size:2rem;font-style:normal;margin-top:5px}.finding{padding:18px;margin:9px 0}.finding-head{display:flex;justify-content:space-between;gap:16px;align-items:start}.finding h3{margin:3px 0;font-size:1.2rem}.category{color:var(--cyan);text-transform:uppercase;letter-spacing:.09em}.question{color:var(--muted);margin:7px 0}.evidence-summary{margin:10px 0}.badge{border:1px solid currentColor;border-radius:999px;padding:6px 10px;font-size:.82rem;white-space:nowrap}.good{color:var(--lime)}.bad{color:var(--red)}.mixed{color:var(--orange)}.neutral{color:var(--cyan)}.open{color:var(--amber)}.progress{height:7px;background:#070a0f;border-radius:99px;overflow:hidden;margin-top:12px}.progress i{display:block;height:100%;background:currentColor}.finding>small{color:var(--muted)}.finding details{margin-top:12px}.finding details summary,.panel>summary{cursor:pointer;color:var(--muted)}.next{border-left:2px solid var(--cyan);padding-left:12px;margin:12px 0}.next p{margin:3px 0;color:var(--muted)}.egrid{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}.egrid div{background:#0b1018;padding:10px;border-radius:10px}.egrid b,.egrid small{display:block}.egrid small{color:var(--muted);word-break:break-word}.panel{margin-top:18px;overflow:hidden}.panel>summary{padding:16px;font-weight:750}.panel-body{padding:0 16px 16px}.note{color:var(--muted)}.experiment-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.experiment-grid section{padding:15px}.experiment-grid h3{margin-top:0}input,select,button{font:inherit;border-radius:10px;border:1px solid var(--line);background:#101621;color:var(--text);padding:10px 12px}input,select{width:100%;margin:5px 0 10px}button{cursor:pointer;font-weight:750}.audit{display:grid;grid-template-columns:1fr auto auto;gap:12px;align-items:center;padding:11px;margin:6px 0}.audit b,.audit small{display:block}.audit small{color:var(--muted)}.pass{color:var(--lime)}.fail{color:var(--red)}.pending,.observation,.warn{color:var(--amber)}pre{white-space:pre-wrap;word-break:break-word;background:#070a0f;padding:12px;border-radius:10px;color:#bcc8d6;max-height:320px;overflow:auto}footer{color:var(--muted);padding:30px 0}@media(max-width:900px){.summary{grid-template-columns:repeat(3,1fr)}.grid{grid-template-columns:repeat(2,1fr)}.experiment-grid{grid-template-columns:1fr}.section-title{display:block}.section-title small{display:block;margin-top:7px}.egrid{grid-template-columns:1fr 1fr}}@media(max-width:560px){.summary,.grid,.egrid{grid-template-columns:1fr}.finding-head{display:block}.badge{display:inline-block;margin-top:8px}.audit{display:block}.audit>*{display:block;margin-top:4px}}`; }
