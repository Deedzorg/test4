import http from 'node:http';
import { spawn } from 'node:child_process';
import { randomUUID, timingSafeEqual } from 'node:crypto';
import { hostname } from 'node:os';
import pg from 'pg';
import { WebSocketServer } from 'ws';

const { Pool } = pg;
const VERSION = '1.3.0';
const PORT = Number(process.env.PORT || 3000);
const INTERNAL_PORT = PORT + 137;
const HOST = process.env.HOST || '0.0.0.0';
const RUN_ID = randomUUID();
const INSTANCE_ID = randomUUID();
const STARTED_AT = new Date().toISOString();
const ADMIN_KEY = process.env.GAUNTLET_ADMIN_KEY || '';
const DATABASE_URL = process.env.DATABASE_URL || '';
const RELEASE = { releaseId: 'v1.3-baseline-001', experiment: 'reproducibility-observability' };
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
  throw new Error('v1.2 child did not become healthy within 20s');
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
      await recordOnce('WS-PROXY-ECHO-001', 'pass', { externalWebSocketEcho: true }, { externalWebSocketEcho: true }, { source: 'browser-via-infrastry-proxy' }).catch(() => {});
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
    if (forwardedProto(req) === 'https') await recordOnce('PROXY-HTTPS-001', 'pass', { https: true }, { https: true }, { forwardedProto: 'https' });
    return sendJson(res, 200, await v13Status(childStatus, req));
  }
  if (req.method === 'GET' && url.pathname === '/v13/evidence') return sendJson(res, 200, await evidence(req));
  if (req.method === 'GET' && url.pathname === '/v13/confidence') return sendJson(res, 200, { ok: true, confidence: await confidenceSummary(), recentRuns: await recentRuns(80) });
  if (req.method === 'GET' && url.pathname === '/readyz' && readinessActive()) {
    return sendJson(res, 503, { ok: false, readiness: 'forced-not-ready', liveness: true, correlationId: readinessOverride.correlationId, runId: RUN_ID });
  }
  if (req.method === 'GET' && url.pathname === '/v13/access') {
    const status = allowedStatus(Number(url.searchParams.get('status') || 200));
    const correlationId = String(url.searchParams.get('correlationId') || randomUUID()).slice(0, 120);
    remember({ correlationId, type: 'access-probe', status, at: now() });
    log('access.probe', { requestId, correlationId, status });
    return sendJson(res, status, { ok: status < 400, correlationId, status, runId: RUN_ID });
  }
  if (req.method === 'POST' && url.pathname === '/v13/log-marker') {
    if (!authorized(req)) return sendJson(res, 403, { ok: false, error: 'unauthorized' });
    const body = await readBody(req);
    const correlationId = randomUUID();
    const label = String(body.label || 'observability-check').slice(0, 120);
    const marker = { at: now(), correlationId, label, runId: RUN_ID, instanceId: INSTANCE_ID, releaseId: RELEASE.releaseId };
    console.log(JSON.stringify({ level: 'info', event: 'gauntlet.stdout_marker', ...marker }));
    console.error(JSON.stringify({ level: 'error', event: 'gauntlet.stderr_marker', ...marker }));
    await recordRun('OBS-LOG-MARKER-001', 'observation', { stdout: 'visible', stderr: 'visible' }, { emitted: true }, { correlationId, label });
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
    await recordRun('READINESS-SEPARATION-001', verdict, { health: 200, ready: 503 }, { health: health.status, ready: ready.status }, { correlationId, seconds, scope: 'endpoint-separation-only' });
    remember({ correlationId, type: 'readiness', health: health.status, ready: ready.status, at: now() });
    return sendJson(res, 201, { ok: true, correlationId, seconds, health: health.status, ready: ready.status, verdict });
  }
  if (req.method === 'POST' && url.pathname === '/v13/observation') {
    const body = await readBody(req);
    const testId = normalizeTestId(body.testId);
    const verdict = normalizeVerdict(body.verdict);
    if (!testId || !verdict) return sendJson(res, 400, { ok: false, error: 'invalid_observation' });
    const row = await recordRun(testId, verdict, body.expected || {}, body.actual || {}, body.evidence || {});
    return sendJson(res, 201, { ok: true, row });
  }
  return proxyToChild(req, res);
}

async function recordBootSuite() {
  const s = await childJson('/api/status');
  const checks = [
    ['CORE-RUNTIME-001', 'pass', { reachable: true }, { reachable: true, node: s.instance?.node }],
    ['NET-DYNAMIC-PORT-001', process.env.PORT ? 'pass' : 'fail', { platformPort: true }, { platformPort: Boolean(process.env.PORT) }],
    ['ENV-SENTINEL-001', s.environment?.sentinelPresent ? 'pass' : 'fail', { sentinel: true }, { sentinel: Boolean(s.environment?.sentinelPresent) }],
    ['STORAGE-WRITE-001', s.persistence?.storage?.ok ? 'pass' : 'fail', { writable: true }, { writable: Boolean(s.persistence?.storage?.ok), detail: s.persistence?.storage?.detail }],
    ['STORAGE-CONTINUITY-001', Number(s.persistence?.storage?.continuityBoots || 0) > 1 ? 'pass' : 'pending', { continuityBoots: '>=2' }, { identity: s.persistence?.storage?.identity, continuityBoots: s.persistence?.storage?.continuityBoots }],
    ['DB-CONNECT-001', s.database?.ok ? 'pass' : 'fail', { connected: true }, { connected: Boolean(s.database?.ok), identity: s.database?.identity }],
    ['DB-CONTINUITY-001', Number(s.database?.bootCount || 0) > 1 ? 'pass' : 'pending', { bootCount: '>=2' }, { bootCount: s.database?.bootCount, identity: s.database?.identity, rows: s.database?.probeRows }],
    ['RESTART-RECOVERY-001', Number(s.database?.bootCount || 0) > 1 ? 'pass' : 'pending', { observedBoots: '>=2' }, { observedBoots: s.database?.bootCount }],
    ['SHUTDOWN-GRACEFUL-001', await gracefulCount() > 0 ? 'pass' : 'pending', { gracefulShutdowns: '>=1' }, { shutdownSignals: await signalCount(), gracefulShutdowns: await gracefulCount() }]
  ];
  for (const [id, verdict, expected, actual] of checks) await recordOnce(id, verdict, expected, actual, { source: 'automatic-boot-suite' });
  log('assurance.boot_suite_recorded', { count: checks.length });
}

async function v13Status(childStatus, req) {
  return {
    ok: true,
    service: 'Infrastry Gauntlet', version: VERSION, release: RELEASE,
    run: { runId: RUN_ID, instanceId: INSTANCE_ID, host: hostname(), pid: process.pid, startedAt: STARTED_AT },
    child: childStatus,
    proxy: { forwardedProto: forwardedProto(req), httpsObserved: forwardedProto(req) === 'https' },
    readinessExperiment: readinessOverride,
    observability: { requests, websocketConnections: wsConnections, activeWebSockets: wss?.clients?.size || 0, correlations: correlations.slice(-12) },
    lifecycle: { shutdownSignals: await signalCount(), gracefulShutdowns: await gracefulCount() },
    confidence: await confidenceSummary()
  };
}

async function evidence(req) {
  const childStatus = await childJson('/api/status');
  return {
    schemaVersion: 1,
    generatedAt: now(),
    status: await v13Status(childStatus, req),
    confidence: await confidenceSummary(),
    recentRuns: await recentRuns(120),
    methodology: {
      confirmationThreshold: 3,
      confirmedPass: '3+ PASS and 0 FAIL',
      confirmedDefect: '3+ FAIL and 0 PASS',
      intermittent: 'at least 1 PASS and 1 FAIL',
      rule: 'Negative findings remain observations until reproduced under controlled conditions.'
    }
  };
}

async function recordOnce(testId, verdict, expected, actual, evidence) {
  if (!pool) return null;
  const existing = await pool.query(`SELECT id FROM gauntlet_v13_test_runs WHERE test_id=$1 AND run_id=$2 LIMIT 1`, [testId, RUN_ID]);
  if (existing.rowCount) return existing.rows[0];
  return recordRun(testId, verdict, expected, actual, evidence);
}

async function recordRun(testId, verdict, expected = {}, actual = {}, evidence = {}) {
  const row = { testId, verdict, runId: RUN_ID, releaseId: RELEASE.releaseId, instanceId: INSTANCE_ID, expected, actual, evidence };
  if (!pool) return row;
  const r = await pool.query(`INSERT INTO gauntlet_v13_test_runs(test_id,run_id,release_id,instance_id,verdict,expected,actual,evidence)
    VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8::jsonb) RETURNING *`,
    [testId, RUN_ID, RELEASE.releaseId, INSTANCE_ID, verdict, JSON.stringify(expected), JSON.stringify(actual), JSON.stringify(evidence)]);
  return r.rows[0];
}

async function confidenceSummary() {
  if (!pool) return [];
  const r = await pool.query(`SELECT test_id, COUNT(*)::int total,
    COUNT(*) FILTER(WHERE verdict='pass')::int pass,
    COUNT(*) FILTER(WHERE verdict='fail')::int fail,
    COUNT(*) FILTER(WHERE verdict='warn')::int warn,
    COUNT(*) FILTER(WHERE verdict='pending')::int pending,
    COUNT(DISTINCT instance_id)::int instances, COUNT(DISTINCT release_id)::int releases,
    MIN(created_at) first_seen, MAX(created_at) last_seen
    FROM gauntlet_v13_test_runs GROUP BY test_id ORDER BY test_id`);
  return r.rows.map((x) => ({ testId: x.test_id, totalRuns: +x.total, passRuns: +x.pass, failRuns: +x.fail, warnRuns: +x.warn, pendingRuns: +x.pending, distinctInstances: +x.instances, distinctReleases: +x.releases, firstSeen: x.first_seen, lastSeen: x.last_seen, confidence: confidenceLabel(+x.pass, +x.fail) }));
}

function confidenceLabel(pass, fail) {
  if (pass >= 3 && fail === 0) return 'confirmed-pass';
  if (fail >= 3 && pass === 0) return 'confirmed-defect';
  if (pass > 0 && fail > 0) return 'intermittent';
  if (pass === 2 && fail === 0) return 'likely-pass';
  if (fail === 2 && pass === 0) return 'likely-defect';
  if (pass + fail === 1) return 'single-observation';
  return 'insufficient-evidence';
}

async function recentRuns(limit) {
  if (!pool) return [];
  const r = await pool.query(`SELECT id,test_id,run_id,release_id,instance_id,verdict,expected,actual,evidence,created_at FROM gauntlet_v13_test_runs ORDER BY id DESC LIMIT $1`, [Math.min(200, limit)]);
  return r.rows;
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
function normalizeTestId(v) { const s = String(v || '').trim().toUpperCase(); return /^[A-Z0-9][A-Z0-9._-]{2,63}$/.test(s) ? s : null; }
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

function page() { return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Infrastry Gauntlet v1.3</title><style>${css()}</style></head><body><main>
<p class="eyebrow">DEEDZ LABS / PLATFORM ASSURANCE HARNESS</p><h1>Infrastry <span>Gauntlet</span></h1><p class="lede">v1.3 · Reproducibility + Observability. Evidence-driven testing with repeatable run IDs, durable evidence, correlated logs and confidence labels.</p>
<div class="actions"><button onclick="refreshAll()">Refresh</button><button onclick="exportEvidence()">Export evidence</button></div><div id="state" class="box">Loading…</div>
<h2>Run identity</h2><div id="identity" class="grid"></div><h2>Reproduction confidence</h2><div id="confidence"></div>
<div class="split"><section><h2>Observability markers</h2><input id="key" type="password" placeholder="GAUNTLET_ADMIN_KEY"><input id="label" value="observability-check"><button onclick="logMarker()">Emit stdout + stderr markers</button><pre id="logOut">Not emitted.</pre></section>
<section><h2>Readiness separation</h2><p>Temporarily force <code>/readyz=503</code> while <code>/healthz=200</code>.</p><button onclick="readiness('start')">Start 60s test</button><button onclick="readiness('stop')">Stop</button><pre id="readyOut">Not running.</pre></section></div>
<div class="split"><section><h2>Access-log probes</h2><button onclick="probe(200)">200</button><button onclick="probe(404)">404</button><button onclick="probe(500)">500</button><pre id="accessOut">No probes.</pre></section>
<section><h2>WebSocket proxy</h2><p id="wsState">connecting…</p><input id="wsMsg" value="goose-check"><button onclick="sendWs()">Send echo</button><pre id="wsOut">Waiting…</pre></section></div>
<h2>Recent evidence</h2><pre id="recent"></pre><footer>Built to produce evidence, not vibes. · confirmation threshold: 3 controlled reproductions</footer></main><script>${clientJs()}</script></body></html>`; }
function clientJs(){ return `let WS;const $=id=>document.getElementById(id);const esc=v=>String(v??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));async function j(p,o={}){const r=await fetch(p,{cache:'no-store',...o,headers:{'Content-Type':'application/json',...(o.headers||{})}});const t=await r.text();let b={};try{b=t?JSON.parse(t):{}}catch{b={raw:t}}if(!r.ok)throw Object.assign(new Error(r.status+' '+p),{body:b});return b}async function refreshAll(){try{const [s,c]=await Promise.all([j('/v13/status'),j('/v13/confidence')]);$('state').textContent='v'+s.version+' · '+s.release.releaseId+' · run '+s.run.runId;$('identity').innerHTML=[['v1.3 run',s.run.runId],['supervisor',s.run.instanceId],['child',s.child.instance.id],['storage',s.child.persistence.storage.identity||'none'],['storage boots',s.child.persistence.storage.continuityBoots],['database',s.child.database.identity||'none'],['DB boots',s.child.database.bootCount],['shutdowns',s.lifecycle.gracefulShutdowns]].map(x=>'<div class="card"><b>'+esc(x[0])+'</b><small>'+esc(x[1])+'</small></div>').join('');$('confidence').innerHTML=(c.confidence||[]).map(x=>'<div class="row"><span><b>'+esc(x.testId)+'</b><small>'+x.passRuns+' pass · '+x.failRuns+' fail · '+x.pendingRuns+' pending · '+x.distinctInstances+' instances</small></span><strong class="'+esc(x.confidence)+'">'+esc(x.confidence)+'</strong></div>').join('')||'<div class="row">No v1.3 persisted results yet.</div>';$('recent').textContent=JSON.stringify((c.recentRuns||[]).slice(0,30),null,2)}catch(e){$('state').textContent='Error: '+e.message}}function h(){return {'x-gauntlet-key':$('key').value||''}}async function logMarker(){try{$('logOut').textContent=JSON.stringify(await j('/v13/log-marker',{method:'POST',headers:h(),body:JSON.stringify({label:$('label').value})}),null,2);refreshAll()}catch(e){$('logOut').textContent=JSON.stringify(e.body||{error:e.message},null,2)}}async function readiness(a){try{$('readyOut').textContent=JSON.stringify(await j('/v13/readiness',{method:'POST',headers:h(),body:JSON.stringify({action:a,seconds:60})}),null,2);refreshAll()}catch(e){$('readyOut').textContent=JSON.stringify(e.body||{error:e.message},null,2)}}async function probe(s){const id=crypto.randomUUID();const r=await fetch('/v13/access?status='+s+'&correlationId='+id,{cache:'no-store'});const b=await r.json();$('accessOut').textContent=JSON.stringify({correlationId:id,observedStatus:r.status,body:b},null,2)}function connect(){const p=location.protocol==='https:'?'wss:':'ws:';WS=new WebSocket(p+'//'+location.host+'/ws');WS.onopen=()=>$('wsState').textContent='connected';WS.onclose=()=>{ $('wsState').textContent='disconnected';setTimeout(connect,2500)};WS.onmessage=e=>$('wsOut').textContent=e.data}function sendWs(){if(WS?.readyState===1)WS.send($('wsMsg').value)}async function exportEvidence(){const e=await j('/v13/evidence');const b=new Blob([JSON.stringify(e,null,2)],{type:'application/json'});const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='infrastry-gauntlet-v13-'+e.status.release.releaseId+'-'+Date.now()+'.json';a.click();URL.revokeObjectURL(a.href)}connect();refreshAll();setInterval(refreshAll,15000);`; }
function css(){return `:root{color-scheme:dark;--p:#111722;--line:#283345;--t:#f4f7fb;--m:#9aa7b8;--a:#9dfc61;--c:#6ae5ff;--w:#ffd166;--b:#ff6b7a}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 20% 0%,#102632,#090b10 35%);color:var(--t);font-family:system-ui,sans-serif}main{width:min(1180px,calc(100% - 28px));margin:auto;padding:44px 0}.eyebrow{color:var(--c);font-size:12px;letter-spacing:.15em;font-weight:800}h1{font-size:clamp(3rem,8vw,6.5rem);line-height:.9;letter-spacing:-.06em;margin:10px 0}h1 span{color:var(--a)}.lede{color:var(--m);max-width:900px;font-size:18px;line-height:1.6}.actions{display:flex;gap:8px;margin:24px 0;flex-wrap:wrap}.box,section,.card,.row{background:var(--p);border:1px solid var(--line);border-radius:16px;padding:16px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.card b,.card small{display:block}.card small,.row small{color:var(--m);word-break:break-all}.split{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin:14px 0}input,button{font:inherit;border-radius:10px;border:1px solid var(--line);background:#111722;color:var(--t);padding:10px 12px}input{width:100%;margin:5px 0 10px}button{cursor:pointer;font-weight:750}.row{display:flex;justify-content:space-between;gap:12px;margin:7px 0}.confirmed-pass,.likely-pass{color:var(--a)}.confirmed-defect,.likely-defect{color:var(--b)}.intermittent{color:#ff9d66}.single-observation,.insufficient-evidence{color:var(--w)}pre{white-space:pre-wrap;word-break:break-word;background:#080b10;padding:12px;border-radius:10px;color:#bcc8d6;max-height:360px;overflow:auto}footer{color:var(--m);padding:30px 0}@media(max-width:850px){.grid{grid-template-columns:1fr 1fr}.split{grid-template-columns:1fr}}@media(max-width:500px){.grid{grid-template-columns:1fr}}`;}
