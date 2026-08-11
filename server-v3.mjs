import http from 'node:http';
import { appendFile, mkdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { createHash, randomUUID, timingSafeEqual } from 'node:crypto';
import { hostname } from 'node:os';
import { resolve, join } from 'node:path';
import pg from 'pg';
import { WebSocketServer } from 'ws';

const { Pool } = pg;
const VERSION = '1.2.0';
const PORT = Number.parseInt(process.env.PORT || '3000', 10);
const HOST = process.env.HOST || '0.0.0.0';
const DATA_DIR = resolve(process.env.DATA_DIR || '.data');
const STATE_FILE = join(DATA_DIR, 'state.json');
const STORAGE_ID_FILE = join(DATA_DIR, 'storage-identity.json');
const EVENTS_FILE = join(DATA_DIR, 'events.ndjson');
const DATABASE_URL = process.env.DATABASE_URL || '';
const REQUIRE_DATABASE = /^true$/i.test(process.env.REQUIRE_DATABASE || 'false');
const CHAOS_ENABLED = /^true$/i.test(process.env.CHAOS_ENABLED || 'false');
const ADMIN_KEY = process.env.GAUNTLET_ADMIN_KEY || '';
const SENTINEL_PRESENT = Boolean(process.env.GAUNTLET_SENTINEL);
const INSTANCE_ID = randomUUID();
const STARTED_AT = new Date().toISOString();
const CLIENT_JS = new URL('./public/app.js', import.meta.url);
const RELEASE_FILE = new URL('./gauntlet-release.json', import.meta.url);

let pool = null;
let clientJs = '';
let release = { releaseId: 'unknown', experiment: 'unknown' };
let storage = { ok: false, detail: 'not tested', dataDir: DATA_DIR, identity: null, identityCreatedAt: null, continuityBoots: 0 };
let database = {
  configured: Boolean(DATABASE_URL), required: REQUIRE_DATABASE, ok: false,
  detail: DATABASE_URL ? 'initializing' : 'not configured', identity: null,
  identityCreatedAt: null, bootCount: 0, probeRows: 0, maxProbeId: null,
  endpointFingerprint: safeDatabaseEndpointFingerprint(DATABASE_URL), databaseName: null,
  serverVersion: null, gracefulShutdowns: 0
};
let state = {
  schemaVersion: 3, bootCount: 0, persistentWrites: 0, databaseWrites: 0,
  snapshots: 0, storageIdentity: null, storageContinuityBoots: 0
};
let requests = 0;
let wsConnections = 0;
let heartbeat = { count: 0, lastAt: null };

await initialize();

const server = http.createServer(async (req, res) => {
  requests++;
  setHeaders(res);
  try { await route(req, res); }
  catch (error) {
    log('request.failed', { path: req.url, message: error.message, level: 'error' });
    json(res, 500, { ok: false, error: 'internal_error', detail: error.message });
  }
});

const wss = new WebSocketServer({ noServer: true });
wss.on('connection', (ws) => {
  wsConnections++;
  ws.send(JSON.stringify({ type: 'hello', instanceId: INSTANCE_ID, releaseId: release.releaseId, bootCount: state.bootCount, at: new Date().toISOString() }));
  ws.on('message', (raw) => ws.send(JSON.stringify({ type: 'echo', value: raw.toString().slice(0, 2048), instanceId: INSTANCE_ID, releaseId: release.releaseId, at: new Date().toISOString() })));
});
server.on('upgrade', (req, socket, head) => {
  if (new URL(req.url, 'http://local').pathname !== '/ws') return socket.destroy();
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});
server.listen(PORT, HOST, () => log('server.listening', { host: HOST, port: PORT, node: process.version, releaseId: release.releaseId }));

const heart = setInterval(() => {
  heartbeat = { count: heartbeat.count + 1, lastAt: new Date().toISOString() };
  if (heartbeat.count % 4 === 0) void event('background.heartbeat', heartbeat);
}, 15000);
heart.unref();
for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, () => void shutdown(signal));

async function initialize() {
  try { clientJs = await readFile(CLIENT_JS, 'utf8'); }
  catch (error) {
    clientJs = `console.error(${JSON.stringify('Gauntlet client asset failed to load')});`;
    log('client.asset_failed', { message: error.message, level: 'error' });
  }
  try { release = { ...release, ...JSON.parse(await readFile(RELEASE_FILE, 'utf8')) }; }
  catch (error) { log('release.metadata_failed', { message: error.message, level: 'warn' }); }

  await initializeStorage();
  await initializeDatabase();
  await event('process.boot', {
    releaseId: release.releaseId,
    bootCount: state.bootCount,
    storageIdentity: storage.identity,
    storageContinuityBoots: storage.continuityBoots,
    databaseIdentity: database.identity,
    databaseBootCount: database.bootCount,
    databaseEndpointFingerprint: database.endpointFingerprint
  });
}

async function initializeStorage() {
  try {
    await mkdir(DATA_DIR, { recursive: true });
    try { state = { ...state, ...JSON.parse(await readFile(STATE_FILE, 'utf8')) }; }
    catch (error) { if (error.code !== 'ENOENT') throw error; }

    let identity;
    try { identity = JSON.parse(await readFile(STORAGE_ID_FILE, 'utf8')); }
    catch (error) {
      if (error.code !== 'ENOENT') throw error;
      identity = { id: randomUUID(), createdAt: STARTED_AT };
      await writeFile(STORAGE_ID_FILE, JSON.stringify(identity, null, 2));
    }

    const sameIdentity = Boolean(state.storageIdentity && state.storageIdentity === identity.id);
    state.schemaVersion = 3;
    state.bootCount = Number(state.bootCount || 0) + 1;
    state.storageContinuityBoots = sameIdentity ? Number(state.storageContinuityBoots || 1) + 1 : 1;
    state.storageIdentity = identity.id;
    state.lastBootAt = STARTED_AT;
    state.lastInstanceId = INSTANCE_ID;
    state.lastReleaseId = release.releaseId;
    await saveState();

    const probe = join(DATA_DIR, `.probe-${INSTANCE_ID}`);
    await writeFile(probe, 'ok');
    const probeOk = (await readFile(probe, 'utf8')) === 'ok';
    await rm(probe, { force: true });
    storage = {
      ok: probeOk, detail: probeOk ? 'read/write verified' : 'probe mismatch', dataDir: DATA_DIR,
      identity: identity.id, identityCreatedAt: identity.createdAt,
      continuityBoots: state.storageContinuityBoots
    };
  } catch (error) {
    storage = { ...storage, ok: false, detail: error.message, dataDir: DATA_DIR };
    state.bootCount = Math.max(1, Number(state.bootCount || 0));
  }
}

async function initializeDatabase() {
  if (!DATABASE_URL) {
    database = { ...database, configured: false, ok: false, detail: REQUIRE_DATABASE ? 'DATABASE_URL missing' : 'not configured' };
    return;
  }

  try {
    pool = new Pool({ connectionString: DATABASE_URL, max: 3, connectionTimeoutMillis: 5000 });
    await pool.query(`CREATE TABLE IF NOT EXISTS gauntlet_probe (
      id BIGSERIAL PRIMARY KEY,
      instance_id TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`);
    await pool.query(`CREATE TABLE IF NOT EXISTS gauntlet_meta (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`);
    await pool.query(`CREATE TABLE IF NOT EXISTS gauntlet_runtime_events (
      id BIGSERIAL PRIMARY KEY,
      event_type TEXT NOT NULL,
      instance_id TEXT NOT NULL,
      release_id TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`);

    const candidateIdentity = randomUUID();
    await pool.query(
      `INSERT INTO gauntlet_meta(key, value) VALUES ('database_identity', $1)
       ON CONFLICT (key) DO NOTHING`,
      [candidateIdentity]
    );
    const identityResult = await pool.query(`SELECT value, created_at FROM gauntlet_meta WHERE key='database_identity'`);
    await pool.query(
      `INSERT INTO gauntlet_runtime_events(event_type, instance_id, release_id) VALUES ('boot', $1, $2)`,
      [INSTANCE_ID, release.releaseId]
    );

    const stats = await pool.query(`
      SELECT
        (SELECT COUNT(*)::int FROM gauntlet_runtime_events WHERE event_type='boot') AS boot_count,
        (SELECT COUNT(*)::int FROM gauntlet_runtime_events WHERE event_type='graceful_shutdown') AS graceful_shutdowns,
        (SELECT COUNT(*)::int FROM gauntlet_probe) AS probe_rows,
        (SELECT MAX(id)::bigint FROM gauntlet_probe) AS max_probe_id,
        current_database() AS database_name,
        current_setting('server_version') AS server_version
    `);
    const row = stats.rows[0];
    database = {
      ...database,
      configured: true, required: REQUIRE_DATABASE, ok: true, detail: 'connected',
      identity: identityResult.rows[0]?.value || null,
      identityCreatedAt: identityResult.rows[0]?.created_at || null,
      bootCount: Number(row.boot_count || 0),
      gracefulShutdowns: Number(row.graceful_shutdowns || 0),
      probeRows: Number(row.probe_rows || 0),
      maxProbeId: row.max_probe_id === null ? null : String(row.max_probe_id),
      databaseName: row.database_name || null,
      serverVersion: row.server_version || null,
      endpointFingerprint: safeDatabaseEndpointFingerprint(DATABASE_URL)
    };
  } catch (error) {
    database = { ...database, configured: true, required: REQUIRE_DATABASE, ok: false, detail: error.message };
  }
}

async function refreshDatabaseStats() {
  if (!pool || !database.ok) return;
  const stats = await pool.query(`
    SELECT
      (SELECT COUNT(*)::int FROM gauntlet_runtime_events WHERE event_type='boot') AS boot_count,
      (SELECT COUNT(*)::int FROM gauntlet_runtime_events WHERE event_type='graceful_shutdown') AS graceful_shutdowns,
      (SELECT COUNT(*)::int FROM gauntlet_probe) AS probe_rows,
      (SELECT MAX(id)::bigint FROM gauntlet_probe) AS max_probe_id
  `);
  const row = stats.rows[0];
  database.bootCount = Number(row.boot_count || 0);
  database.gracefulShutdowns = Number(row.graceful_shutdowns || 0);
  database.probeRows = Number(row.probe_rows || 0);
  database.maxProbeId = row.max_probe_id === null ? null : String(row.max_probe_id);
}

async function route(req, res) {
  const url = new URL(req.url, 'http://local');
  if (req.method === 'GET' && url.pathname === '/') return html(res, page());
  if (req.method === 'GET' && url.pathname === '/app.js') return javascript(res, clientJs);
  if (req.method === 'GET' && url.pathname === '/healthz') return json(res, 200, { ok: true, service: 'infrastry-gauntlet', version: VERSION, releaseId: release.releaseId, instanceId: INSTANCE_ID });
  if (req.method === 'GET' && url.pathname === '/readyz') {
    const ok = storage.ok && (!REQUIRE_DATABASE || database.ok);
    return json(res, ok ? 200 : 503, { ok, storage, database });
  }
  if (req.method === 'GET' && url.pathname === '/api/status') {
    await refreshDatabaseStats().catch(() => {});
    return json(res, 200, status(req));
  }
  if (req.method === 'GET' && url.pathname === '/api/tests') {
    await refreshDatabaseStats().catch(() => {});
    return json(res, 200, { ok: true, tests: tests(req) });
  }
  if (req.method === 'GET' && url.pathname === '/api/events') return json(res, 200, { ok: true, events: await recentEvents() });
  if (req.method === 'GET' && url.pathname === '/api/evidence') {
    await refreshDatabaseStats().catch(() => {});
    return json(res, 200, continuityEvidence());
  }
  if (req.method === 'GET' && url.pathname === '/metrics') {
    await refreshDatabaseStats().catch(() => {});
    return text(res, 200,
      `gauntlet_boot_count ${state.bootCount}\n` +
      `gauntlet_storage_continuity_boots ${storage.continuityBoots}\n` +
      `gauntlet_database_boot_count ${database.bootCount}\n` +
      `gauntlet_database_probe_rows ${database.probeRows}\n` +
      `gauntlet_persistent_writes_total ${state.persistentWrites}\n` +
      `gauntlet_database_writes_total ${state.databaseWrites}\n` +
      `gauntlet_requests_total ${requests}\n` +
      `gauntlet_active_websockets ${wss.clients.size}\n`
    );
  }

  if (req.method === 'POST' && url.pathname === '/api/persist') {
    if (!storage.ok) return json(res, 409, { ok: false, error: 'storage_not_available', storage });
    const body = await bodyJson(req);
    state.persistentWrites++;
    state.lastMarker = String(body.marker || `marker-${state.persistentWrites}`).slice(0, 160);
    state.lastWriteAt = new Date().toISOString();
    await saveState();
    await event('storage.persist', { marker: state.lastMarker });
    return json(res, 201, { ok: true, marker: state.lastMarker, persistentWrites: state.persistentWrites, bootCount: state.bootCount, storageIdentity: storage.identity });
  }

  if (req.method === 'POST' && url.pathname === '/api/database/probe') {
    if (!pool || !database.ok) return json(res, 409, { ok: false, error: 'database_not_available', database });
    const result = await pool.query('INSERT INTO gauntlet_probe(instance_id) VALUES($1) RETURNING id, created_at', [INSTANCE_ID]);
    state.databaseWrites++;
    if (storage.ok) await saveState();
    await refreshDatabaseStats();
    return json(res, 201, { ok: true, row: result.rows[0], databaseWrites: state.databaseWrites, databaseIdentity: database.identity, probeRows: database.probeRows, maxProbeId: database.maxProbeId });
  }

  if (req.method === 'POST' && url.pathname === '/api/snapshot') {
    state.snapshots++;
    if (storage.ok) await saveState();
    await refreshDatabaseStats().catch(() => {});
    await event('evidence.snapshot', { snapshot: state.snapshots, status: status(req), tests: tests(req), continuity: continuityEvidence() });
    return json(res, 201, { ok: true, snapshot: state.snapshots });
  }

  if (req.method === 'POST' && url.pathname === '/api/chaos/crash') {
    if (!authorized(req)) return json(res, 403, { ok: false, error: 'chaos_disabled_or_unauthorized' });
    await event('chaos.crash_requested', {});
    json(res, 202, { ok: true, message: 'exit scheduled' });
    setTimeout(() => process.exit(77), 250).unref();
    return;
  }
  return json(res, 404, { ok: false, error: 'not_found' });
}

function status(req) {
  const forwardedProto = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim();
  return {
    ok: storage.ok && (!REQUIRE_DATABASE || database.ok),
    service: 'Infrastry Gauntlet', version: VERSION, release,
    instance: {
      id: INSTANCE_ID, host: hostname(), pid: process.pid, node: process.version,
      startedAt: STARTED_AT, uptimeSeconds: Math.round(process.uptime()), rssBytes: process.memoryUsage().rss
    },
    persistence: {
      storage, bootCount: state.bootCount, persistentWrites: state.persistentWrites,
      lastMarker: state.lastMarker || null, lastWriteAt: state.lastWriteAt || null,
      lastGracefulShutdownAt: state.lastGracefulShutdownAt || null
    },
    database: { ...database, writes: state.databaseWrites },
    environment: {
      portProvided: Boolean(process.env.PORT), dataDirProvided: Boolean(process.env.DATA_DIR),
      sentinelPresent: SENTINEL_PRESENT, databaseUrlPresent: Boolean(DATABASE_URL),
      adminKeyPresent: Boolean(ADMIN_KEY), requireDatabase: REQUIRE_DATABASE,
      chaosEnabled: CHAOS_ENABLED, nodeEnv: process.env.NODE_ENV || null,
      sourceRevision: safeSourceRevision()
    },
    proxy: { forwardedProto: forwardedProto || null, httpsObserved: forwardedProto === 'https' || Boolean(req.socket.encrypted) },
    background: heartbeat,
    traffic: { requests, websocketConnections: wsConnections, activeWebSockets: wss.clients.size },
    snapshots: state.snapshots,
    continuity: continuityEvidence()
  };
}

function continuityEvidence() {
  return {
    releaseId: release.releaseId,
    experiment: release.experiment,
    instanceId: INSTANCE_ID,
    processStartedAt: STARTED_AT,
    storage: {
      identity: storage.identity,
      identityCreatedAt: storage.identityCreatedAt,
      continuityBoots: storage.continuityBoots,
      bootCount: state.bootCount,
      dataDir: DATA_DIR,
      lastMarker: state.lastMarker || null
    },
    database: {
      identity: database.identity,
      identityCreatedAt: database.identityCreatedAt,
      bootCount: database.bootCount,
      probeRows: database.probeRows,
      maxProbeId: database.maxProbeId,
      endpointFingerprint: database.endpointFingerprint,
      databaseName: database.databaseName,
      serverVersion: database.serverVersion,
      gracefulShutdowns: database.gracefulShutdowns
    }
  };
}

function tests(req) {
  const s = status(req);
  const age = heartbeat.lastAt ? Math.round((Date.now() - Date.parse(heartbeat.lastAt)) / 1000) : null;
  return [
    test('runtime', 'pass', `HTTP process running on ${process.version}`),
    test('dynamic-port', process.env.PORT ? 'pass' : 'warn', process.env.PORT ? 'PORT supplied by platform' : 'using local fallback port'),
    test('health', 'pass', '/healthz and /readyz exposed'),
    test('environment', SENTINEL_PRESENT ? 'pass' : 'warn', SENTINEL_PRESENT ? 'GAUNTLET_SENTINEL detected' : 'set GAUNTLET_SENTINEL to prove env injection'),
    test('storage-write', storage.ok ? 'pass' : 'fail', storage.detail),
    test('storage-continuity', storage.continuityBoots > 1 ? 'pass' : 'pending', `storageIdentity=${short(storage.identity)} continuityBoots=${storage.continuityBoots}`),
    test('websocket', 'pass', 'upgrade endpoint available at /ws; browser performs live echo'),
    test('background-work', age === null ? 'pending' : age <= 45 ? 'pass' : 'fail', age === null ? 'waiting for first heartbeat' : `heartbeat ${age}s ago`),
    test('database', database.configured ? (database.ok ? 'pass' : 'fail') : (REQUIRE_DATABASE ? 'fail' : 'skip'), database.detail),
    test('database-continuity', database.ok ? (database.bootCount > 1 ? 'pass' : 'pending') : 'skip', database.ok ? `databaseIdentity=${short(database.identity)} boots=${database.bootCount} rows=${database.probeRows}` : database.detail),
    test('https-proxy', s.proxy.httpsObserved ? 'pass' : 'warn', s.proxy.httpsObserved ? 'HTTPS observed' : 'no HTTPS forwarding signal'),
    test('restart-recovery', database.ok && database.bootCount > 1 ? 'pass' : 'pending', database.ok ? `database observed ${database.bootCount} process boot(s)` : `filesystem observed ${state.bootCount} boot(s)`),
    test('graceful-shutdown', database.ok && database.gracefulShutdowns > 0 ? 'pass' : (state.lastGracefulShutdownAt ? 'pass' : 'pending'), database.gracefulShutdowns > 0 ? `database recorded ${database.gracefulShutdowns} graceful shutdown(s)` : (state.lastGracefulShutdownAt || 'not observed yet'))
  ];
}

function test(id, statusValue, detail) { return { id, status: statusValue, passed: statusValue === 'pass', detail }; }
function short(value) { return value ? String(value).slice(0, 8) : 'none'; }
function safeSourceRevision() {
  return process.env.GITHUB_SHA || process.env.COMMIT_SHA || process.env.SOURCE_VERSION || process.env.GIT_COMMIT_SHA || process.env.INFRASTRY_GIT_SHA || null;
}
function safeDatabaseEndpointFingerprint(value) {
  if (!value) return null;
  try {
    const parsed = new URL(value);
    const safeEndpoint = `${parsed.protocol}//${parsed.hostname}:${parsed.port || 'default'}${parsed.pathname}`;
    return createHash('sha256').update(safeEndpoint).digest('hex').slice(0, 12);
  } catch {
    return createHash('sha256').update('configured').digest('hex').slice(0, 12);
  }
}
async function bodyJson(req) {
  const chunks = []; let n = 0;
  for await (const chunk of req) {
    n += chunk.length;
    if (n > 32768) throw new Error('body_too_large');
    chunks.push(chunk);
  }
  return chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : {};
}
async function saveState() {
  const tmp = `${STATE_FILE}.tmp`;
  await writeFile(tmp, JSON.stringify(state, null, 2));
  await rename(tmp, STATE_FILE);
}
async function event(type, data) {
  const record = { at: new Date().toISOString(), type, instanceId: INSTANCE_ID, releaseId: release.releaseId, ...data };
  if (storage.ok) { try { await appendFile(EVENTS_FILE, `${JSON.stringify(record)}\n`); } catch {} }
  log(type, data);
}
async function recentEvents() {
  if (!storage.ok) return [];
  try { return (await readFile(EVENTS_FILE, 'utf8')).trim().split('\n').filter(Boolean).slice(-50).map(JSON.parse).reverse(); }
  catch (error) {
    if (error.code === 'ENOENT') return [];
    return [{ at: new Date().toISOString(), type: 'events.read_failed', detail: error.message }];
  }
}
function authorized(req) {
  if (!CHAOS_ENABLED || !ADMIN_KEY) return false;
  const a = Buffer.from(String(req.headers['x-gauntlet-key'] || ''));
  const b = Buffer.from(ADMIN_KEY);
  return a.length === b.length && timingSafeEqual(a, b);
}
function setHeaders(res) {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
  res.setHeader('Content-Security-Policy', "default-src 'self'; connect-src 'self' ws: wss:; style-src 'unsafe-inline'; script-src 'self'; base-uri 'none'; frame-ancestors 'none'");
}
function json(res, code, value) { res.statusCode = code; res.setHeader('Content-Type', 'application/json; charset=utf-8'); res.setHeader('Cache-Control', 'no-store'); res.end(JSON.stringify(value)); }
function text(res, code, value) { res.statusCode = code; res.setHeader('Content-Type', 'text/plain; charset=utf-8'); res.end(value); }
function javascript(res, value) { res.statusCode = 200; res.setHeader('Content-Type', 'text/javascript; charset=utf-8'); res.setHeader('Cache-Control', 'no-store'); res.end(value); }
function html(res, value) { res.statusCode = 200; res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.setHeader('Cache-Control', 'no-store'); res.end(value); }
function log(eventName, data = {}) { console.log(JSON.stringify({ at: new Date().toISOString(), level: data.level || 'info', event: eventName, service: 'infrastry-gauntlet', instanceId: INSTANCE_ID, releaseId: release.releaseId, ...data })); }

async function shutdown(signal) {
  clearInterval(heart);
  state.lastGracefulShutdownAt = new Date().toISOString();
  if (storage.ok) await saveState().catch(() => {});
  if (pool && database.ok) {
    await pool.query(`INSERT INTO gauntlet_runtime_events(event_type, instance_id, release_id) VALUES ('graceful_shutdown', $1, $2)`, [INSTANCE_ID, release.releaseId]).catch(() => {});
  }
  await event('process.shutdown', { signal }).catch(() => {});
  for (const client of wss.clients) client.close(1001, 'shutdown');
  await new Promise((resolveClose) => server.close(resolveClose));
  if (pool) await pool.end().catch(() => {});
  process.exit(0);
}

function page() {
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Infrastry Gauntlet</title><style>${css()}</style></head><body><main><p class="eyebrow">DEEDZ LABS / DEPLOYMENT HARNESS</p><h1>Infrastry <span>Gauntlet</span></h1><p class="lede">Evidence-driven testing for repository understanding, provisioning, deployment, verification, persistence, networking, databases, monitoring and recovery.</p><div class="actions"><button data-action="refresh">Run self-test</button><button data-action="snapshot">Save snapshot</button><button data-action="export">Export report</button></div><div id="clientState" class="client pending">Client diagnostics: loading browser harness…</div><section id="score"></section><h2>Continuity evidence</h2><p class="muted">Automatic identities and counters. No button clicks required.</p><div id="continuity" class="grid continuity"></div><h2>Live signals</h2><div id="signals" class="grid"></div><h2>Automated checks</h2><div id="tests"></div><h2>Manual probes <span class="muted">optional</span></h2><div class="split"><section><h2>Persistence marker</h2><p>Useful for notes, but automatic storage identity is the authoritative continuity test.</p><input id="marker" value="manual-marker"><button data-action="persist">Write marker</button><pre id="persistOut">No marker written.</pre></section><section><h2>WebSocket</h2><p id="wsState">Waiting for client…</p><input id="wsMsg" value="goose-check"><button data-action="ws">Send echo</button><pre id="wsOut">Waiting…</pre></section></div><div class="split"><section><h2>Database probe</h2><button data-action="db">Write database row</button><pre id="dbOut">Not tested.</pre></section><section><h2>Instance observation</h2><strong id="instancesCount" class="big">0</strong><p>unique instance IDs observed by this browser</p><pre id="instances"></pre></section></div><h2>Human feedback</h2><div class="notes"><textarea id="plan" placeholder="What did Infrastry detect in its launch plan?"></textarea><textarea id="intervention" placeholder="What required manual intervention?"></textarea><textarea id="recovery" placeholder="How did recovery/debugging behave?"></textarea><textarea id="cost" placeholder="What resources and costs were shown?"></textarea></div><h2>Recent persisted events</h2><pre id="events"></pre><footer>Built to produce evidence, not vibes. · v${VERSION}</footer></main><script src="/app.js" defer></script></body></html>`;
}

function css() {
  return `:root{color-scheme:dark;--bg:#090b10;--p:#111722;--line:#283345;--t:#f4f7fb;--m:#9aa7b8;--a:#9dfc61;--c:#6ae5ff;--w:#ffd166;--b:#ff6b7a}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 20% 0%,#102632,#090b10 35%);color:var(--t);font-family:system-ui,sans-serif}main{width:min(1180px,calc(100% - 28px));margin:auto;padding:48px 0}.eyebrow{color:var(--c);font-size:12px;letter-spacing:.15em;font-weight:800}h1{font-size:clamp(3rem,8vw,6.5rem);line-height:.9;letter-spacing:-.06em;margin:10px 0}h1 span{color:var(--a)}.lede{color:var(--m);max-width:850px;font-size:18px;line-height:1.6}.actions{display:flex;gap:8px;flex-wrap:wrap;margin:25px 0 14px}button,input,textarea{font:inherit;border-radius:10px;border:1px solid var(--line);background:#111722;color:var(--t);padding:10px 12px}button{cursor:pointer;font-weight:750}button:hover{border-color:var(--c)}input,textarea{width:100%;margin:6px 0 10px}textarea{min-height:100px}.client,.card,section,#tests>div,#score{background:var(--p);border:1px solid var(--line);border-radius:16px;padding:16px}.client{margin:0 0 24px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.continuity{margin-bottom:30px}.card strong{display:block}.card span,.muted{color:var(--m);font-size:13px}.card code{display:block;margin-top:5px;font-size:12px;color:var(--c);word-break:break-all}.pass{color:var(--a)}.warn,.pending{color:var(--w)}.fail{color:var(--b)}.skip{color:var(--m)}#tests{display:grid;gap:7px;margin-bottom:25px}#tests>div{display:flex;justify-content:space-between;gap:12px}#score{display:flex;gap:25px;margin-bottom:30px;min-height:54px}.split{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin:14px 0}.notes{display:grid;grid-template-columns:1fr 1fr;gap:10px}pre{white-space:pre-wrap;word-break:break-word;background:#080b10;padding:12px;border-radius:10px;color:#bcc8d6;max-height:320px;overflow:auto}.big{font-size:50px;color:var(--c)}footer{color:var(--m);padding:30px 0}@media(max-width:800px){.grid{grid-template-columns:1fr 1fr}.split,.notes{grid-template-columns:1fr}}@media(max-width:500px){.grid{grid-template-columns:1fr}}`;
}
