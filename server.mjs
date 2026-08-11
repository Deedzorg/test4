import http from 'node:http';
import { appendFile, mkdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { randomUUID, timingSafeEqual } from 'node:crypto';
import { hostname } from 'node:os';
import { resolve, join } from 'node:path';
import pg from 'pg';
import { WebSocketServer } from 'ws';

const { Pool } = pg;
const PORT = Number.parseInt(process.env.PORT || '3000', 10);
const HOST = process.env.HOST || '0.0.0.0';
const DATA_DIR = resolve(process.env.DATA_DIR || '.data');
const STATE_FILE = join(DATA_DIR, 'state.json');
const EVENTS_FILE = join(DATA_DIR, 'events.ndjson');
const DATABASE_URL = process.env.DATABASE_URL || '';
const REQUIRE_DATABASE = /^true$/i.test(process.env.REQUIRE_DATABASE || 'false');
const CHAOS_ENABLED = /^true$/i.test(process.env.CHAOS_ENABLED || 'false');
const ADMIN_KEY = process.env.GAUNTLET_ADMIN_KEY || '';
const SENTINEL_PRESENT = Boolean(process.env.GAUNTLET_SENTINEL);
const INSTANCE_ID = randomUUID();
const STARTED_AT = new Date().toISOString();

let pool = null;
let storage = { ok: false, detail: 'initializing' };
let database = { configured: Boolean(DATABASE_URL), required: REQUIRE_DATABASE, ok: !REQUIRE_DATABASE && !DATABASE_URL, detail: DATABASE_URL ? 'initializing' : 'not configured' };
let state = { schemaVersion: 1, bootCount: 0, persistentWrites: 0, databaseWrites: 0, snapshots: 0 };
let requests = 0;
let wsConnections = 0;
let heartbeat = { count: 0, lastAt: null };

await initialize();

const server = http.createServer(async (req, res) => {
  requests++;
  setHeaders(res);
  try { await route(req, res); }
  catch (error) { console.error(JSON.stringify({ at:new Date().toISOString(), level:'error', event:'request.failed', path:req.url, message:error.message })); json(res, 500, { ok:false, error:'internal_error' }); }
});

const wss = new WebSocketServer({ noServer: true });
wss.on('connection', (ws) => {
  wsConnections++;
  ws.send(JSON.stringify({ type:'hello', instanceId:INSTANCE_ID, bootCount:state.bootCount, at:new Date().toISOString() }));
  ws.on('message', (raw) => ws.send(JSON.stringify({ type:'echo', value:raw.toString().slice(0,2048), instanceId:INSTANCE_ID, at:new Date().toISOString() })));
});
server.on('upgrade', (req, socket, head) => {
  if (new URL(req.url, 'http://local').pathname !== '/ws') return socket.destroy();
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});
server.listen(PORT, HOST, () => log('server.listening', { host:HOST, port:PORT, instanceId:INSTANCE_ID, node:process.version }));

const heart = setInterval(() => { heartbeat = { count:heartbeat.count + 1, lastAt:new Date().toISOString() }; if (heartbeat.count % 4 === 0) event('background.heartbeat', heartbeat).catch(()=>{}); }, 15000);
heart.unref();
for (const sig of ['SIGTERM','SIGINT']) process.on(sig, () => shutdown(sig));

async function initialize() {
  await mkdir(DATA_DIR, { recursive:true });
  try { state = { ...state, ...JSON.parse(await readFile(STATE_FILE, 'utf8')) }; } catch (e) { if (e.code !== 'ENOENT') throw e; }
  state.bootCount++;
  state.lastBootAt = STARTED_AT;
  state.lastInstanceId = INSTANCE_ID;
  await save();
  const probe = join(DATA_DIR, `.probe-${INSTANCE_ID}`);
  try { await writeFile(probe, 'ok'); storage = { ok:(await readFile(probe,'utf8')) === 'ok', detail:'read/write verified' }; await rm(probe,{force:true}); }
  catch (e) { storage = { ok:false, detail:e.message }; }
  if (DATABASE_URL) {
    try {
      pool = new Pool({ connectionString:DATABASE_URL, max:3, connectionTimeoutMillis:5000 });
      await pool.query('CREATE TABLE IF NOT EXISTS gauntlet_probe (id BIGSERIAL PRIMARY KEY, instance_id TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())');
      database = { configured:true, required:REQUIRE_DATABASE, ok:true, detail:'connected' };
    } catch (e) { database = { configured:true, required:REQUIRE_DATABASE, ok:false, detail:e.message }; }
  } else database = { configured:false, required:REQUIRE_DATABASE, ok:!REQUIRE_DATABASE, detail:REQUIRE_DATABASE ? 'DATABASE_URL missing' : 'not configured' };
  await event('process.boot', { bootCount:state.bootCount, storageOk:storage.ok, database });
}

async function route(req, res) {
  const u = new URL(req.url, 'http://local');
  if (req.method === 'GET' && u.pathname === '/') return html(res, page());
  if (req.method === 'GET' && u.pathname === '/healthz') return json(res,200,{ok:true,service:'infrastry-gauntlet',instanceId:INSTANCE_ID});
  if (req.method === 'GET' && u.pathname === '/readyz') { const ok=storage.ok&&(!REQUIRE_DATABASE||database.ok); return json(res,ok?200:503,{ok,storage,database}); }
  if (req.method === 'GET' && u.pathname === '/api/status') return json(res,200,status(req));
  if (req.method === 'GET' && u.pathname === '/api/tests') return json(res,200,{ok:true,tests:tests(req)});
  if (req.method === 'GET' && u.pathname === '/api/events') return json(res,200,{ok:true,events:await recentEvents()});
  if (req.method === 'GET' && u.pathname === '/metrics') return text(res,200,`gauntlet_boot_count ${state.bootCount}\ngauntlet_persistent_writes_total ${state.persistentWrites}\ngauntlet_requests_total ${requests}\ngauntlet_active_websockets ${wss.clients.size}\n`);
  if (req.method === 'POST' && u.pathname === '/api/persist') { const body=await bodyJson(req); state.persistentWrites++; state.lastMarker=String(body.marker||`marker-${state.persistentWrites}`).slice(0,160); state.lastWriteAt=new Date().toISOString(); await save(); await event('storage.persist',{marker:state.lastMarker}); return json(res,201,{ok:true,marker:state.lastMarker,persistentWrites:state.persistentWrites,bootCount:state.bootCount}); }
  if (req.method === 'POST' && u.pathname === '/api/database/probe') { if(!pool||!database.ok)return json(res,409,{ok:false,error:'database_not_available',database}); const r=await pool.query('INSERT INTO gauntlet_probe(instance_id) VALUES($1) RETURNING id, created_at',[INSTANCE_ID]); state.databaseWrites++; await save(); return json(res,201,{ok:true,row:r.rows[0],databaseWrites:state.databaseWrites}); }
  if (req.method === 'POST' && u.pathname === '/api/snapshot') { state.snapshots++; await save(); await event('evidence.snapshot',{snapshot:state.snapshots,status:status(req),tests:tests(req)}); return json(res,201,{ok:true,snapshot:state.snapshots}); }
  if (req.method === 'POST' && u.pathname === '/api/chaos/crash') { if(!authorized(req))return json(res,403,{ok:false,error:'chaos_disabled_or_unauthorized'}); await event('chaos.crash_requested',{}); json(res,202,{ok:true,message:'exit scheduled'}); setTimeout(()=>process.exit(77),250).unref(); return; }
  return json(res,404,{ok:false,error:'not_found'});
}

function status(req) {
  const proto = String(req.headers['x-forwarded-proto']||'').split(',')[0].trim();
  return { ok:storage.ok&&(!REQUIRE_DATABASE||database.ok), service:'Infrastry Gauntlet', version:'1.0.0', instance:{ id:INSTANCE_ID, host:hostname(), pid:process.pid, node:process.version, startedAt:STARTED_AT, uptimeSeconds:Math.round(process.uptime()), rssBytes:process.memoryUsage().rss }, persistence:{ storage, bootCount:state.bootCount, persistentWrites:state.persistentWrites, lastMarker:state.lastMarker||null, lastWriteAt:state.lastWriteAt||null, lastGracefulShutdownAt:state.lastGracefulShutdownAt||null }, database:{...database,writes:state.databaseWrites}, environment:{ portProvided:Boolean(process.env.PORT), dataDirProvided:Boolean(process.env.DATA_DIR), sentinelPresent:SENTINEL_PRESENT, databaseUrlPresent:Boolean(DATABASE_URL), requireDatabase:REQUIRE_DATABASE, chaosEnabled:CHAOS_ENABLED, nodeEnv:process.env.NODE_ENV||null }, proxy:{ forwardedProto:proto||null, httpsObserved:proto==='https'||Boolean(req.socket.encrypted) }, background:heartbeat, traffic:{ requests, websocketConnections:wsConnections, activeWebSockets:wss.clients.size }, snapshots:state.snapshots };
}
function tests(req) {
  const s=status(req), age=heartbeat.lastAt?Math.round((Date.now()-Date.parse(heartbeat.lastAt))/1000):null;
  return [
    t('runtime','pass','HTTP process is running'),
    t('dynamic-port',process.env.PORT?'pass':'warn',process.env.PORT?'PORT supplied by environment':'using local fallback port'),
    t('health','pass','/healthz and /readyz available'),
    t('environment',SENTINEL_PRESENT?'pass':'warn',SENTINEL_PRESENT?'GAUNTLET_SENTINEL detected':'set GAUNTLET_SENTINEL to prove injection'),
    t('storage-write',storage.ok?'pass':'fail',storage.detail),
    t('storage-persistence',state.bootCount>1?'pass':'pending',`bootCount=${state.bootCount}, writes=${state.persistentWrites}`),
    t('websocket','pass','upgrade endpoint available at /ws'),
    t('background-work',age===null?'pending':age<=45?'pass':'fail',age===null?'waiting for first heartbeat':`heartbeat ${age}s ago`),
    t('database',database.configured?(database.ok?'pass':'fail'):(REQUIRE_DATABASE?'fail':'skip'),database.detail),
    t('https-proxy',s.proxy.httpsObserved?'pass':'warn',s.proxy.httpsObserved?'HTTPS observed':'no HTTPS forwarding signal on this request'),
    t('restart-recovery',state.bootCount>1?'pass':'pending',`observed ${state.bootCount} boot(s)`),
    t('graceful-shutdown',state.lastGracefulShutdownAt?'pass':'pending',state.lastGracefulShutdownAt||'not observed yet')
  ];
}
function t(id,status,detail){return{id,status,passed:status==='pass',detail};}
async function bodyJson(req){const chunks=[];let n=0;for await(const c of req){n+=c.length;if(n>32768)throw new Error('body_too_large');chunks.push(c);}return chunks.length?JSON.parse(Buffer.concat(chunks).toString('utf8')):{};}
async function save(){const tmp=`${STATE_FILE}.tmp`;await writeFile(tmp,JSON.stringify(state,null,2));await rename(tmp,STATE_FILE);}
async function event(type,data){const e={at:new Date().toISOString(),type,instanceId:INSTANCE_ID,...data};await appendFile(EVENTS_FILE,`${JSON.stringify(e)}\n`);log(type,data);}
async function recentEvents(){try{return (await readFile(EVENTS_FILE,'utf8')).trim().split('\n').filter(Boolean).slice(-50).map(JSON.parse).reverse();}catch(e){if(e.code==='ENOENT')return[];throw e;}}
function authorized(req){if(!CHAOS_ENABLED||!ADMIN_KEY)return false;const a=Buffer.from(String(req.headers['x-gauntlet-key']||'')),b=Buffer.from(ADMIN_KEY);return a.length===b.length&&timingSafeEqual(a,b);}
function setHeaders(res){res.setHeader('X-Content-Type-Options','nosniff');res.setHeader('Referrer-Policy','no-referrer');res.setHeader('Permissions-Policy','camera=(), microphone=(), geolocation=()');res.setHeader('Content-Security-Policy',"default-src 'self'; connect-src 'self' ws: wss:; style-src 'unsafe-inline'; script-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'");}
function json(res,code,v){res.statusCode=code;res.setHeader('Content-Type','application/json; charset=utf-8');res.setHeader('Cache-Control','no-store');res.end(JSON.stringify(v));}
function text(res,code,v){res.statusCode=code;res.setHeader('Content-Type','text/plain; charset=utf-8');res.end(v);}
function html(res,v){res.statusCode=200;res.setHeader('Content-Type','text/html; charset=utf-8');res.end(v);}
function log(event,data={}){console.log(JSON.stringify({at:new Date().toISOString(),level:'info',event,service:'infrastry-gauntlet',instanceId:INSTANCE_ID,...data}));}
async function shutdown(signal){clearInterval(heart);state.lastGracefulShutdownAt=new Date().toISOString();await save().catch(()=>{});await event('process.shutdown',{signal}).catch(()=>{});for(const c of wss.clients)c.close(1001,'shutdown');await new Promise(r=>server.close(r));if(pool)await pool.end().catch(()=>{});process.exit(0);}

function page(){return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Infrastry Gauntlet</title><style>${css()}</style></head><body><main><p class="eyebrow">DEEDZ LABS / DEPLOYMENT HARNESS</p><h1>Infrastry <span>Gauntlet</span></h1><p class="lede">Evidence-driven testing for repository understanding, provisioning, deployment, verification, persistence, networking, databases, monitoring and recovery.</p><div class="actions"><button onclick="refreshAll()">Run self-test</button><button onclick="snapshot()">Save snapshot</button><button onclick="exportReport()">Export report</button></div><section id="score"></section><h2>Live signals</h2><div id="signals" class="grid"></div><h2>Automated checks</h2><div id="tests"></div><div class="split"><section><h2>Persistence</h2><p>Write a marker, redeploy or restart, then verify it survives.</p><input id="marker" placeholder="before-redeploy-01"><button onclick="persist()">Write marker</button><pre id="persistOut">No marker written.</pre></section><section><h2>WebSocket</h2><p id="wsState">Connecting…</p><input id="wsMsg" value="goose-check"><button onclick="wsSend()">Send echo</button><pre id="wsOut">Waiting…</pre></section></div><div class="split"><section><h2>Database probe</h2><button onclick="dbProbe()">Write database row</button><pre id="dbOut">Not tested.</pre></section><section><h2>Instance observation</h2><strong id="instancesCount" class="big">0</strong><p>unique instance IDs observed by this browser</p><pre id="instances"></pre></section></div><h2>Human feedback</h2><div class="notes"><textarea id="plan" placeholder="What did Infrastry detect in its launch plan?"></textarea><textarea id="intervention" placeholder="What required manual intervention?"></textarea><textarea id="recovery" placeholder="How did recovery/debugging behave?"></textarea><textarea id="cost" placeholder="What resources and costs were shown?"></textarea></div><h2>Recent persisted events</h2><pre id="events"></pre><footer>Built to produce evidence, not vibes.</footer></main><script>${client()}</script></body></html>`;}
function css(){return `:root{color-scheme:dark;--bg:#090b10;--p:#111722;--line:#283345;--t:#f4f7fb;--m:#9aa7b8;--a:#9dfc61;--c:#6ae5ff;--w:#ffd166;--b:#ff6b7a}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 20% 0%,#102632,#090b10 35%);color:var(--t);font-family:system-ui,sans-serif}main{width:min(1100px,calc(100% - 28px));margin:auto;padding:48px 0}.eyebrow{color:var(--c);font-size:12px;letter-spacing:.15em;font-weight:800}h1{font-size:clamp(3rem,8vw,6.5rem);line-height:.9;letter-spacing:-.06em;margin:10px 0}h1 span{color:var(--a)}.lede{color:var(--m);max-width:800px;font-size:18px;line-height:1.6}.actions{display:flex;gap:8px;flex-wrap:wrap;margin:25px 0 35px}button,input,textarea{font:inherit;border-radius:10px;border:1px solid var(--line);background:#111722;color:var(--t);padding:10px 12px}button{cursor:pointer;font-weight:750}button:hover{border-color:var(--c)}input,textarea{width:100%;margin:6px 0 10px}textarea{min-height:100px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.card,section,#tests>div,#score{background:var(--p);border:1px solid var(--line);border-radius:16px;padding:16px}.card strong{display:block}.card span,.muted{color:var(--m);font-size:13px}.pass{color:var(--a)}.warn,.pending{color:var(--w)}.fail{color:var(--b)}.skip{color:var(--m)}#tests{display:grid;gap:7px;margin-bottom:25px}#tests>div{display:flex;justify-content:space-between;gap:12px}#score{display:flex;gap:25px;margin-bottom:30px}.split{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin:14px 0}.notes{display:grid;grid-template-columns:1fr 1fr;gap:10px}pre{white-space:pre-wrap;word-break:break-word;background:#080b10;padding:12px;border-radius:10px;color:#bcc8d6;max-height:320px;overflow:auto}.big{font-size:50px;color:var(--c)}footer{color:var(--m);padding:30px 0}@media(max-width:800px){.grid{grid-template-columns:1fr 1fr}.split,.notes{grid-template-columns:1fr}}@media(max-width:500px){.grid{grid-template-columns:1fr}}`;}
function client(){return `let S=null,T=[],E=[],ws;const ids=new Set(JSON.parse(localStorage.getItem('g.ids')||'[]'));for(const id of ['plan','intervention','recovery','cost']){const e=document.getElementById(id);e.value=localStorage.getItem('g.'+id)||'';e.oninput=()=>localStorage.setItem('g.'+id,e.value)}async function api(p,o={}){const r=await fetch(p,{headers:{'Content-Type':'application/json'},...o}),v=await r.json();if(!r.ok)throw Error(r.status+' '+JSON.stringify(v));return v}async function refreshAll(){S=await api('/api/status');T=(await api('/api/tests')).tests;E=(await api('/api/events')).events;if(S.instance?.id){ids.add(S.instance.id);localStorage.setItem('g.ids',JSON.stringify([...ids]))}render()}function render(){signals.innerHTML=[['Runtime','pass',S.instance.node],['Storage',S.persistence.storage.ok?'pass':'fail',S.persistence.storage.detail],['Environment',S.environment.sentinelPresent?'pass':'warn',S.environment.sentinelPresent?'sentinel detected':'sentinel missing'],['Database',S.database.configured?(S.database.ok?'pass':'fail'):'skip',S.database.detail],['HTTPS',S.proxy.httpsObserved?'pass':'warn',S.proxy.forwardedProto||'not observed'],['Background',S.background.lastAt?'pass':'pending',S.background.lastAt||'waiting'],['Boots',S.persistence.bootCount>1?'pass':'pending',String(S.persistence.bootCount)],['Instance','pass',S.instance.id.slice(0,8)]].map(x=>'<div class="card"><strong>'+x[0]+'</strong><b class="'+x[1]+'">'+x[1]+'</b><br><span>'+x[2]+'</span></div>').join('');tests.innerHTML=T.map(t=>'<div><span>'+t.id+' — '+t.detail+'</span><b class="'+t.status+'">'+t.status+'</b></div>').join('');score.innerHTML='<b class="pass">PASS '+T.filter(x=>x.status==='pass').length+'</b><b class="pending">PENDING/SKIP '+T.filter(x=>['pending','skip'].includes(x.status)).length+'</b><b class="fail">WARN/FAIL '+T.filter(x=>['warn','fail'].includes(x.status)).length+'</b>';events.textContent=E.map(e=>e.at+'  '+e.type+'  '+JSON.stringify(e)).join('\n');instancesCount.textContent=ids.size;instances.textContent=[...ids].join('\n')}async function persist(){persistOut.textContent=JSON.stringify(await api('/api/persist',{method:'POST',body:JSON.stringify({marker:marker.value})}),null,2);refreshAll()}async function dbProbe(){try{dbOut.textContent=JSON.stringify(await api('/api/database/probe',{method:'POST',body:'{}'}),null,2)}catch(e){dbOut.textContent=e.message}refreshAll()}async function snapshot(){alert('Snapshot '+(await api('/api/snapshot',{method:'POST',body:'{}'})).snapshot+' saved');refreshAll()}function connect(){const p=location.protocol==='https:'?'wss:':'ws:';ws=new WebSocket(p+'//'+location.host+'/ws');ws.onopen=()=>wsState.textContent='connected';ws.onmessage=e=>{wsOut.textContent=e.data;try{const m=JSON.parse(e.data);if(m.instanceId){ids.add(m.instanceId);localStorage.setItem('g.ids',JSON.stringify([...ids]));render()}}catch{}};ws.onclose=()=>{wsState.textContent='reconnecting';setTimeout(connect,2000)}}function wsSend(){if(ws?.readyState===1)ws.send(wsMsg.value)}function exportReport(){const n={generatedAt:new Date().toISOString(),url:location.href,userAgent:navigator.userAgent,status:S,tests:T,events:E,instances:[...ids],notes:Object.fromEntries(['plan','intervention','recovery','cost'].map(id=>[id,document.getElementById(id).value]))};const b=new Blob([JSON.stringify(n,null,2)],{type:'application/json'}),u=URL.createObjectURL(b),a=document.createElement('a');a.href=u;a.download='infrastry-gauntlet-report.json';a.click();URL.revokeObjectURL(u)}connect();refreshAll();setInterval(refreshAll,15000);`;}
