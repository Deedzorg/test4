import http from 'node:http';

const originalCreateServer = http.createServer;
const AUTOMATION_VERSION = '1.4.0';

http.createServer = function patchedCreateServer(listener, ...rest) {
  const wrapped = async (req, res) => {
    const url = new URL(req.url || '/', 'http://local');

    if (req.method === 'GET' && url.pathname === '/auto') {
      res.statusCode = 200;
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      res.setHeader('Cache-Control', 'no-store');
      res.setHeader('X-Content-Type-Options', 'nosniff');
      res.setHeader('Referrer-Policy', 'no-referrer');
      res.setHeader('Content-Security-Policy', "default-src 'self'; connect-src 'self' ws: wss:; style-src 'unsafe-inline'; script-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'");
      res.end(automationPage());
      return;
    }

    if (req.method === 'GET' && url.pathname === '/') {
      const originalEnd = res.end.bind(res);
      res.end = (chunk, encoding, callback) => {
        try {
          const text = Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk ?? '');
          if (text.includes('</header>') && !text.includes('/auto')) {
            const injected = text.replace(
              '</header>',
              '<div style="margin:12px 0 0"><a href="/auto" style="display:inline-block;padding:10px 14px;border:1px solid #456;border-radius:10px;color:#9dfc61;text-decoration:none;font-weight:800">Run automated tests →</a></div></header>'
            );
            res.removeHeader('Content-Length');
            return originalEnd(injected, encoding, callback);
          }
        } catch {}
        return originalEnd(chunk, encoding, callback);
      };
    }

    return listener(req, res);
  };

  return originalCreateServer.call(http, wrapped, ...rest);
};

await import('./server-v6.mjs');

function automationPage() {
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Infrastry Gauntlet Automation</title><style>${css()}</style></head><body><main>
  <a class="back" href="/">← Back to evidence dashboard</a>
  <p class="eyebrow">DEEDZ LABS / AUTOMATED ASSURANCE</p>
  <h1>Gauntlet <span>Auto Runner</span></h1>
  <p class="lede">You should not need to manually juggle correlation IDs. Enter the Gauntlet admin key once, then let the browser run the safe tests for you.</p>

  <section class="card hero">
    <label>Gauntlet admin key</label>
    <input id="key" type="password" autocomplete="off" placeholder="Enter once for this browser tab">
    <div class="buttons">
      <button class="primary" onclick="runSafeSuite()">Run safe automated suite</button>
      <button onclick="runReadiness()">Run 60-second readiness test</button>
    </div>
    <p class="note"><b>Safe suite:</b> WebSocket echo + stdout/stderr marker + HTTP 200/404/500 probes. It does not crash the app or change the database.</p>
  </section>

  <section class="card">
    <div class="row"><div><p class="eyebrow">SAFE SUITE</p><h2>Automated observability</h2></div><span id="safeState" class="pill">Ready</span></div>
    <pre id="safeOut">No safe suite has run in this tab.</pre>
    <div id="safeVerify" hidden class="verify">
      <h3>One final visual check</h3>
      <p>The app cannot read your private authenticated Infrastry dashboard. Open <b>Infrastry → More → Application logs</b> and search the suite/correlation IDs above. If the Runtime stdout/stderr marker and the Access 200/404/500 probes are visible with the correct statuses, click once:</p>
      <button class="primary" onclick="verifySafeSuite()">I can see all safe-suite markers</button>
      <pre id="verifyOut">Not verified.</pre>
    </div>
  </section>

  <section class="card">
    <div class="row"><div><p class="eyebrow">READINESS</p><h2>60-second readiness/liveness test</h2></div><span id="readyState" class="pill">Not running</span></div>
    <p class="note">The app stays alive at <code>/healthz = 200</code> while <code>/readyz = 503</code>. The runner samples both automatically for 60 seconds.</p>
    <pre id="readyOut">Not run.</pre>
    <div id="readyVerify" hidden class="verify">
      <p>After the 60-second window, look at Infrastry Monitoring/Application Logs. Choose what you actually observed:</p>
      <div class="buttons">
        <button onclick="verifyReadiness('pass')">Infrastry reacted to not-ready</button>
        <button onclick="verifyReadiness('fail')">Infrastry stayed liveness-only</button>
      </div>
      <pre id="readyVerifyOut">Not verified.</pre>
    </div>
  </section>

  <section class="card compact">
    <h2>Why one click is still manual</h2>
    <p>Gauntlet can generate and verify application-side behavior automatically. It cannot truthfully claim that a marker appeared inside your private Infrastry dashboard unless you confirm it or Infrastry exposes a logs API for the harness to query.</p>
  </section>

  <footer>Automation helper v${AUTOMATION_VERSION} · safe tests first · evidence before conclusions</footer>
  </main><script>${clientJs()}</script></body></html>`;
}

function clientJs() {
  return `let lastSuite=null,lastReadiness=null;const $=id=>document.getElementById(id);function keyHeaders(){const input=$('key').value||sessionStorage.getItem('gauntletAdminKey')||'';if(input)sessionStorage.setItem('gauntletAdminKey',input);return {'Content-Type':'application/json','x-gauntlet-key':input}}async function call(path,options={},acceptAny=false){const r=await fetch(path,{cache:'no-store',...options,headers:{'Content-Type':'application/json',...(options.headers||{})}});const text=await r.text();let body={};try{body=text?JSON.parse(text):{}}catch{body={raw:text}}if(!acceptAny&&!r.ok){const e=new Error(r.status+' '+path);e.body=body;throw e}return {status:r.status,body}}function uid(){return crypto.randomUUID()}function requireKey(){const h=keyHeaders();if(!h['x-gauntlet-key'])throw new Error('Enter the Gauntlet admin key first.');return h}function websocketEcho(suiteId,correlationId){return new Promise((resolve,reject)=>{const proto=location.protocol==='https:'?'wss:':'ws:';const ws=new WebSocket(proto+'//'+location.host+'/ws');const timer=setTimeout(()=>{try{ws.close()}catch{}reject(new Error('WebSocket echo timed out'))},7000);ws.onopen=()=>ws.send(JSON.stringify({suiteId,correlationId,value:'gauntlet-auto-echo'}));ws.onmessage=e=>{try{const m=JSON.parse(e.data);if(m.type==='echo'&&String(m.value||'').includes(correlationId)){clearTimeout(timer);ws.close();resolve({pass:true,correlationId,instanceId:m.instanceId,releaseId:m.releaseId})}}catch{}};ws.onerror=()=>{clearTimeout(timer);reject(new Error('WebSocket connection failed'))}})}async function runSafeSuite(){try{const h=requireKey();$('safeState').textContent='Running';$('safeOut').textContent='Starting safe suite…';$('safeVerify').hidden=true;const marker=(await call('/v13/log-marker',{method:'POST',headers:h,body:JSON.stringify({label:'gauntlet-auto-suite'})})).body;const suiteId=uid();const wsCorrelationId=uid();const websocket=await websocketEcho(suiteId,wsCorrelationId);const access=[];for(const status of [200,404,500]){const r=await call('/v13/access',{method:'POST',headers:h,body:JSON.stringify({status})},true);access.push({status,correlationId:r.body.correlationId,responseStatus:r.status})}lastSuite={suiteId,runtimeCorrelationId:marker.correlationId,websocket,access};sessionStorage.setItem('gauntletLastAutoSuite',JSON.stringify(lastSuite));$('safeOut').textContent=JSON.stringify({suite:'complete',suiteId,websocket,runtimeLog:{emitted:true,correlationId:marker.correlationId,platformVerification:'pending'},access:access.map(x=>({...x,platformVerification:'pending'}))},null,2);$('safeState').textContent='App-side complete';$('safeVerify').hidden=false}catch(e){$('safeState').textContent='Stopped';$('safeOut').textContent=JSON.stringify(e.body||{error:e.message},null,2)}}async function verifySafeSuite(){try{const h=requireKey();if(!lastSuite){const saved=sessionStorage.getItem('gauntletLastAutoSuite');if(saved)lastSuite=JSON.parse(saved)}if(!lastSuite)throw new Error('Run the safe suite first.');const rows=[];rows.push((await call('/v13/verify',{method:'POST',headers:h,body:JSON.stringify({testKey:'runtime-log-capture',verdict:'pass',correlationId:lastSuite.runtimeCorrelationId,note:'Auto Runner: user confirmed stdout and stderr marker visible in Infrastry Runtime logs.'})})).body);for(const item of lastSuite.access){rows.push((await call('/v13/verify',{method:'POST',headers:h,body:JSON.stringify({testKey:'access-logging',verdict:'pass',correlationId:item.correlationId,note:'Auto Runner: user confirmed HTTP '+item.status+' probe visible with correct status in Infrastry Access logs.'})})).body)}$('verifyOut').textContent=JSON.stringify({verified:true,records:rows.length,results:rows},null,2);$('safeState').textContent='Verified'}catch(e){$('verifyOut').textContent=JSON.stringify(e.body||{error:e.message},null,2)}}async function runReadiness(){try{const h=requireKey();$('readyState').textContent='Running 0/60';$('readyVerify').hidden=true;const start=(await call('/v13/readiness',{method:'POST',headers:h,body:JSON.stringify({action:'start',seconds:60})})).body;lastReadiness={correlationId:start.correlationId,samples:[]};for(let i=0;i<12;i++){await new Promise(r=>setTimeout(r,5000));const [health,ready]=await Promise.all([call('/healthz',{},true),call('/readyz',{},true)]);lastReadiness.samples.push({second:(i+1)*5,health:health.status,ready:ready.status});$('readyState').textContent='Running '+((i+1)*5)+'/60';$('readyOut').textContent=JSON.stringify({correlationId:start.correlationId,lastSample:lastReadiness.samples.at(-1)},null,2)}await call('/v13/readiness',{method:'POST',headers:h,body:JSON.stringify({action:'stop'})},true);lastReadiness.appVerdict=lastReadiness.samples.every(x=>x.health===200&&x.ready===503)?'pass':'mixed';sessionStorage.setItem('gauntletLastReadiness',JSON.stringify(lastReadiness));$('readyOut').textContent=JSON.stringify({complete:true,correlationId:start.correlationId,applicationEndpointResult:lastReadiness.appVerdict,samples:lastReadiness.samples,platformResponse:'needs one visual confirmation in Infrastry'},null,2);$('readyState').textContent='App-side complete';$('readyVerify').hidden=false}catch(e){$('readyState').textContent='Stopped';$('readyOut').textContent=JSON.stringify(e.body||{error:e.message},null,2)}}async function verifyReadiness(verdict){try{const h=requireKey();if(!lastReadiness){const saved=sessionStorage.getItem('gauntletLastReadiness');if(saved)lastReadiness=JSON.parse(saved)}if(!lastReadiness)throw new Error('Run the readiness test first.');const note=verdict==='pass'?'Auto Runner: user observed Infrastry reacting to readiness=503 while liveness remained 200.':'Auto Runner: user observed Infrastry remaining healthy/liveness-only while readiness=503.';const r=(await call('/v13/verify',{method:'POST',headers:h,body:JSON.stringify({testKey:'readiness-platform',verdict,correlationId:lastReadiness.correlationId,note})})).body;$('readyVerifyOut').textContent=JSON.stringify(r,null,2);$('readyState').textContent='Verified'}catch(e){$('readyVerifyOut').textContent=JSON.stringify(e.body||{error:e.message},null,2)}}const savedKey=sessionStorage.getItem('gauntletAdminKey');if(savedKey)$('key').value=savedKey;const savedSuite=sessionStorage.getItem('gauntletLastAutoSuite');if(savedSuite){try{lastSuite=JSON.parse(savedSuite);$('safeVerify').hidden=false}catch{}}const savedReady=sessionStorage.getItem('gauntletLastReadiness');if(savedReady){try{lastReadiness=JSON.parse(savedReady);$('readyVerify').hidden=false}catch{}}`;
}

function css() {
  return `:root{color-scheme:dark;--bg:#080b10;--panel:#111722;--line:#283345;--text:#f4f7fb;--muted:#9aa7b8;--lime:#9dfc61;--cyan:#6ae5ff;--amber:#ffd166;--red:#ff6b7a}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 18% 0%,#102632,var(--bg) 38%);color:var(--text);font-family:system-ui,sans-serif}main{width:min(1040px,calc(100% - 28px));margin:auto;padding:38px 0}.back{color:var(--muted);text-decoration:none}.eyebrow{color:var(--cyan);font-size:12px;letter-spacing:.15em;font-weight:850;margin:18px 0 6px}h1{font-size:clamp(3rem,8vw,5.5rem);line-height:.92;letter-spacing:-.055em;margin:8px 0}h1 span{color:var(--lime)}h2{margin:0}.lede,.note{color:var(--muted);line-height:1.6}.card{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:18px;margin:16px 0}.hero{border-color:#3d556d}.compact{padding-bottom:8px}.row{display:flex;justify-content:space-between;gap:18px;align-items:start}.pill{border:1px solid var(--line);border-radius:999px;padding:6px 10px;color:var(--amber);white-space:nowrap}.buttons{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}input,button{font:inherit;border-radius:10px;border:1px solid var(--line);background:#101621;color:var(--text);padding:11px 12px}input{width:100%;margin:7px 0}.primary{border-color:#52734d;background:#1d3320;color:var(--lime)}button{cursor:pointer;font-weight:800}.verify{margin-top:14px;padding:14px;border:1px solid #5c5530;background:#17150d;border-radius:12px}pre{white-space:pre-wrap;word-break:break-word;background:#070a0f;padding:13px;border-radius:10px;color:#c0ccda;max-height:390px;overflow:auto}code{color:var(--cyan)}footer{color:var(--muted);padding:24px 0}@media(max-width:620px){.row{display:block}.pill{display:inline-block;margin-top:8px}.buttons button{width:100%}}`;
}
