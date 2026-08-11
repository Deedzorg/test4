(() => {
  'use strict';
  const $ = (id) => document.getElementById(id);
  const els = Object.fromEntries(['clientState','score','signals','tests','events','marker','persistOut','wsState','wsMsg','wsOut','dbOut','instancesCount','instances','plan','intervention','recovery','cost'].map((id) => [id, $(id)]));
  let status = null;
  let tests = [];
  let events = [];
  let ws = null;
  const instanceIds = new Set(JSON.parse(localStorage.getItem('gauntlet.instanceIds') || '[]'));

  window.addEventListener('error', (event) => setClient(`Client error: ${event.message}`, 'fail'));
  window.addEventListener('unhandledrejection', (event) => setClient(`Client promise error: ${String(event.reason)}`, 'fail'));

  for (const id of ['plan','intervention','recovery','cost']) {
    els[id].value = localStorage.getItem(`gauntlet.${id}`) || '';
    els[id].addEventListener('input', () => localStorage.setItem(`gauntlet.${id}`, els[id].value));
  }

  document.querySelector('[data-action="refresh"]').addEventListener('click', () => refreshAll());
  document.querySelector('[data-action="snapshot"]').addEventListener('click', () => snapshot());
  document.querySelector('[data-action="export"]').addEventListener('click', () => exportReport());
  document.querySelector('[data-action="persist"]').addEventListener('click', () => persist());
  document.querySelector('[data-action="ws"]').addEventListener('click', () => wsSend());
  document.querySelector('[data-action="db"]').addEventListener('click', () => dbProbe());

  function setClient(message, state = 'pass') {
    els.clientState.className = `client ${state}`;
    els.clientState.textContent = message;
  }

  async function api(path, options = {}) {
    const response = await fetch(path, { headers: { 'Content-Type': 'application/json' }, ...options });
    const value = await response.json();
    if (!response.ok) throw new Error(`${response.status} ${JSON.stringify(value)}`);
    return value;
  }

  async function refreshAll() {
    try {
      [status, tests, events] = await Promise.all([
        api('/api/status'),
        api('/api/tests').then((v) => v.tests),
        api('/api/events').then((v) => v.events)
      ]);
      if (status.instance?.id) rememberInstance(status.instance.id);
      render();
      setClient(`Browser harness active · API reachable · v${status.version}`, 'pass');
    } catch (error) {
      setClient(`Browser harness loaded, API refresh failed: ${error.message}`, 'fail');
    }
  }

  function render() {
    if (!status) return;
    const cards = [
      ['Runtime', 'pass', status.instance.node],
      ['Storage', status.persistence.storage.ok ? 'pass' : 'fail', status.persistence.storage.detail],
      ['Environment', status.environment.sentinelPresent ? 'pass' : 'warn', status.environment.sentinelPresent ? 'sentinel detected' : 'sentinel missing'],
      ['Database', status.database.configured ? (status.database.ok ? 'pass' : 'fail') : 'skip', status.database.detail],
      ['HTTPS', status.proxy.httpsObserved ? 'pass' : 'warn', status.proxy.forwardedProto || 'not observed'],
      ['Background', status.background.lastAt ? 'pass' : 'pending', status.background.lastAt || 'waiting'],
      ['Boots', status.persistence.bootCount > 1 ? 'pass' : 'pending', String(status.persistence.bootCount)],
      ['Instance', 'pass', status.instance.id.slice(0, 8)]
    ];
    els.signals.innerHTML = cards.map(([name, cls, detail]) => `<div class="card"><strong>${escapeHtml(name)}</strong><b class="${cls}">${cls}</b><br><span>${escapeHtml(String(detail))}</span></div>`).join('');
    els.tests.innerHTML = tests.map((t) => `<div><span>${escapeHtml(t.id)} — ${escapeHtml(String(t.detail))}</span><b class="${t.status}">${t.status}</b></div>`).join('');
    els.score.innerHTML = `<b class="pass">PASS ${tests.filter((x) => x.status === 'pass').length}</b><b class="pending">PENDING/SKIP ${tests.filter((x) => ['pending','skip'].includes(x.status)).length}</b><b class="fail">WARN/FAIL ${tests.filter((x) => ['warn','fail'].includes(x.status)).length}</b>`;
    els.events.textContent = events.map((e) => `${e.at}  ${e.type}  ${JSON.stringify(e)}`).join('\n');
    els.instancesCount.textContent = String(instanceIds.size);
    els.instances.textContent = [...instanceIds].join('\n');
  }

  function rememberInstance(id) {
    instanceIds.add(id);
    localStorage.setItem('gauntlet.instanceIds', JSON.stringify([...instanceIds]));
  }

  async function persist() {
    try {
      const result = await api('/api/persist', { method: 'POST', body: JSON.stringify({ marker: els.marker.value }) });
      els.persistOut.textContent = JSON.stringify(result, null, 2);
      await refreshAll();
    } catch (error) { els.persistOut.textContent = error.message; }
  }

  async function dbProbe() {
    try { els.dbOut.textContent = JSON.stringify(await api('/api/database/probe', { method: 'POST', body: '{}' }), null, 2); }
    catch (error) { els.dbOut.textContent = error.message; }
    await refreshAll();
  }

  async function snapshot() {
    try {
      const result = await api('/api/snapshot', { method: 'POST', body: '{}' });
      setClient(`Snapshot ${result.snapshot} saved · browser harness active`, 'pass');
      await refreshAll();
    } catch (error) { setClient(`Snapshot failed: ${error.message}`, 'fail'); }
  }

  function connectWebSocket() {
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    els.wsState.textContent = 'connecting…';
    try { ws = new WebSocket(`${protocol}//${location.host}/ws`); }
    catch (error) { els.wsState.textContent = `constructor failed: ${error.message}`; return; }
    ws.onopen = () => { els.wsState.textContent = 'connected'; };
    ws.onmessage = (event) => {
      els.wsOut.textContent = event.data;
      try { const message = JSON.parse(event.data); if (message.instanceId) { rememberInstance(message.instanceId); render(); } } catch {}
    };
    ws.onerror = () => { els.wsState.textContent = 'error'; };
    ws.onclose = () => { els.wsState.textContent = 'reconnecting…'; setTimeout(connectWebSocket, 2000); };
  }

  function wsSend() {
    if (ws?.readyState === WebSocket.OPEN) ws.send(els.wsMsg.value);
    else els.wsOut.textContent = `WebSocket not open (state ${ws?.readyState ?? 'none'})`;
  }

  function exportReport() {
    const report = { generatedAt: new Date().toISOString(), url: location.href, userAgent: navigator.userAgent, status, tests, events, instances: [...instanceIds], notes: Object.fromEntries(['plan','intervention','recovery','cost'].map((id) => [id, els[id].value])) };
    const url = URL.createObjectURL(new Blob([JSON.stringify(report, null, 2)], { type: 'application/json' }));
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = 'infrastry-gauntlet-report.json';
    anchor.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  function escapeHtml(value) {
    return value.replace(/[&<>"']/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
  }

  setClient('Browser harness loaded · checking APIs…', 'pending');
  connectWebSocket();
  refreshAll();
  setInterval(refreshAll, 15000);
})();
