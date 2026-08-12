import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import net from 'node:net';
import { WebSocket } from 'ws';

const ADMIN_KEY = 'gauntlet-ci-admin-key';

async function portFree(port) {
  return new Promise((resolve) => {
    const server = net.createServer();
    server.once('error', () => resolve(false));
    server.listen(port, '127.0.0.1', () => server.close(() => resolve(true)));
  });
}

async function openPortPair() {
  for (let i = 0; i < 40; i++) {
    const port = 22000 + Math.floor(Math.random() * 25000);
    if (port + 137 > 65535) continue;
    if (await portFree(port) && await portFree(port + 137)) return port;
  }
  throw new Error('could not find a free public/internal port pair');
}

async function waitFor(url, timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  let lastError;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url, { cache: 'no-store' });
      if (response.ok) return response;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw lastError || new Error(`timed out waiting for ${url}`);
}

async function post(base, path, body, withKey = true) {
  return fetch(base + path, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(withKey ? { 'x-gauntlet-key': ADMIN_KEY } : {})
    },
    body: JSON.stringify(body)
  });
}

function websocketEcho(url, correlationId) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    const timer = setTimeout(() => {
      ws.terminate();
      reject(new Error('websocket echo timed out'));
    }, 7000);

    ws.once('open', () => {
      ws.send(JSON.stringify({ correlationId, value: 'ci-echo' }));
    });

    ws.on('message', (raw) => {
      try {
        const message = JSON.parse(raw.toString());
        if (message.type === 'echo' && String(message.value).includes(correlationId)) {
          clearTimeout(timer);
          ws.close();
          resolve(message);
        }
      } catch {}
    });

    ws.once('error', (error) => {
      clearTimeout(timer);
      reject(error);
    });
  });
}

test('v1.4 Auto Runner black-box suite', { timeout: 45000 }, async () => {
  const port = await openPortPair();
  const base = `http://127.0.0.1:${port}`;
  const dataDir = await mkdtemp(join(tmpdir(), 'infrastry-gauntlet-ci-'));
  let stdout = '';
  let stderr = '';

  const child = spawn(process.execPath, ['server-v8.mjs'], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      PORT: String(port),
      HOST: '127.0.0.1',
      DATA_DIR: dataDir,
      DATABASE_URL: '',
      REQUIRE_DATABASE: 'false',
      CHAOS_ENABLED: 'false',
      GAUNTLET_ADMIN_KEY: ADMIN_KEY,
      GAUNTLET_SENTINEL: 'ci-sentinel'
    },
    stdio: ['ignore', 'pipe', 'pipe']
  });

  child.stdout.on('data', (chunk) => { stdout += chunk.toString(); });
  child.stderr.on('data', (chunk) => { stderr += chunk.toString(); });

  try {
    await waitFor(base + '/v13/status');

    const auto = await fetch(base + '/auto');
    assert.equal(auto.status, 200);
    const autoHtml = await auto.text();
    assert.match(autoHtml, /Run safe automated suite/);
    assert.match(autoHtml, /seconds:65/, 'readiness override must outlive the full 60s sampling window');

    const unauthorized = await post(base, '/v13/log-marker', { label: 'ci-unauthorized' }, false);
    assert.equal(unauthorized.status, 403);

    const marker = await post(base, '/v13/log-marker', { label: 'ci-marker' });
    assert.equal(marker.status, 201);
    const markerBody = await marker.json();
    assert.ok(markerBody.correlationId);

    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.match(stdout, new RegExp(markerBody.correlationId));
    assert.match(stderr, new RegExp(markerBody.correlationId));

    for (const status of [200, 404, 500]) {
      const response = await post(base, '/v13/access', { status });
      assert.equal(response.status, status);
      const body = await response.json();
      assert.equal(body.status, status);
      assert.ok(body.correlationId);
    }

    const wsCorrelation = `ws-${Date.now()}`;
    const echoed = await websocketEcho(`ws://127.0.0.1:${port}/ws`, wsCorrelation);
    assert.match(String(echoed.value), new RegExp(wsCorrelation));

    const readiness = await post(base, '/v13/readiness', { action: 'start', seconds: 15 });
    assert.equal(readiness.status, 201);
    const readinessBody = await readiness.json();
    assert.equal(readinessBody.health, 200);
    assert.equal(readinessBody.ready, 503);

    const health = await fetch(base + '/healthz');
    const ready = await fetch(base + '/readyz');
    assert.equal(health.status, 200);
    assert.equal(ready.status, 503);

    const stop = await post(base, '/v13/readiness', { action: 'stop' });
    assert.equal(stop.status, 200);
  } finally {
    if (child.exitCode === null) child.kill('SIGTERM');
    await new Promise((resolve) => {
      if (child.exitCode !== null) return resolve();
      const timer = setTimeout(() => {
        child.kill('SIGKILL');
        resolve();
      }, 9000);
      child.once('exit', () => {
        clearTimeout(timer);
        resolve();
      });
    });
    await rm(dataDir, { recursive: true, force: true });
  }
});
