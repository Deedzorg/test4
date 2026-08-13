import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import net from 'node:net';
import { WebSocket } from 'ws';
import {
  databasePersistenceVerdict,
  storagePersistenceVerdict
} from '../lib/evidence-quality.mjs';

const ACTIVE_RELEASE = 'v1.4.2-evidence-quality';

function db(identity, bootCount, rows = 0, endpointFingerprint = '6b0ee5be03a9') {
  return { identity, bootCount, rows, endpointFingerprint };
}

test('database persistence compares raw prior observations, including pending baselines', () => {
  const identity = '1a722435-5614-489f-95c7-48a3095a9350';
  const first = db(identity, 1);
  const second = db(identity, 2);
  const third = db(identity, 3);

  assert.equal(databasePersistenceVerdict(first, second), 'pass');
  assert.equal(databasePersistenceVerdict(second, third), 'pass');
  assert.equal(databasePersistenceVerdict(first, db('replacement-db', 2)), 'fail');
  assert.equal(databasePersistenceVerdict(second, db(identity, 2)), 'fail', 'boot history must advance');
  assert.equal(databasePersistenceVerdict(null, first), 'pending');
});

test('application storage comparison reports continuity vs replacement without calling either a defect', () => {
  assert.equal(storagePersistenceVerdict({ identity: 'storage-a' }, { identity: 'storage-a' }), 'pass');
  assert.equal(storagePersistenceVerdict({ identity: 'storage-a' }, { identity: 'storage-b' }), 'fail');
  assert.equal(storagePersistenceVerdict(null, { identity: 'storage-b' }), 'pending');
});

async function portFree(port) {
  return new Promise((resolve) => {
    const server = net.createServer();
    server.once('error', () => resolve(false));
    server.listen(port, '127.0.0.1', () => server.close(() => resolve(true)));
  });
}

async function openPortPair() {
  for (let i = 0; i < 50; i++) {
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

function websocketHello(url) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    const timer = setTimeout(() => {
      ws.terminate();
      reject(new Error('websocket hello timed out'));
    }, 7000);
    ws.on('message', (raw) => {
      try {
        const message = JSON.parse(raw.toString());
        if (message.type === 'hello') {
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

test('v1.4.2 exposes the active release consistently across status, evidence, UI and WebSocket', { timeout: 45000 }, async () => {
  const port = await openPortPair();
  const base = `http://127.0.0.1:${port}`;
  const dataDir = await mkdtemp(join(tmpdir(), 'infrastry-gauntlet-quality-'));
  let stderr = '';
  const child = spawn(process.execPath, ['server-v9.mjs'], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      PORT: String(port),
      HOST: '127.0.0.1',
      DATA_DIR: dataDir,
      DATABASE_URL: '',
      REQUIRE_DATABASE: 'false',
      CHAOS_ENABLED: 'false',
      GAUNTLET_ADMIN_KEY: 'quality-ci-admin-key',
      GAUNTLET_SENTINEL: 'quality-ci-sentinel'
    },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  child.stderr.on('data', (chunk) => { stderr += chunk.toString(); });

  try {
    await waitFor(base + '/v13/status');

    const status = await (await fetch(base + '/v13/status')).json();
    assert.equal(status.version, '1.4.2');
    assert.equal(status.release.releaseId, ACTIVE_RELEASE);
    assert.equal(status.child.release.releaseId, ACTIVE_RELEASE);
    assert.equal(status.evidenceQuality.releaseLabelsNormalized, true);

    const evidence = await (await fetch(base + '/v13/evidence')).json();
    assert.equal(evidence.schemaVersion, 4);
    assert.equal(evidence.version, '1.4.2');
    assert.equal(evidence.release.releaseId, ACTIVE_RELEASE);
    assert.equal(evidence.runtimeStatus.release.releaseId, ACTIVE_RELEASE);

    const auto = await (await fetch(base + '/auto')).text();
    assert.match(auto, /Automation helper v1\.4\.2/);

    const hello = await websocketHello(`ws://127.0.0.1:${port}/ws`);
    assert.equal(hello.releaseId, ACTIVE_RELEASE);
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

  assert.equal(stderr.includes('SyntaxError'), false, stderr);
});
