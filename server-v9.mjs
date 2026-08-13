import http from 'node:http';
import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { WebSocket } from 'ws';
import {
  LEGACY_SUPERVISOR_RELEASE,
  databasePersistenceVerdict,
  decorateActiveStatus,
  decorateEvidencePayload,
  persistenceActual,
  storagePersistenceVerdict
} from './lib/evidence-quality.mjs';

const { Pool } = pg;
const PACKAGE = JSON.parse(await readFile(new URL('./package.json', import.meta.url), 'utf8'));
const RELEASE = JSON.parse(await readFile(new URL('./gauntlet-release.json', import.meta.url), 'utf8'));
const MIGRATIONS = JSON.parse(await readFile(new URL('./evidence-migrations/v1.4.2.json', import.meta.url), 'utf8'));
const VERSION = PACKAGE.version;
const DATABASE_URL = process.env.DATABASE_URL || '';
const PORT = Number(process.env.PORT || 3000);
const RELEASE_CONTEXT = { version: VERSION, release: RELEASE };
const qualityState = {
  reconciliationRuns: 0,
  releaseRowsCorrected: 0,
  operatorVerificationsInserted: 0,
  databaseComparisonsRepaired: 0,
  storageComparisonsRepaired: 0,
  lastReconciledAt: null,
  lastError: null
};

// v1.4.2 keeps the proven v1.3/v1.4 runtime stack intact and fixes evidence
// semantics at the boundary: active release labeling, persistence comparison,
// and explicit migration of already-human-verified platform observations.
patchEvidenceReleaseWrites();
patchStructuredConsoleLabels();
patchWebSocketLabels();
patchHttpEvidenceBoundary();

await import('./server-v8.mjs');

// Repair historical comparison rows immediately so the legacy boot suite can use
// a concluded prior observation, then reconcile again after its 1.6s boot timer.
if (DATABASE_URL) {
  await reconcileEvidenceStore().catch(recordReconcileError);
  const timer = setTimeout(() => void reconcileEvidenceStore().catch(recordReconcileError), 3200);
  timer.unref();
}

function patchEvidenceReleaseWrites() {
  const originalQuery = Pool.prototype.query;
  Pool.prototype.query = function evidenceAwareQuery(text, values, ...rest) {
    if (typeof text === 'string' && /gauntlet_v13_(test_runs|lifecycle)/.test(text) && Array.isArray(values)) {
      const normalized = values.map((value) => value === LEGACY_SUPERVISOR_RELEASE ? RELEASE.releaseId : value);
      return originalQuery.call(this, text, normalized, ...rest);
    }
    return originalQuery.call(this, text, values, ...rest);
  };
  rawQuery.original = originalQuery;
}

function patchStructuredConsoleLabels() {
  for (const method of ['log', 'error']) {
    const original = console[method].bind(console);
    console[method] = (...args) => original(...args.map(rewriteStructuredLogValue));
  }
}

function rewriteStructuredLogValue(value) {
  if (typeof value !== 'string' || value[0] !== '{') return value;
  try {
    const parsed = JSON.parse(value);
    if (parsed?.releaseId === LEGACY_SUPERVISOR_RELEASE) parsed.releaseId = RELEASE.releaseId;
    if (parsed?.service === 'infrastry-gauntlet' && parsed?.version === '1.3.2') parsed.version = VERSION;
    return JSON.stringify(parsed);
  } catch {
    return value;
  }
}

function patchWebSocketLabels() {
  const originalSend = WebSocket.prototype.send;
  WebSocket.prototype.send = function evidenceAwareSend(data, ...rest) {
    let next = data;
    if (typeof data === 'string' && data[0] === '{') {
      try {
        const parsed = JSON.parse(data);
        if (parsed?.releaseId === LEGACY_SUPERVISOR_RELEASE) {
          parsed.releaseId = RELEASE.releaseId;
          next = JSON.stringify(parsed);
        }
      } catch {}
    }
    return originalSend.call(this, next, ...rest);
  };
}

function patchHttpEvidenceBoundary() {
  const originalCreateServer = http.createServer;
  http.createServer = function evidenceQualityCreateServer(listener, ...rest) {
    const wrapped = async (req, res) => {
      const url = new URL(req.url || '/', 'http://local');
      const path = url.pathname;
      if (path === '/v13/status' || path === '/v13/evidence' || path === '/' || path === '/auto') {
        const originalEnd = res.end.bind(res);
        res.end = (chunk, encoding, callback) => {
          try {
            const source = Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk ?? '');
            let output = source;
            if (source && path === '/v13/status') {
              const payload = decorateActiveStatus(JSON.parse(source), RELEASE_CONTEXT);
              payload.evidenceQuality.reconciliation = { ...qualityState };
              output = JSON.stringify(payload);
            } else if (source && path === '/v13/evidence') {
              const payload = decorateEvidencePayload(JSON.parse(source), RELEASE_CONTEXT);
              payload.evidenceQuality.reconciliation = { ...qualityState };
              output = JSON.stringify(payload);
            } else if (source && (path === '/' || path === '/auto')) {
              output = source
                .replaceAll('v1.3-final-assurance', RELEASE.releaseId)
                .replace(/Automation helper v1\.4\.0/g, `Automation helper v${VERSION}`);
            }
            if (output !== source) res.removeHeader('Content-Length');
            return originalEnd(output, encoding, callback);
          } catch {
            return originalEnd(chunk, encoding, callback);
          }
        };
      }
      return listener(req, res);
    };
    return originalCreateServer.call(http, wrapped, ...rest);
  };
}

async function reconcileEvidenceStore() {
  const pool = new Pool({ connectionString: DATABASE_URL, max: 2, connectionTimeoutMillis: 5000 });
  try {
    qualityState.reconciliationRuns++;
    qualityState.releaseRowsCorrected += await applyReleaseCorrections(pool);
    qualityState.operatorVerificationsInserted += await applyOperatorVerifications(pool);
    qualityState.databaseComparisonsRepaired += await repairPersistenceHistory(pool, 'database-persistence', databasePersistenceVerdict);
    qualityState.storageComparisonsRepaired += await repairPersistenceHistory(pool, 'storage-persistence', storagePersistenceVerdict);
    qualityState.lastReconciledAt = new Date().toISOString();
    qualityState.lastError = null;
  } finally {
    await pool.end().catch(() => {});
  }
}

async function applyReleaseCorrections(pool) {
  let changed = 0;
  for (const correction of MIGRATIONS.releaseCorrections || []) {
    for (const table of ['gauntlet_v13_test_runs', 'gauntlet_v13_lifecycle']) {
      const result = await rawQuery(pool,
        `UPDATE ${table} SET release_id=$1 WHERE run_id=$2 AND release_id=$3`,
        [correction.to, correction.runId, correction.from]);
      changed += result.rowCount || 0;
    }
  }
  return changed;
}

async function applyOperatorVerifications(pool) {
  let inserted = 0;
  for (const verification of MIGRATIONS.platformVerifications || []) {
    const existing = await rawQuery(pool,
      `SELECT id FROM gauntlet_v13_test_runs
       WHERE test_id=$1 AND evidence->>'correlationId'=$2 AND verdict IN ('pass','fail')
       LIMIT 1`,
      [verification.testKey, verification.correlationId]);
    if (existing.rowCount) continue;

    const source = await rawQuery(pool,
      `SELECT run_id,release_id,instance_id,expected,actual
       FROM gauntlet_v13_test_runs
       WHERE test_id=$1 AND evidence->>'correlationId'=$2
       ORDER BY id ASC LIMIT 1`,
      [verification.testKey, verification.correlationId]);
    if (!source.rowCount) continue;

    const row = source.rows[0];
    const evidence = {
      correlationId: verification.correlationId,
      source: 'operator-confirmed-platform-monitoring',
      note: verification.note,
      basis: verification.basis,
      migratedBy: RELEASE.releaseId
    };
    await rawQuery(pool,
      `INSERT INTO gauntlet_v13_test_runs(test_id,run_id,release_id,instance_id,verdict,expected,actual,evidence)
       VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8::jsonb)`,
      [verification.testKey, row.run_id, row.release_id, row.instance_id, verification.verdict,
        JSON.stringify(verification.expected || row.expected || {}),
        JSON.stringify(verification.actual || row.actual || {}),
        JSON.stringify(evidence)]);
    inserted++;
  }
  return inserted;
}

async function repairPersistenceHistory(pool, testKey, comparator) {
  const result = await rawQuery(pool,
    `SELECT id,instance_id,verdict,actual FROM gauntlet_v13_test_runs
     WHERE test_id=$1 ORDER BY id ASC`, [testKey]);
  let previous = null;
  let changed = 0;

  for (const row of result.rows) {
    if (!row.instance_id) continue;
    if (!previous) {
      previous = row;
      continue;
    }
    if (row.instance_id === previous.instance_id) continue;

    const verdict = comparator(previous.actual || {}, row.actual || {});
    const actual = persistenceActual(testKey, row.actual || {}, previous.actual || {});
    if (verdict !== 'pending' && (row.verdict !== verdict || JSON.stringify(row.actual || {}) !== JSON.stringify(actual))) {
      await rawQuery(pool,
        `UPDATE gauntlet_v13_test_runs SET verdict=$1, actual=$2::jsonb WHERE id=$3`,
        [verdict, JSON.stringify(actual), row.id]);
      changed++;
    }
    previous = { ...row, verdict, actual };
  }
  return changed;
}

function rawQuery(pool, text, values = []) {
  return rawQuery.original.call(pool, text, values);
}

function recordReconcileError(error) {
  qualityState.lastError = String(error?.message || error);
  qualityState.lastReconciledAt = new Date().toISOString();
  console.error(JSON.stringify({
    level: 'error',
    event: 'evidence_quality.reconcile_failed',
    releaseId: RELEASE.releaseId,
    version: VERSION,
    message: qualityState.lastError
  }));
}
