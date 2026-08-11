# Infrastry Gauntlet v1.3 — Reproducibility + Observability

## Purpose

Produce repeatable, attributable evidence about repository understanding, provisioning, deployment, verification, persistence, networking, databases, monitoring, recovery, and developer-facing observability without treating single anomalies as platform defects.

## Reporting model

Tests use one permanent human-readable identity. Run numbers are evidence, not part of the test name.

Examples:

- **Runtime** — not `CORE-RUNTIME-001`
- **Dynamic port** — not `NET-DYNAMIC-PORT-001`
- **Environment injection** — not `ENV-SENTINEL-001`
- **Writable storage** — not `STORAGE-WRITE-001`
- **Storage persistence** — not `STORAGE-CONTINUITY-001`
- **Database connection** — not `DB-CONNECT-001`
- **Database persistence** — not `DB-CONTINUITY-001`
- **Restart recovery** — not `RESTART-RECOVERY-001`
- **Graceful shutdown** — not `SHUTDOWN-GRACEFUL-001`
- **HTTPS proxy** — not `PROXY-HTTPS-001`
- **WebSocket proxy** — not `WS-PROXY-ECHO-001`
- **Readiness vs liveness** — not `READINESS-SEPARATION-001`
- **Runtime log capture** — not `OBS-LOG-MARKER-001`

Each finding should read as:

> **Database persistence** — PASS · 3/3 successful runs · Confirmed pass
>
> Same database identity and endpoint fingerprint observed across three independent application instances. Existing rows remained intact and database boot count increased on every replacement.

The report may retain a compact machine key such as `database-persistence`, but internal keys should not be the primary UI copy.

## Evidence model

Every application boot creates a unique `runId` and `instanceId`. Those identifiers belong in the evidence details, not in the test title. Durable PostgreSQL records store individual observations and lifecycle events.

Each test summary contains:

- finding name
- current verdict
- confidence
- concluded runs
- pass / fail / pending counts
- distinct application instances
- distinct releases
- expected behavior
- observed behavior
- evidence / correlation data
- first observed / last observed timestamps

## Confidence policy

- **Confirmed pass**: at least 3 concluded PASS runs and 0 FAIL runs.
- **Confirmed defect**: at least 3 concluded FAIL runs and 0 PASS runs.
- **Intermittent**: at least 1 PASS and at least 1 FAIL.
- **Likely pass / likely defect**: 2 matching concluded runs with no opposite result.
- **Single observation**: exactly 1 concluded PASS or FAIL run.
- **Insufficient evidence**: no concluded PASS/FAIL result yet.

Pending and observational runs are retained but do not establish a defect.

## v1.3 experiments

### Readiness vs liveness

A protected endpoint temporarily forces `/readyz` to return HTTP 503 while `/healthz` continues to return HTTP 200. The harness verifies the endpoint separation internally. Infrastry's platform/monitoring reaction must be correlated from Application Logs before any conclusion about readiness-aware health checks is made.

### Observability correlation

A protected marker emits one structured stdout entry and one structured stderr entry using the same correlation ID. Access-log probes generate controlled 200, 404, and 500 responses with correlation IDs. These are compared with Infrastry's Platform / Build / Runtime / Access timeline.

### Shutdown receipts

The supervisor records `shutdown_signal_received`, forwards SIGTERM to the child process, waits for it to exit, and then records `graceful_shutdown` in durable PostgreSQL when possible. A graceful-shutdown conclusion requires durable receipts rather than merely observing process replacement.

### Reproduction matrix

Automatic checks are persisted once per run. Confidence is aggregated across distinct process instances and releases.

## Scientific reporting rule

Negative findings remain observations until reproduced under controlled conditions. The final report must distinguish confirmed, likely, intermittent, single-observation, and not-tested findings. The UI and exports should lead with clear findings and supporting evidence, not internal test codes.
