# Infrastry Gauntlet v1.3 — Reproducibility + Observability

## Purpose

Produce repeatable, attributable evidence about repository understanding, provisioning, deployment, verification, persistence, networking, databases, monitoring, recovery, and developer-facing observability without treating single anomalies as platform defects.

## Evidence model

Every v1.3 application boot creates a unique supervisor `runId` and `instanceId`. The existing v1.2 harness remains active behind the supervisor and continues to provide storage/database continuity evidence. v1.3 adds durable PostgreSQL records for test runs and lifecycle events.

## Confidence policy

- **confirmed-pass**: at least 3 concluded PASS runs and 0 FAIL runs.
- **confirmed-defect**: at least 3 concluded FAIL runs and 0 PASS runs.
- **intermittent**: at least 1 PASS and at least 1 FAIL.
- **likely-pass / likely-defect**: 2 matching concluded runs with no opposite result.
- **single-observation**: exactly 1 concluded run.
- **insufficient-evidence**: no concluded PASS/FAIL result yet.

Pending and observational runs are retained but do not by themselves establish a defect.

## v1.3 experiments

### Readiness separation

A protected endpoint temporarily forces `/readyz` to return HTTP 503 while `/healthz` continues to return HTTP 200. The harness verifies the endpoint separation internally. Infrastry's platform/monitoring reaction must be correlated from Application Logs before any conclusion about readiness-aware health checks is made.

### Observability correlation

A protected marker emits one structured stdout entry and one structured stderr entry using the same correlation ID. Access-log probes generate controlled 200, 404, and 500 responses with correlation IDs. These can be compared with Infrastry's Platform / Build / Runtime / Access timeline.

### Shutdown receipts

The v1.3 supervisor records `shutdown_signal_received`, forwards SIGTERM to the v1.2 child process, waits for it to exit, and then records `graceful_shutdown` in durable PostgreSQL when possible. A graceful-shutdown conclusion requires durable receipts rather than merely observing process replacement.

### Reproduction matrix

Automatic boot checks are persisted once per v1.3 run in PostgreSQL. Confidence is aggregated across distinct process instances and releases.

## Scientific reporting rule

Negative findings remain observations until reproduced under controlled conditions. The final report must distinguish confirmed, likely, intermittent, single-observation, passed, and not-tested findings.
