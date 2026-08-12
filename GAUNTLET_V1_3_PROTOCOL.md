# Infrastry Gauntlet v1.3.2 — Final assurance protocol

## Purpose

Produce repeatable, attributable evidence about repository understanding, provisioning, deployment, verification, persistence, networking, databases, monitoring, recovery, and developer-facing observability without turning single anomalies into platform accusations.

## Reporting model

Every test has one permanent human-readable name. Run counts, instance IDs, releases, timestamps and correlation IDs are evidence fields, not part of the test name.

The primary report answers four questions:

1. What did we test?
2. What actually happened?
3. How many independent times did it happen?
4. How confident are we?

Raw expected/actual values remain available in expandable evidence and the JSON export.

## Independence policy

Confidence counts independent trials, not clicks or database rows.

- Deployment, runtime, storage, database and lifecycle findings deduplicate by runtime instance.
- Observability findings deduplicate by verified correlation ID.
- Repeated refreshes on the same runtime do not increase confidence.
- Platform-facing observations do not count as PASS/FAIL until correlated with Infrastry logs or monitoring.

## Confidence policy

Three independent matching trials are required for confirmation.

- **Confirmed pass** — 3+ independent passes and no failures.
- **Confirmed defect** — 3+ independent failures and no passes.
- **Likely pass** — 2 independent passes and no failures.
- **Likely concern** — 2 independent failures and no passes.
- **Intermittent** — contradictory pass/fail evidence.
- **Needs evidence** — evidence exists but has not met a conclusion threshold.
- **Not tested** — no controlled evidence exists yet.

Architectural characteristics are reported neutrally. For example, application filesystem behavior is described as **persistent**, **ephemeral**, or **inconsistent** rather than automatically being called a defect.

## Evidence integrity

Evidence-writing experiment controls require `GAUNTLET_ADMIN_KEY`. Public visitors can read the report but cannot submit verification results or controlled log/readiness/access experiments.

Historical v1.3 records are retained and mapped from their former `-001` identifiers into the permanent finding names so previous evidence is not discarded.

## Controlled experiments

### Readiness and liveness

The harness can temporarily hold `/healthz` at HTTP 200 while `/readyz` returns HTTP 503. Endpoint separation is recorded automatically. Any claim about Infrastry's platform reaction requires correlation with its Platform/Monitoring logs before a pass/fail is recorded.

### Runtime logs

A protected control emits matching structured stdout and stderr markers with a unique correlation ID. The result remains an observation until that ID is verified in Infrastry Runtime logs.

### Access logs

Protected controls generate known HTTP 200, 404 and 500 responses with unique correlation IDs. Results remain observations until verified in Infrastry Access logs.

### Shutdown receipts

The supervisor records receipt of SIGTERM/SIGINT, forwards SIGTERM to the child harness, waits for exit, and records a durable graceful-shutdown receipt in PostgreSQL when possible.

### Persistence comparisons

Storage and database continuity are compared automatically against evidence from a prior independent runtime. Database persistence requires matching database identity/fingerprint, non-decreasing existing data, and advancing boot history. Storage identity changes are reported as evidence of an ephemeral application filesystem, not automatically as a defect.

## Final reporting rule

Negative platform feedback must be reproducible. A finding may be described as confirmed only when its defined independence threshold is satisfied and the underlying raw evidence is preserved for audit. Unsupported conclusions remain **Needs evidence** or **Not tested**.
