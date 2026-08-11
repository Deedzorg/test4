# Infrastry Gauntlet

**Infrastry Gauntlet** is an evidence-driven deployment test app for measuring what Infrastry can actually infer, provision, deploy, verify, monitor, persist, and recover.

It intentionally contains **no Dockerfile, Procfile, fixed production port, or committed secrets**. The first benchmark is whether Infrastry can understand the repository from normal application signals and launch it correctly.

## What it tests

- Node runtime detection and `npm start`
- dynamic platform `PORT`
- `/healthz` liveness and `/readyz` readiness
- HTTPS / reverse-proxy signals
- environment variable injection via `GAUNTLET_SENTINEL`
- writable filesystem behavior
- persistence across restarts/redeployments
- WebSocket upgrade/echo at `/ws`
- recurring background heartbeat work
- optional PostgreSQL connection and write probe
- unique instance IDs for scaling/load-balancing observation
- structured JSON runtime logs
- `/metrics` counters
- graceful shutdown persistence
- optional authenticated crash/recovery testing
- persisted evidence snapshots
- browser-exported JSON reports and human feedback notes

## Environment

See `.env.example`.

- `GAUNTLET_SENTINEL` proves environment injection without exposing its value.
- `DATA_DIR` selects the writable/persistent data directory; default is `.data`.
- `DATABASE_URL` enables PostgreSQL testing.
- `REQUIRE_DATABASE=true` makes database connectivity a readiness requirement.
- `CHAOS_ENABLED=true` plus `GAUNTLET_ADMIN_KEY` enables the protected recovery endpoint.

## Recommended Infrastry test sequence

1. Connect this repository without adding Docker or deployment instructions.
2. Before changing anything, record what Infrastry says it detected and plans to provision.
3. Deploy and open the dashboard.
4. Set `GAUNTLET_SENTINEL=infrastry-found-me` and verify the environment test.
5. Write a persistence marker, restart/redeploy, and confirm the marker plus boot counter survive.
6. Add a PostgreSQL resource, set `DATABASE_URL`, then set `REQUIRE_DATABASE=true` and run the database probe.
7. Observe WebSocket behavior and unique instance IDs.
8. Save evidence snapshots and export the JSON report.
9. Only in a controlled test, enable chaos mode and trigger a protected crash to evaluate restart/recovery behavior.
10. Record resources provisioned, manual intervention, recovery quality, and cost in the dashboard feedback fields.

## Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | liveness |
| `GET /readyz` | storage/database-aware readiness |
| `GET /api/status` | runtime evidence |
| `GET /api/tests` | self-test matrix |
| `POST /api/persist` | persistent marker write |
| `POST /api/database/probe` | PostgreSQL write test |
| `POST /api/snapshot` | persisted evidence snapshot |
| `GET /api/events` | recent persisted events |
| `GET /metrics` | lightweight counters |
| `WS /ws` | WebSocket handshake and echo |
| `POST /api/chaos/crash` | protected restart/recovery test |

The canonical benchmark list is in `gauntlet.manifest.json`.

## Local baseline

```bash
npm install
npm run check
npm test
npm start
```

Then open `http://localhost:3000`.

The goal is simple: **produce reliable evidence, not vibes.**
