# Infrastry Gauntlet — Redeploy Test 001

Purpose: force a harmless new source revision so we can test release replacement without changing application behavior.

Baseline before redeploy:
- Marker: `infrastry-persist-test-001`
- Boot count: `1`
- PostgreSQL write count: `1`
- WebSocket live echo: passed
- Snapshot count: `1`
- Baseline instance: `386f950c-bae5-4fe0-bd42-7b536b39444a`

Expected after redeploy:
- A new instance ID should appear.
- If `/app/.data` is persistent, boot count should increase and the marker should remain.
- If `/app/.data` is ephemeral, boot count should reset to 1 and the marker should disappear.
- PostgreSQL data should remain available independently of app filesystem persistence.
