import assert from 'node:assert/strict';
import { access, readFile } from 'node:fs/promises';
import test from 'node:test';

const manifest = JSON.parse(await readFile(new URL('../gauntlet.manifest.json', import.meta.url), 'utf8'));
const pkg = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8'));

test('manifest contains core infrastructure tests', () => {
  const ids = new Set(manifest.tests.map((entry) => entry.id));
  for (const expected of ['runtime','dynamic-port','health','environment','storage-write','storage-persistence','websocket','background-work','database','https-proxy','restart-recovery','scaling','cost']) {
    assert.ok(ids.has(expected), `missing ${expected}`);
  }
});

test('repository leaves infrastructure inference to the platform', async () => {
  assert.match(pkg.scripts.start, /^node server-v\d+\.mjs$/, 'start script should point at the active versioned Gauntlet server');
  const entrypoint = pkg.scripts.start.split(/\s+/)[1];
  await access(new URL(`../${entrypoint}`, import.meta.url));
  assert.equal(manifest.intentionalConstraints.fixedPort, false);
  for (const filename of ['Dockerfile', 'Procfile']) {
    await assert.rejects(access(new URL(`../${filename}`, import.meta.url)));
  }
});
