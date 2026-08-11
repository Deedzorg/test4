import { readFile, writeFile } from 'node:fs/promises';

const sourceUrl = new URL('./server.mjs', import.meta.url);
const runtimeUrl = new URL('./.gauntlet-runtime.mjs', import.meta.url);
const source = await readFile(sourceUrl, 'utf8');
const needle = 'function client(){return `';

if (!source.includes(needle)) {
  throw new Error('Gauntlet client patch target not found');
}

const patched = source.replace(needle, 'function client(){return String.raw`');
await writeFile(runtimeUrl, patched, 'utf8');
await import(`${runtimeUrl.href}?v=${Date.now()}`);
