import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

// Regression test for the napi-rs CommonJS loader being inlined into an ESM
// bundle. A consumer with "type":"module" that bundles us with @vercel/ncc or
// webpack used to crash with:
//   ReferenceError: __dirname is not defined in ES module scope
//
// napi-rs 3.x no longer emits any `__dirname` reference: the generated
// `index.js` locates the addon with a bare relative
// `require('./fjall.<triple>.node')` (plus the per-platform
// `@fjall-js/fjall-<triple>` fallback), which a bundler resolves itself. The
// old `scripts/patch-esm-loader.mjs` post-build rewrite is therefore obsolete
// and has been removed; this test now guards that the generated loader stays
// `__dirname`-free (and hence ESM-bundler safe) by construction.

const here = dirname(fileURLToPath(import.meta.url));
const loaderSource = readFileSync(join(here, '..', 'dist', 'index.js'), 'utf8');

describe('index.js native loader is ESM-bundler safe', () => {
  it('never references `__dirname` (would throw in ESM scope)', () => {
    expect(loaderSource).not.toMatch(/\b__dirname\b/);
  });

  it('locates the addon via a bare relative require', () => {
    expect(loaderSource).toMatch(/require\(\s*['"]\.\/fjall\.[^'"]+\.node['"]\)/);
  });

  it('has no `__dirname` reference outside comments', () => {
    const offending = loaderSource
      .split('\n')
      .map((line) => line.trim())
      .filter(
        (line) => /\b__dirname\b/.test(line) && !line.startsWith('//') && !line.startsWith('*'),
      );
    expect(offending).toEqual([]);
  });
});
