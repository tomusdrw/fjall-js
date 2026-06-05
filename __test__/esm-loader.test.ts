import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

// Regression test for the napi-rs CommonJS loader being inlined into an ESM
// bundle. A consumer with "type":"module" that bundles us with @vercel/ncc or
// webpack used to crash with:
//   ReferenceError: __dirname is not defined in ES module scope
// scripts/patch-esm-loader.mjs (run as part of `npm run build`) rewrites the
// loader's `join(__dirname, ...)` calls to a guarded __napiDirname. This test
// guards that the published index.js never regresses to a bare __dirname.

const here = dirname(fileURLToPath(import.meta.url));
const loaderSource = readFileSync(join(here, '..', 'index.js'), 'utf8');

describe('index.js native loader is ESM-bundler safe', () => {
  it('does not call join(__dirname, ...) (would throw in ESM scope)', () => {
    expect(loaderSource).not.toMatch(/join\(__dirname\b/);
  });

  it('uses the guarded __napiDirname fallback instead', () => {
    expect(loaderSource).toContain('__napiDirname');
    expect(loaderSource).toMatch(/typeof __dirname !== ['"]undefined['"]/);
  });

  it('has no unguarded __dirname outside comments and the typeof guard', () => {
    const offending = loaderSource
      .split('\n')
      .map((line) => line.trim())
      .filter(
        (line) =>
          /\b__dirname\b/.test(line) &&
          !line.startsWith('//') &&
          !line.startsWith('*') &&
          !/typeof __dirname/.test(line),
      );
    expect(offending).toEqual([]);
  });
});
