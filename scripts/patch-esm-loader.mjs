// Post-build patch that makes the napi-rs generated `index.js` loader safe to
// inline into an ES-module bundle.
//
// Why this exists
// ---------------
// `napi build` emits a CommonJS loader (`index.js`) that locates the native
// `.node` binary with `join(__dirname, 'fjall.<triple>.node')`. `__dirname` is
// a CommonJS-only global. Loading the package directly with Node (CJS *or* ESM)
// is fine, because our package.json has no `"type": "module"`, so Node treats
// `index.js` as CommonJS and `__dirname` is defined.
//
// The breakage happens when a downstream project with `"type": "module"`
// *bundles* us with a tool such as @vercel/ncc or webpack: the CJS loader gets
// inlined into the consumer's ESM output, where `__dirname` is undefined, and
// the binding load throws:
//
//   ReferenceError: __dirname is not defined in ES module scope
//
// Fix: rewrite every `__dirname` reference to a guarded `__napiDirname` that
// falls back to '.' when `__dirname` is unavailable. In a bundle the native
// require is resolved by the bundler / optionalDependencies, so the filesystem
// path that `__dirname` would have produced is unused there anyway.
//
// The script is idempotent and fails loudly if the napi-rs loader format
// changes, so we never silently publish an unpatched (crashing) loader.

import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const target = join(dirname(fileURLToPath(import.meta.url)), '..', 'index.js');

if (!existsSync(target)) {
  // Non-fatal: lets this run as a `prepublishOnly` safety net even when there
  // is nothing built yet. The build script chains `napi build && this`, so a
  // genuine build failure is already surfaced by napi itself.
  console.warn(
    `[patch-esm-loader] index.js not found at ${target} — skipping (run \`napi build\` first).`,
  );
  process.exit(0);
}

let src = readFileSync(target, 'utf8');

if (src.includes('__napiDirname')) {
  console.log('[patch-esm-loader] index.js already patched — nothing to do.');
  process.exit(0);
}

const ANCHOR = "const { join } = require('path')";
if (!src.includes(ANCHOR)) {
  console.error(
    `[patch-esm-loader] ERROR: anchor ${JSON.stringify(ANCHOR)} not found in index.js.\n` +
      '  The napi-rs loader format changed; update scripts/patch-esm-loader.mjs.',
  );
  process.exit(1);
}

const rewritten = (src.match(/\bjoin\(__dirname\b/g) || []).length;

// Rewrite every bare __dirname, then declare the guarded replacement once.
src = src.replace(/\b__dirname\b/g, '__napiDirname');

const guard = [
  ANCHOR,
  '',
  '// --- Patched by scripts/patch-esm-loader.mjs for ESM-bundler compatibility ---',
  '// __dirname is undefined when this CommonJS loader is inlined into an ESM',
  '// bundle (e.g. @vercel/ncc, webpack) by a consumer with "type":"module".',
  "const __napiDirname = typeof __dirname !== 'undefined' ? __dirname : '.'",
].join('\n');

src = src.replace(ANCHOR, () => guard);

// Safety net: every remaining __dirname must live in a comment or inside the
// `typeof __dirname` guard. A bare __dirname anywhere else would still throw in
// ES module scope, so abort without writing rather than ship a broken loader.
const offending = src.split('\n').filter((line) => {
  if (!/\b__dirname\b/.test(line)) return false;
  const trimmed = line.trim();
  if (trimmed.startsWith('//') || trimmed.startsWith('*')) return false;
  if (/typeof __dirname/.test(line)) return false;
  return true;
});
if (offending.length > 0) {
  console.error(
    '[patch-esm-loader] ERROR: unguarded __dirname remains after patching:\n' +
      offending.map((l) => `    ${l.trim()}`).join('\n') +
      '\n  Aborting without writing.',
  );
  process.exit(1);
}

writeFileSync(target, src);
console.log(
  `[patch-esm-loader] Patched index.js: rewrote ${rewritten} \`join(__dirname, …)\` ` +
    'call(s) to a guarded __napiDirname.',
);
