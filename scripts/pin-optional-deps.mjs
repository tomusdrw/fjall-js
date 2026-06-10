// Inject the platform sub-packages into the main package's optionalDependencies,
// pinned to the main package's exact version. Run by the release workflow right
// before `npm publish` of the main package.
//
// Why this is done at publish time instead of being committed
// -----------------------------------------------------------
// The main package depends (optionally, per-platform) on `@fjall-js/fjall-<triple>`
// at its *own* version. Committing those optionalDependencies makes the manifest
// reference versions that do not exist on the registry yet (they are published by
// this very release), so `npm ci` rejects the lockfile as out of sync:
//
//   npm error Missing: @fjall-js/fjall-darwin-arm64@ from lock file
//
// That broke every release. Keeping them out of the committed manifest means
// `npm ci` always works (in CI and for contributors); this script re-adds them
// to the published artifact only. The names are read from the npm/<triple>/
// package.json files so adding a platform needs no change here.

import { existsSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const root = JSON.parse(readFileSync('package.json', 'utf8'));
const { version } = root;

const optionalDependencies = {};
for (const entry of readdirSync('npm', { withFileTypes: true })) {
  if (!entry.isDirectory()) continue;
  const manifest = join('npm', entry.name, 'package.json');
  if (!existsSync(manifest)) continue;
  const { name } = JSON.parse(readFileSync(manifest, 'utf8'));
  optionalDependencies[name] = version;
}

if (Object.keys(optionalDependencies).length === 0) {
  console.error('[pin-optional-deps] ERROR: no platform packages found under npm/.');
  process.exit(1);
}

root.optionalDependencies = optionalDependencies;
writeFileSync('package.json', JSON.stringify(root, null, 2) + '\n');

console.log(
  `[pin-optional-deps] Pinned optionalDependencies to ${version}:\n` +
    Object.keys(optionalDependencies)
      .map((name) => `    ${name}`)
      .join('\n'),
);
