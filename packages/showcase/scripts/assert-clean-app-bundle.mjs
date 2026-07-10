#!/usr/bin/env node
// Post-build tripwire: the production app bundle must contain NO demo token.
// The tokens are public dev credentials (worker/demo-actors.ts); shipping
// them in the deployed SPA is what made "paste the demo token as the real
// secret" the path of least resistance. main.tsx keeps the switcher behind a
// DEV-only dynamic import — this script proves the dead branch actually got
// eliminated instead of trusting the bundler.
//
// Token literals are duplicated here BY DESIGN: importing demo-actors.ts from
// the assertion script would let a rename in one place silently blind the
// check. demo-actors.test.ts pins these against the module.

import { readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const DEMO_TOKENS = [
  'demo-admin',
  'demo-builder',
  'demo-operator',
  'demo-reviewer',
  'demo-viewer',
];

const distDir = join(dirname(fileURLToPath(import.meta.url)), '../dist');

function* walk(dir) {
  for (const entry of readdirSync(dir)) {
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) yield* walk(path);
    else yield path;
  }
}

let checked = 0;
const hits = [];
for (const path of walk(distDir)) {
  if (!/\.(js|css|html|map)$/.test(path)) continue;
  checked += 1;
  const content = readFileSync(path, 'utf8');
  for (const token of DEMO_TOKENS) {
    if (content.includes(token)) hits.push(`${path}: contains '${token}'`);
  }
}

if (checked === 0) {
  console.error('assert-clean-app-bundle: no bundle files found in dist/');
  process.exit(1);
}
if (hits.length > 0) {
  console.error(
    'assert-clean-app-bundle: demo tokens leaked into the production bundle:',
  );
  for (const hit of hits) console.error(`  ${hit}`);
  process.exit(1);
}
console.log(
  `assert-clean-app-bundle: ${checked} bundle files are demo-token-free`,
);
