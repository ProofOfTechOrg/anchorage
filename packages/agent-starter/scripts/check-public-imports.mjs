// SPDX-License-Identifier: Apache-2.0

import { readdir, readFile } from 'node:fs/promises';
import { extname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));
const roots = ['src', 'test', 'scripts'];
const forbidden = [
  /@proofoftech\/(?:flowsafe|breakwater)\/(?:src|dist)\//,
  /packages\/(?:flowsafe|breakwater)\/src\//,
  /\.\.\/\.\.\/(?:flowsafe|breakwater)\//,
];

async function filesIn(directory) {
  const files = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await filesIn(path)));
    else if (['.ts', '.mts', '.js', '.mjs'].includes(extname(path)))
      files.push(path);
  }
  return files;
}

const violations = [];
for (const name of roots) {
  for (const path of await filesIn(join(root, name))) {
    const source = await readFile(path, 'utf8');
    if (forbidden.some((pattern) => pattern.test(source))) {
      violations.push(relative(root, path));
    }
  }
}

if (violations.length > 0) {
  throw new Error(
    `starter code must use published package entrypoints; forbidden imports in: ${violations.join(', ')}`,
  );
}

console.log('public-import boundary: ok');
