#!/usr/bin/env node

import { spawnSync } from 'node:child_process';
import {
  existsSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(scriptDirectory, '..');
const pnpm = process.platform === 'win32' ? 'pnpm.cmd' : 'pnpm';
const internalMilestonePattern =
  /\b(?:CI-M-\d{3}(?:-\d{3})?|DL-\d{3}|INV-\d+|M-\d{3}|RA-\d{3}|[A-Z]-S\d+|R-[A-Z0-9][A-Z0-9-]*|[A-Z]-D\d+|D(?:[2-9]|\d{2,})|F\d+|P\d+(?:-lite)?|Track [A-Z]|Phase \d+)\b/g;

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: repositoryRoot,
    env: process.env,
    stdio: 'inherit',
    ...options,
  });

  if (result.error) throw result.error;
  if (result.status !== 0) {
    const exitDescription =
      result.signal === null
        ? `exit code ${result.status}`
        : `signal ${result.signal}`;
    throw new Error(
      `${command} ${args.join(' ')} failed with ${exitDescription}`,
    );
  }
}

function gitRevision() {
  if (process.env.GITHUB_SHA) return process.env.GITHUB_SHA;

  const result = spawnSync('git', ['rev-parse', 'HEAD'], {
    cwd: repositoryRoot,
    encoding: 'utf8',
  });
  if (result.status !== 0) {
    throw new Error(
      `Unable to resolve the Git revision: ${result.stderr.trim()}`,
    );
  }
  return result.stdout.trim();
}

function convert(optionsFile, jsonFile, htmlDirectory, revision) {
  run(pnpm, [
    'exec',
    'typedoc',
    '--options',
    optionsFile,
    '--json',
    jsonFile,
    '--out',
    htmlDirectory,
    '--gitRevision',
    revision,
  ]);

  if (!existsSync(jsonFile)) {
    throw new Error(`TypeDoc did not create ${jsonFile}`);
  }
}

function assertGeneratedSite() {
  for (const output of ['index.html', '.nojekyll', 'sitemap.xml']) {
    const outputPath = join(repositoryRoot, 'docs', 'api', output);
    if (!existsSync(outputPath)) {
      throw new Error(`Generated API site is missing docs/api/${output}`);
    }
  }

  const index = readFileSync(
    join(repositoryRoot, 'docs', 'api', 'index.html'),
    'utf8',
  );
  if (!index.includes('Anchorage API reference')) {
    throw new Error('Generated API site has an unexpected title');
  }

  const exposures = [];
  const pending = [join(repositoryRoot, 'docs', 'api')];
  while (pending.length > 0) {
    const directory = pending.pop();
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) pending.push(path);
      else if (entry.isFile() && entry.name.endsWith('.html')) {
        const html = readFileSync(path, 'utf8');
        for (const match of html.matchAll(internalMilestonePattern)) {
          exposures.push(
            `${match[0]} in ${path.slice(repositoryRoot.length + 1)}`,
          );
        }
      }
    }
  }
  if (exposures.length > 0) {
    const shown = [...new Set(exposures)].sort().slice(0, 20);
    throw new Error(
      `Generated API docs expose ${exposures.length} internal milestone token(s):\n${shown
        .map((exposure) => `- ${exposure}`)
        .join('\n')}${exposures.length > shown.length ? '\n- …' : ''}`,
    );
  }
}

const temporaryDirectory = mkdtempSync(join(tmpdir(), 'anchorage-api-docs-'));
const revision = gitRevision();

try {
  run(pnpm, ['--filter', '@proofoftech/breakwater', 'build']);

  const passes = [
    {
      options: 'packages/breakwater/typedoc.json',
      json: join(temporaryDirectory, 'breakwater.json'),
      html: join(temporaryDirectory, 'breakwater'),
    },
    {
      options: 'packages/flowsafe/typedoc.json',
      json: join(temporaryDirectory, 'flowsafe.json'),
      html: join(temporaryDirectory, 'flowsafe'),
    },
    {
      options: 'packages/flowsafe/src/approval-ui/typedoc.json',
      json: join(temporaryDirectory, 'approval-ui.json'),
      html: join(temporaryDirectory, 'approval-ui'),
    },
  ];

  for (const pass of passes) {
    convert(pass.options, pass.json, pass.html, revision);
  }

  run(pnpm, [
    'exec',
    'typedoc',
    '--options',
    'typedoc.json',
    '--entryPoints',
    ...passes.map((pass) => pass.json),
    '--gitRevision',
    revision,
  ]);
  assertGeneratedSite();
} finally {
  rmSync(temporaryDirectory, { force: true, recursive: true });
}
