import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));

export const PUBLISH_PREREQUISITES = Object.freeze([
  {
    name: '@proofoftech/breakwater',
    directory: 'packages/breakwater',
  },
]);

function command(program, args, options = {}) {
  const result = spawnSync(program, args, {
    cwd: ROOT,
    encoding: 'utf8',
    ...(options.capture ? {} : { stdio: 'inherit' }),
  });
  return {
    status: result.status,
    stdout: result.stdout ?? '',
    stderr: result.stderr ?? '',
  };
}

function packageVersion(directory) {
  const manifest = JSON.parse(
    readFileSync(join(ROOT, directory, 'package.json'), 'utf8'),
  );
  return manifest.version;
}

function published(name, version) {
  const result = command(
    'npm',
    ['view', `${name}@${version}`, 'version', '--json'],
    { capture: true },
  );
  if (result.status === 0) {
    return JSON.parse(result.stdout) === version;
  }
  const diagnostic = `${result.stdout}\n${result.stderr}`;
  if (/\bE404\b|404 Not Found|No match found/i.test(diagnostic)) return false;
  throw new Error(`npm view failed for ${name}@${version}: ${diagnostic}`);
}

function publishPackage(target, version) {
  const result = command('pnpm', [
    '--dir',
    target.directory,
    'publish',
    '--access',
    'public',
    '--tag',
    'latest',
    '--no-git-checks',
  ]);
  if (result.status !== 0) {
    throw new Error(`publish failed for ${target.name}@${version}`);
  }
}

async function waitUntilPublished(name, version) {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    if (published(name, version)) return;
    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }
  throw new Error(`${name}@${version} did not become visible on npm`);
}

function ensureTag(name, version) {
  const tag = `${name}@${version}`;
  const existing = command(
    'git',
    ['rev-parse', '--quiet', '--verify', `refs/tags/${tag}`],
    { capture: true },
  );
  if (existing.status === 0) return;
  const tagged = command('git', ['tag', tag]);
  if (tagged.status !== 0) throw new Error(`failed to create tag ${tag}`);
  console.log(`New tag: ${tag}`);
}

export async function publishRelease(hooks) {
  for (const target of PUBLISH_PREREQUISITES) {
    const version = hooks.version(target);
    if (!(await hooks.published(target, version))) {
      await hooks.publish(target, version);
      await hooks.waitUntilPublished(target, version);
    }
    await hooks.ensureTag(target, version);
  }
  await hooks.publishRemainder();
}

async function main() {
  await publishRelease({
    version: (target) => packageVersion(target.directory),
    published: (target, version) => published(target.name, version),
    publish: publishPackage,
    waitUntilPublished: (target, version) =>
      waitUntilPublished(target.name, version),
    ensureTag: (target, version) => ensureTag(target.name, version),
    publishRemainder: async () => {
      const result = command('pnpm', ['changeset', 'publish']);
      if (result.status !== 0) throw new Error('changeset publish failed');
    },
  });
}

if (fileURLToPath(import.meta.url) === process.argv[1]) {
  await main();
}
