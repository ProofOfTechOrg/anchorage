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

// `error` is carried because a spawn that never launches (missing binary,
// unreadable cwd) reports status AND signal as null, so a caller inspecting
// only those two reports a signal kill that never happened and drops the real
// cause. Exported so the release path and its pre-flight check share one spawn
// policy rather than one verifying a shape the other does not use.
export function command(program, args, options = {}) {
  const result = spawnSync(program, args, {
    cwd: options.cwd ?? ROOT,
    encoding: 'utf8',
    ...(options.capture ? {} : { stdio: 'inherit' }),
  });
  return {
    status: result.status,
    signal: result.signal,
    error: result.error,
    stdout: result.stdout ?? '',
    stderr: result.stderr ?? '',
  };
}

/** Human-readable cause for a spawn result that did not exit 0. */
export function failureReason(result) {
  if (result.error) return result.error.message;
  if (result.signal) return `killed by ${result.signal}`;
  return `exit ${result.status}`;
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

/**
 * Scope the publish to one package by SPAWN DIRECTORY, never by pnpm's global
 * `--dir`/`-C` flag: `pnpm publish` does not consume that flag, so pnpm
 * forwards both the directory and the subcommand name into the underlying
 * `npm publish` argv. npm then sees three positionals where it accepts one
 * (`publish --ignore-scripts <tarball> packages/breakwater publish ...`) and
 * exits EUSAGE before contacting the registry.
 *
 * Only the publish runs in the package directory. Version lookup, tagging, and
 * the Changesets remainder stay at ROOT, where the workspace and git dir are.
 */
export function publishInvocation(target, options = {}) {
  return {
    args: [
      'publish',
      '--access',
      'public',
      '--tag',
      'latest',
      '--no-git-checks',
      // Owned here rather than appended by the caller, so the pre-flight check
      // verifies the argv this function actually produces.
      ...(options.dryRun ? ['--dry-run'] : []),
    ],
    cwd: join(ROOT, target.directory),
  };
}

function publishPackage(target, version) {
  const { args, cwd } = publishInvocation(target);
  const result = command('pnpm', args, { cwd });
  if (result.status !== 0) {
    throw new Error(
      `publish failed for ${target.name}@${version}: ${failureReason(result)}`,
    );
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

/**
 * The line `changesets/action` greps out of this command's stdout to decide
 * which tags to push and which GitHub releases to create.
 *
 * This is a string contract, not a log message. Breakwater publishes OUTSIDE
 * `changeset publish`, so nothing else announces its tag — if this format ever
 * drifts from the action's regex, npm publish still succeeds, the tag never
 * reaches origin, no release is created, and the run exits 0. Silent, unlike
 * every other failure in this file. `publish-ordered.test.mjs` pins it against
 * the action's own pattern.
 */
export function newTagAnnouncement(tag) {
  return `New tag: ${tag}`;
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
  if (tagged.status !== 0) {
    throw new Error(`failed to create tag ${tag}: ${failureReason(tagged)}`);
  }
  console.log(newTagAnnouncement(tag));
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
