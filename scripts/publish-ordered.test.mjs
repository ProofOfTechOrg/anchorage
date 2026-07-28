import assert from 'node:assert/strict';
import { isAbsolute, sep } from 'node:path';
import test from 'node:test';
import {
  newTagAnnouncement,
  PUBLISH_PREREQUISITES,
  publishInvocation,
  publishRelease,
} from './publish-ordered.mjs';

/**
 * Deliberately models nothing about how pnpm consumes flags — that varies by
 * version, and an earlier version of this file that DID model it treated
 * `--dir` as value-taking, swallowed the leaked path, and passed on the broken
 * argv. This asserts the property that holds regardless: the publish argv names
 * no filesystem location at all. See `publishInvocation` in publish-ordered.mjs
 * for why a leaked path breaks the release.
 */
function pathLike(args) {
  return args.filter(
    (arg) => arg.includes('/') || arg.includes('\\') || arg.startsWith('.'),
  );
}

/** Scoping flags in any spelling pnpm accepts, including `--flag=value`. */
function scopingFlags(args) {
  return args.filter((arg) =>
    ['--dir', '-C', '--filter', '-F', '--prefix'].some(
      (flag) => arg === flag || arg.startsWith(`${flag}=`),
    ),
  );
}

/**
 * The exact pattern `changesets/action` applies to this command's stdout to
 * decide which tags to push and which releases to create (its `src/run.ts`).
 * Mirrored by value: drift in either direction must fail here, because the
 * production symptom is silent — published to npm, never tagged, exit 0.
 */
const CHANGESETS_NEW_TAG = /New tag:\s+(@[^/]+\/[^@]+|[^/]+)@([^\s]+)/;

test('publishes Breakwater before the remaining Changesets release', async () => {
  const calls = [];
  await publishRelease({
    version: (target) => {
      calls.push(`version:${target.name}`);
      return '0.6.0';
    },
    published: async (target) => {
      calls.push(`lookup:${target.name}`);
      return false;
    },
    publish: async (target) => calls.push(`publish:${target.name}`),
    waitUntilPublished: async (target) => calls.push(`visible:${target.name}`),
    ensureTag: async (target) => calls.push(`tag:${target.name}`),
    publishRemainder: async () => calls.push('changesets'),
  });

  assert.deepEqual(
    PUBLISH_PREREQUISITES.map((target) => target.name),
    ['@proofoftech/breakwater'],
  );
  assert.deepEqual(calls, [
    'version:@proofoftech/breakwater',
    'lookup:@proofoftech/breakwater',
    'publish:@proofoftech/breakwater',
    'visible:@proofoftech/breakwater',
    'tag:@proofoftech/breakwater',
    'changesets',
  ]);
});

test('an already published prerequisite remains an ordered no-op', async () => {
  const calls = [];
  await publishRelease({
    version: () => '0.6.0',
    published: async () => true,
    publish: async () => calls.push('unexpected publish'),
    waitUntilPublished: async () => calls.push('unexpected wait'),
    ensureTag: async () => calls.push('tag check'),
    publishRemainder: async () => calls.push('changesets'),
  });

  assert.deepEqual(calls, ['tag check', 'changesets']);
});

// Both variants, because the dry-run pre-flight publishes with `dryRun: true`
// and a leak reachable only on the real path would never be spawned in CI.
for (const options of [{}, { dryRun: true }]) {
  const label = options.dryRun ? 'dry-run' : 'release';

  test(`the ${label} publish argv names no directory`, () => {
    for (const target of PUBLISH_PREREQUISITES) {
      const { args } = publishInvocation(target, options);
      assert.equal(args[0], 'publish');
      assert.deepEqual(
        pathLike(args),
        [],
        `${target.name} would forward a path into npm publish: ${args.join(' ')}`,
      );
      assert.deepEqual(
        scopingFlags(args),
        [],
        `${target.name} must be scoped by spawn directory, not a flag: ${args.join(' ')}`,
      );
    }
  });

  test(`the ${label} publish runs in the package directory`, () => {
    for (const target of PUBLISH_PREREQUISITES) {
      const { cwd } = publishInvocation(target, options);
      assert.ok(isAbsolute(cwd), `${target.name} publish cwd must be absolute`);
      assert.ok(
        cwd.endsWith(target.directory.split('/').join(sep)),
        `${target.name} publish cwd must be its package directory, got ${cwd}`,
      );
    }
  });
}

test('only the dry-run variant carries --dry-run', () => {
  for (const target of PUBLISH_PREREQUISITES) {
    assert.ok(
      !publishInvocation(target).args.includes('--dry-run'),
      `${target.name} would never actually publish`,
    );
    assert.ok(
      publishInvocation(target, { dryRun: true }).args.includes('--dry-run'),
      `${target.name} pre-flight would publish for real`,
    );
  }
});

test('the tag announcement matches the pattern changesets/action greps for', () => {
  for (const target of PUBLISH_PREREQUISITES) {
    const announcement = newTagAnnouncement(`${target.name}@9.9.9`);
    const match = CHANGESETS_NEW_TAG.exec(announcement);
    assert.ok(
      match,
      `changesets/action would not push a tag for ${announcement}`,
    );
    assert.equal(match[1], target.name);
    assert.equal(match[2], '9.9.9');
  }
});
