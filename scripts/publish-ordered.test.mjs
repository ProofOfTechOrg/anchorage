import assert from 'node:assert/strict';
import { isAbsolute, sep } from 'node:path';
import test from 'node:test';
import {
  newTagAnnouncement,
  PUBLISH_PREREQUISITES,
  peerFloorGrammarViolations,
  prerequisitePeerFloorViolations,
  publishInvocation,
  publishRelease,
  VISIBILITY_DEADLINE_MS,
  VISIBILITY_POLL_MS,
  waitUntilPublished,
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

function peerFloorFixture({
  breakwaterVersion = '0.13.0',
  flowsafeRange = '>=0.13.0 <1.0.0',
} = {}) {
  return new Map([
    ['@proofoftech/breakwater', { version: breakwaterVersion }],
    [
      '@proofoftech/flowsafe',
      {
        peerDependencies: {
          '@proofoftech/breakwater': flowsafeRange,
        },
      },
    ],
  ]);
}

test('publishes Breakwater then Flowsafe before the remaining Changesets release', async () => {
  const calls = [];
  await publishRelease({
    peerFloors: async () => calls.push('peer-floors'),
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
    ['@proofoftech/breakwater', '@proofoftech/flowsafe'],
  );
  assert.deepEqual(calls, [
    'peer-floors',
    'version:@proofoftech/breakwater',
    'lookup:@proofoftech/breakwater',
    'publish:@proofoftech/breakwater',
    'visible:@proofoftech/breakwater',
    'tag:@proofoftech/breakwater',
    'version:@proofoftech/flowsafe',
    'lookup:@proofoftech/flowsafe',
    'publish:@proofoftech/flowsafe',
    'visible:@proofoftech/flowsafe',
    'tag:@proofoftech/flowsafe',
    'changesets',
  ]);
});

test('an already published prerequisite remains an ordered no-op', async () => {
  const calls = [];
  await publishRelease({
    peerFloors: async () => calls.push('peer-floors'),
    version: () => '0.6.0',
    published: async () => true,
    publish: async () => calls.push('unexpected publish'),
    waitUntilPublished: async () => calls.push('unexpected wait'),
    ensureTag: async () => calls.push('tag check'),
    publishRemainder: async () => calls.push('changesets'),
  });

  assert.deepEqual(calls, [
    'peer-floors',
    ...PUBLISH_PREREQUISITES.map(() => 'tag check'),
    'changesets',
  ]);
});

/**
 * The visibility wait is a real-time loop against a registry that takes
 * minutes, so it is exercised on a clock the wait itself advances: `sleep`
 * records its duration and moves `now` forward by it. Nothing here waits on a
 * real timer, and nothing asserts a magnitude for the exported constants — the
 * budget is a judgement call, the loop's use of it is not.
 */
function fakeClock() {
  const sleeps = [];
  let current = 0;
  return {
    sleeps,
    now: () => current,
    sleep: async (ms) => {
      sleeps.push(ms);
      current += ms;
    },
  };
}

test('a prerequisite already visible on the first probe never sleeps', async () => {
  const clock = fakeClock();

  await waitUntilPublished('@proofoftech/breakwater', '9.9.9', {
    now: clock.now,
    sleep: clock.sleep,
    isPublished: () => true,
  });

  assert.deepEqual(clock.sleeps, []);
});

test('a prerequisite that appears later is polled at the visibility interval', async () => {
  const clock = fakeClock();
  let probes = 0;

  await waitUntilPublished('@proofoftech/flowsafe', '9.9.9', {
    now: clock.now,
    sleep: clock.sleep,
    isPublished: () => {
      probes += 1;
      return probes === 3;
    },
  });

  assert.equal(probes, 3);
  assert.deepEqual(clock.sleeps, [VISIBILITY_POLL_MS, VISIBILITY_POLL_MS]);
});

test('an expired wait names the package and the last probe failure', async () => {
  const silent = fakeClock();

  await assert.rejects(
    waitUntilPublished('@proofoftech/breakwater', '9.9.9', {
      now: silent.now,
      sleep: silent.sleep,
      isPublished: () => false,
    }),
    { message: '@proofoftech/breakwater@9.9.9 did not become visible on npm' },
  );
  assert.equal(
    silent.sleeps.reduce((total, ms) => total + ms, 0),
    VISIBILITY_DEADLINE_MS,
  );

  const failing = fakeClock();

  await assert.rejects(
    waitUntilPublished('@proofoftech/flowsafe', '9.9.9', {
      now: failing.now,
      sleep: failing.sleep,
      isPublished: () => {
        throw new Error('npm view failed: E500');
      },
    }),
    {
      message:
        '@proofoftech/flowsafe@9.9.9 did not become visible on npm (last probe error: npm view failed: E500)',
    },
  );
});

test('a probe that throws counts as not yet visible', async () => {
  const clock = fakeClock();
  let probes = 0;

  await waitUntilPublished('@proofoftech/breakwater', '9.9.9', {
    now: clock.now,
    sleep: clock.sleep,
    isPublished: () => {
      probes += 1;
      if (probes === 1) throw new Error('npm view failed: E500');
      return true;
    },
  });

  assert.equal(probes, 2);
  assert.deepEqual(clock.sleeps, [VISIBILITY_POLL_MS]);
});

test('a satisfied prerequisite peer floor passes', () => {
  assert.deepEqual(prerequisitePeerFloorViolations(peerFloorFixture()), []);
});

test('an unsatisfied peer floor names both packages', () => {
  const violations = prerequisitePeerFloorViolations(
    peerFloorFixture({ breakwaterVersion: '0.12.0' }),
  );

  assert.equal(violations.length, 1);
  assert.match(violations[0], /@proofoftech\/breakwater/);
  assert.match(violations[0], /@proofoftech\/flowsafe/);
});

test('caret peer ranges fail closed as unsupported grammar', () => {
  const violations = peerFloorGrammarViolations(
    peerFloorFixture({ flowsafeRange: '^0.13.0' }),
  );

  assert.equal(violations.length, 1);
  assert.equal(violations[0].ownerName, '@proofoftech/flowsafe');
  assert.match(violations[0].message, /bounded exact floor/);
});

test('non-exact prerequisite versions fail grammar validation', () => {
  const violations = peerFloorGrammarViolations(
    peerFloorFixture({ breakwaterVersion: '0.13.0-beta.1' }),
  );

  assert.equal(violations.length, 1);
  assert.equal(violations[0].ownerName, '@proofoftech/breakwater');
  assert.match(violations[0].message, /version must be exact/);
});

test('a bounded 1.x peer floor passes grammar validation', () => {
  assert.deepEqual(
    peerFloorGrammarViolations(
      peerFloorFixture({
        breakwaterVersion: '1.2.0',
        flowsafeRange: '>=1.2.0 <2.0.0',
      }),
    ),
    [],
  );
});

test('a peer ceiling that does not match its floor major fails', () => {
  const violations = peerFloorGrammarViolations(
    peerFloorFixture({ flowsafeRange: '>=0.5.0 <2.0.0' }),
  );

  assert.equal(violations.length, 1);
  assert.equal(violations[0].ownerName, '@proofoftech/flowsafe');
  assert.match(violations[0].message, /ceiling/);
});

test('a peer ceiling grammar failure is not duplicated as containment', () => {
  const violations = prerequisitePeerFloorViolations(
    peerFloorFixture({
      breakwaterVersion: '0.4.0',
      flowsafeRange: '>=0.5.0 <2.0.0',
    }),
  );

  assert.deepEqual(violations, [
    '@proofoftech/flowsafe peer @proofoftech/breakwater ceiling 2.0.0 does not match floor >=0.5.0 <2.0.0',
  ]);
});

test('grammar-only validation accepts an unsatisfied floor', () => {
  assert.deepEqual(
    peerFloorGrammarViolations(
      peerFloorFixture({ breakwaterVersion: '0.12.0' }),
    ),
    [],
  );
});

for (const breakwaterVersion of ['1.0.0', '1.2.3']) {
  test(`a prerequisite version ${breakwaterVersion} at or above the ceiling fails containment`, () => {
    const manifests = peerFloorFixture({ breakwaterVersion });

    assert.deepEqual(peerFloorGrammarViolations(manifests), []);
    const violations = prerequisitePeerFloorViolations(manifests);
    assert.equal(violations.length, 1);
    assert.match(violations[0], /does not include/);
  });
}

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
