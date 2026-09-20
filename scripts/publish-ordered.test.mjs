import assert from 'node:assert/strict';
import { isAbsolute, sep } from 'node:path';
import test from 'node:test';
import {
  command,
  newTagAnnouncement,
  PROBE_TIMEOUT_MS,
  PUBLISH_PREREQUISITES,
  peerFloorGrammarViolations,
  prerequisitePeerFloorViolations,
  published,
  publishInvocation,
  publishRelease,
  VISIBILITY_DEADLINE_MS,
  VISIBILITY_POLL_MS,
  viewInvocation,
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

/**
 * The visibility wait is a real-time loop against a registry that takes
 * minutes, so it is exercised on a clock the wait itself advances: `sleep`
 * records its duration and moves `now` forward by it.
 */
function fakeClock() {
  const sleeps = [];
  let current = 0;
  return {
    advance: (ms) => {
      current += ms;
    },
    current: () => current,
    sleeps,
    seams: {
      now: () => current,
      sleep: async (ms) => {
        sleeps.push(ms);
        current += ms;
      },
    },
  };
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

test('the visibility probe revalidates the packument and bounds its fetch', () => {
  const args = viewInvocation('@proofoftech/breakwater', '9.9.9');

  assert.deepEqual(args.slice(0, 4), [
    'view',
    '@proofoftech/breakwater@9.9.9',
    'version',
    '--json',
  ]);
  for (const flag of [
    '--prefer-online',
    '--fetch-timeout=30000',
    '--fetch-retries=1',
    '--fetch-retry-mintimeout=1000',
    '--fetch-retry-maxtimeout=5000',
  ]) {
    assert.ok(
      args.includes(flag),
      `a probe without ${flag} runs on npm's own defaults: ${args.join(' ')}`,
    );
  }
});

test('the publication lookup pins its process policy and classifies completed results', () => {
  const calls = [];
  const run = (program, args, options) => {
    calls.push({ program, args, options });
    return {
      error: undefined,
      signal: null,
      status: 0,
      stderr: 'npm warning stays diagnostic',
      stdout: '"9.9.9"',
    };
  };

  assert.equal(published('package', '9.9.9', run), true);
  assert.equal(
    published('package', '1.0.0', (_program, _args, options) => ({
      error: undefined,
      signal: null,
      status: 0,
      stderr: '',
      stdout: '"2.0.0"',
      options,
    })),
    false,
  );
  assert.deepEqual(calls, [
    {
      program: 'npm',
      args: viewInvocation('package', '9.9.9'),
      options: {
        capture: true,
        killSignal: 'SIGKILL',
        timeout: PROBE_TIMEOUT_MS,
      },
    },
  ]);
  assert.equal(
    published('package', '9.9.9', () => ({
      error: undefined,
      signal: null,
      status: 1,
      stderr: 'npm error code E404',
      stdout: '',
    })),
    false,
  );
  assert.throws(
    () =>
      published('package', '9.9.9', () => ({
        error: undefined,
        signal: null,
        status: 0,
        stderr: '',
        stdout: 'not json',
      })),
    SyntaxError,
  );
});

test('incomplete publication lookups stay operational failures despite E404 output', () => {
  const incompleteResults = [
    {
      error: new Error('spawn failed'),
      signal: null,
      status: null,
      stderr: '',
      stdout: 'E404',
    },
    {
      error: undefined,
      signal: 'SIGKILL',
      status: null,
      stderr: 'E404',
      stdout: '',
    },
    {
      error: undefined,
      signal: null,
      status: null,
      stderr: '',
      stdout: '404 Not Found',
    },
  ];

  for (const result of incompleteResults) {
    assert.throws(
      () => published('package', '9.9.9', () => result),
      /npm view failed for package@9\.9\.9/u,
    );
  }
});

test('the command seam forwards the hard timeout signal to a real child', () => {
  const started = performance.now();
  const result = command(
    process.execPath,
    ['-e', "process.on('SIGTERM', () => {}); setInterval(() => {}, 1_000)"],
    { capture: true, killSignal: 'SIGKILL', timeout: 50 },
  );

  assert.equal(result.signal, 'SIGKILL');
  assert.equal(result.error?.code, 'ETIMEDOUT');
  assert.ok(performance.now() - started < 2_000);
});

test('lookup failures stop publishing and remain retryable during visibility polling', async () => {
  const partial = {
    error: undefined,
    signal: 'SIGKILL',
    status: null,
    stderr: '',
    stdout: 'E404',
  };
  const releaseCalls = [];
  await assert.rejects(
    publishRelease({
      peerFloors: async () => {},
      version: () => '9.9.9',
      published: () => published('package', '9.9.9', () => partial),
      publish: async () => releaseCalls.push('publish'),
      waitUntilPublished: async () => releaseCalls.push('wait'),
      ensureTag: async () => releaseCalls.push('tag'),
      publishRemainder: async () => releaseCalls.push('remainder'),
    }),
    /killed by SIGKILL/u,
  );
  assert.deepEqual(releaseCalls, []);

  const clock = fakeClock();
  let attempts = 0;
  await waitUntilPublished('package', '9.9.9', {
    ...clock.seams,
    isPublished: (name, version) =>
      published(name, version, () => {
        attempts += 1;
        return attempts === 1
          ? partial
          : {
              error: undefined,
              signal: null,
              status: 0,
              stderr: '',
              stdout: '"9.9.9"',
            };
      }),
  });
  assert.equal(attempts, 2);
  assert.deepEqual(clock.sleeps, [VISIBILITY_POLL_MS]);
});

// The floor is the 2026-09-18 release's measured 8m32s from acceptance to
// visibility, so a deadline cut below what has already been observed reds here
// rather than on the next release.
test('the visibility deadline clears the measured publish-to-visibility lag', () => {
  assert.ok(
    VISIBILITY_DEADLINE_MS > 512_000,
    `${VISIBILITY_DEADLINE_MS}ms leaves no margin over a wait already measured at 8m32s`,
  );
});

test('a prerequisite already visible on the first probe never sleeps', async () => {
  const clock = fakeClock();

  await waitUntilPublished('@proofoftech/breakwater', '9.9.9', {
    ...clock.seams,
    isPublished: () => true,
  });

  assert.deepEqual(clock.sleeps, []);
});

test('the default monotonic clock remains callable by the visibility wait', async () => {
  await waitUntilPublished('package', '1.0.0', { isPublished: () => true });
});

test('a prerequisite that appears later is polled at the visibility interval', async () => {
  const clock = fakeClock();
  let probes = 0;

  await waitUntilPublished('@proofoftech/flowsafe', '9.9.9', {
    ...clock.seams,
    isPublished: () => {
      probes += 1;
      return probes === 3;
    },
  });

  assert.equal(probes, 3);
  assert.deepEqual(clock.sleeps, [VISIBILITY_POLL_MS, VISIBILITY_POLL_MS]);
});

test('an asynchronous probe is awaited rather than read as a truthy promise', async () => {
  const clock = fakeClock();
  let probes = 0;

  await waitUntilPublished('@proofoftech/breakwater', '9.9.9', {
    ...clock.seams,
    isPublished: async () => ++probes === 2,
  });

  assert.equal(probes, 2);
  assert.deepEqual(clock.sleeps, [VISIBILITY_POLL_MS]);
});

// A version that appears in the last poll interval is still published, so the
// deadline is what the wait probes at, not what it stops short of.
test('an expiring wait probes at the deadline before it gives up', async () => {
  const clock = fakeClock();
  let probes = 0;

  await assert.rejects(
    waitUntilPublished('@proofoftech/breakwater', '9.9.9', {
      ...clock.seams,
      isPublished: () => {
        probes += 1;
        clock.advance(1_000);
        return false;
      },
    }),
    { message: '@proofoftech/breakwater@9.9.9 did not become visible on npm' },
  );

  assert.equal(clock.sleeps.at(-1), 7_000);
  assert.ok(clock.sleeps.at(-1) < VISIBILITY_POLL_MS);
  assert.equal(clock.current(), VISIBILITY_DEADLINE_MS + 1_000);
  assert.equal(probes, 114);
});

test('an expired wait names a probe that failed during it', async () => {
  const clock = fakeClock();
  const probeError = new Error('npm view failed: E500');
  let probes = 0;

  await assert.rejects(
    waitUntilPublished('@proofoftech/flowsafe', '9.9.9', {
      ...clock.seams,
      isPublished: () => {
        probes += 1;
        if (probes === 1) throw probeError;
        return false;
      },
    }),
    (error) => {
      assert.equal(
        error.message,
        '@proofoftech/flowsafe@9.9.9 did not become visible on npm (a probe failed during the wait: npm view failed: E500)',
      );
      assert.equal(error.cause, probeError);
      return true;
    },
  );
});

test('a probe that throws counts as not yet visible', async () => {
  const clock = fakeClock();
  let probes = 0;

  await waitUntilPublished('@proofoftech/breakwater', '9.9.9', {
    ...clock.seams,
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

test('manifests with no prerequisite peer edge fail the gate closed', () => {
  const violations = prerequisitePeerFloorViolations(
    new Map([
      ['@proofoftech/breakwater', { version: '0.13.0' }],
      ['@proofoftech/flowsafe', { version: '0.21.0' }],
    ]),
  );

  assert.equal(violations.length, 1);
  assert.match(violations[0], /peer-floor gates would verify nothing/);
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
