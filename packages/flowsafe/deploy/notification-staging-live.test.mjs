import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  createWranglerControl,
  relockIfOpen,
} from './notification-staging-live.mjs';

const enabled = 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa';
const disabled = 'bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb';

function fakeCli({ enabledFlag = true } = {}) {
  const calls = [];
  let version = enabled;
  const command = (_executable, args) => {
    calls.push(args);
    if (args[0] === 'deployments') {
      return {
        status: 0,
        stdout: JSON.stringify([
          {
            created_on: '2026-10-15T00:00:00.000Z',
            versions: [{ version_id: version, percentage: 100 }],
          },
        ]),
      };
    }
    if (args[0] === 'versions') {
      return {
        status: 0,
        stdout: JSON.stringify({
          resources: {
            bindings: [
              {
                name: 'NOTIFICATION_PROOF_ENABLED',
                type: 'plain_text',
                text: version === enabled && enabledFlag ? 'true' : 'false',
              },
            ],
          },
        }),
      };
    }
    if (args[0] === 'deploy') {
      version = disabled;
      return { status: 0, stdout: 'deployed' };
    }
    if (args[0] === 'd1') {
      return {
        status: 0,
        stdout: JSON.stringify([{ success: true, results: [{ count: 0 }] }]),
      };
    }
    throw new Error('unexpected CLI');
  };
  const cli = createWranglerControl({ command, token: 'fake-token' });
  return {
    cli,
    calls,
    change() {
      version = disabled;
    },
  };
}

test('version-checked off deploy uses the tracked false config', () => {
  const fixture = fakeCli();
  assert.equal(fixture.cli.currentVersion(), enabled);
  assert.equal(fixture.cli.proofEnabled(enabled), true);
  assert.equal(
    fixture.cli.deployOffIfCurrent(enabled, '/tmp/staging-off-test'),
    disabled,
  );
  assert.equal(fixture.cli.proofEnabled(disabled), false);
  assert.equal(fixture.calls.filter((args) => args[0] === 'deploy').length, 1);
});

test('changed version or disabled flag refuses before deploy', () => {
  const changed = fakeCli();
  changed.change();
  assert.throws(
    () => changed.cli.deployOffIfCurrent(enabled, '/tmp/staging-off-test'),
    /changed/,
  );
  assert.equal(changed.calls.filter((args) => args[0] === 'deploy').length, 0);
  const disabledFlag = fakeCli({ enabledFlag: false });
  assert.throws(
    () => disabledFlag.cli.deployOffIfCurrent(enabled, '/tmp/staging-off-test'),
    /not enabled/,
  );
  assert.equal(
    disabledFlag.calls.filter((args) => args[0] === 'deploy').length,
    0,
  );
});

test('watchdog relock uses a fresh epoch and revision', async () => {
  const calls = [];
  let observedRevision = 1;
  const probe = async (method, path, body) => {
    calls.push({ method, path, body });
    if (method === 'GET')
      return {
        state: 'open',
        mutationEpoch: 1,
        transitionRevision: observedRevision,
        requireMutationEpoch: true,
      };
    return {
      state: 'migration-locked',
      mutationEpoch: 2,
      transitionRevision: 2,
      requireMutationEpoch: true,
    };
  };
  assert.equal(
    (await relockIfOpen(probe, { mutationEpoch: 1, transitionRevision: 1 }))
      .state,
    'migration-locked',
  );
  assert.deepEqual(calls[1].body, {
    expected: 'open',
    next: 'migration-locked',
    expectedMutationEpoch: 1,
    expectedRevision: 1,
    advanceMutationEpoch: true,
  });
  calls.length = 0;
  observedRevision = 2;
  await assert.rejects(
    relockIfOpen(probe, { mutationEpoch: 1, transitionRevision: 1 }),
    /metadata differs/,
  );
  assert.equal(calls.length, 1);
});
