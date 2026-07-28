import assert from 'node:assert/strict';
import test from 'node:test';
import { PUBLISH_PREREQUISITES, publishRelease } from './publish-ordered.mjs';

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
