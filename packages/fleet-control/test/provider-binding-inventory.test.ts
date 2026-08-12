// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { assertSupportedProviderBindings } from '../src/provider-binding-inventory.js';

const deploymentTypes = new Set([
  'd1',
  'durable_object_namespace',
  'service',
  'queue',
  'r2_bucket',
  'plain_text',
  'secret_text',
]);

describe('provider binding inventory', () => {
  it('accepts an exact empty provider inventory', () => {
    expect(
      assertSupportedProviderBindings([], deploymentTypes, 'test Worker'),
    ).toEqual([]);
  });

  it.each([
    [{ type: 'kv_namespace', name: 'OUT_OF_BAND', namespace_id: 'kv-id' }],
    [{ type: 'hyperdrive', name: 'FUTURE_BINDING', id: 'config-id' }],
    [{ type: 'd1', name: 'DB' }],
    [{ type: 'd1', name: 'DB', database_id: '' }],
    [
      {
        type: 'durable_object_namespace',
        name: 'RUNNER',
        namespace_id: 'namespace-id',
        class_name: ' ',
      },
    ],
    [{ type: 'service', name: 'SERVICE', service: ' service-name' }],
    [{ type: 'queue', name: 'QUEUE', queue_name: '' }],
    [{ type: 'r2_bucket', name: 'ARTIFACTS', bucket_name: ' ' }],
    [{ type: 'plain_text', name: '', text: 'value' }],
    [{ type: 'plain_text', text: 'value' }],
    [null],
  ])('rejects an unconsumed or malformed provider binding %#', (binding) => {
    expect(() =>
      assertSupportedProviderBindings(
        [binding],
        deploymentTypes,
        'test Worker',
      ),
    ).toThrow(/binding/u);
  });

  it('rejects duplicate names across otherwise supported binding types', () => {
    expect(() =>
      assertSupportedProviderBindings(
        [
          { type: 'plain_text', name: 'COLLISION', text: 'value' },
          { type: 'secret_text', name: 'COLLISION' },
        ],
        deploymentTypes,
        'test Worker',
      ),
    ).toThrow(/duplicate provider binding names/u);
  });

  it('accepts an empty plain-text value', () => {
    expect(
      assertSupportedProviderBindings(
        [{ type: 'plain_text', name: 'OPTIONAL_VALUE', text: '' }],
        deploymentTypes,
        'test Worker',
      ),
    ).toEqual([{ type: 'plain_text', name: 'OPTIONAL_VALUE' }]);
  });
});
