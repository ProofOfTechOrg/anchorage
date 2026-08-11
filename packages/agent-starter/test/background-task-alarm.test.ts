// SPDX-License-Identifier: Apache-2.0

import type { DurableObjectState } from '@cloudflare/workers-types';
import {
  DEPLOYMENT_IDENTITY_HEADER,
  EXECUTION_PRINCIPAL_HEADER,
} from '@proofoftech/flowsafe/do-runner';
import { describe, expect, it, vi } from 'vitest';
import {
  BACKGROUND_TASKS_INSTANCE_NAME,
  StarterBackgroundTasks,
} from '../src/durable-objects.js';

const DEPLOYMENT_IDENTITY_SECRET = 'test-deployment-identity-secret-0001';

const SENTINEL_SQL = `CREATE TABLE flowsafe_deployment (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  tenant_tag TEXT NOT NULL,
  provisioned_at TEXT NOT NULL
)`;

function identityDatabase(tag = 'other'): Env['DB'] {
  return {
    prepare(query: string) {
      const statement = {
        bind: () => statement,
        all: async () => {
          if (query.includes('sqlite_schema')) {
            return { results: [{ sql: SENTINEL_SQL }] };
          }
          if (query.startsWith('PRAGMA')) {
            return {
              results: [
                { name: 'id', type: 'INTEGER', notnull: 0, pk: 1 },
                { name: 'tenant_tag', type: 'TEXT', notnull: 1, pk: 0 },
                { name: 'provisioned_at', type: 'TEXT', notnull: 1, pk: 0 },
              ],
            };
          }
          if (query.startsWith('SELECT id')) {
            return { results: [{ id: 1, tenant_tag: tag }] };
          }
          throw new Error(`unexpected query: ${query}`);
        },
      };
      return statement;
    },
  } as unknown as Env['DB'];
}

describe('StarterBackgroundTasks alarm identity guard', () => {
  it('re-arms before a deployment identity failure consumes the alarm', async () => {
    const setAlarm = vi.fn();
    const state = {
      id: { name: BACKGROUND_TASKS_INSTANCE_NAME },
      storage: { setAlarm },
    } as unknown as DurableObjectState;
    const object = new StarterBackgroundTasks(state, {
      DB: identityDatabase(),
      DEPLOYMENT_TENANT: 'acme',
      DEPLOYMENT_IDENTITY_SECRET,
    } as Env);

    await expect(object.alarm()).rejects.toThrow(/belongs to 'other'/);
    expect(setAlarm).toHaveBeenCalledTimes(1);
  });

  it.each([
    ['missing', undefined],
    ['malformed', 'not-json'],
  ])('maps a %s trusted-principal header to 403', async (_label, principal) => {
    const state = {
      id: { name: BACKGROUND_TASKS_INSTANCE_NAME },
      storage: { setAlarm: vi.fn() },
    } as unknown as DurableObjectState;
    const object = new StarterBackgroundTasks(state, {
      DB: identityDatabase('acme'),
      DEPLOYMENT_TENANT: 'acme',
      DEPLOYMENT_IDENTITY_SECRET,
    } as Env);
    const headers = new Headers({
      [DEPLOYMENT_IDENTITY_HEADER]: DEPLOYMENT_IDENTITY_SECRET,
    });
    if (principal !== undefined)
      headers.set(EXECUTION_PRINCIPAL_HEADER, principal);

    const response = await object.fetch(
      new Request('http://background/tasks', { headers }),
    );

    expect(response.status).toBe(403);
  });
});
