// SPDX-License-Identifier: Apache-2.0

import { expect, it } from 'vitest';
import {
  restProjection,
  zoneAuthorityResponse,
} from './fixtures/cloudflare-fetch-fixture.js';
import { providerWorld } from './fixtures/provider-world.js';

it.each([
  '/zones?account.id=account',
  '/accounts/account/d1/database',
  '/accounts/account/workers/scripts',
  '/accounts/account/workers/durable_objects/namespaces',
  '/accounts/account/workers/scripts/page-script/secrets',
  '/accounts/account/workers/scripts/page-script/versions',
])('treats explicit first page as the initial provider page: %s', async (path) => {
  const world = providerWorld();
  world.zones.push({ id: 'page-zone' });
  world.createDatabase('page-database');
  world.applyUpload({
    scriptName: 'page-script',
    mode: 'initial',
    tag: 'page-tag',
    mainModule: 'worker.js',
    modules: [{ name: 'worker.js', content: 'export default {}' }],
    bindings: [
      { type: 'secret_text', name: 'SECRET', text: 'inert' },
      {
        type: 'durable_object_namespace',
        name: 'RUNNER',
        class_name: 'Runner',
      },
    ],
  });
  const handler = restProjection(world);
  async function page(number?: number) {
    const url = new URL(`https://api.cloudflare.com/client/v4${path}`);
    if (number !== undefined) url.searchParams.set('page', String(number));
    const response = await handler({
      method: 'GET',
      url: url.href,
      headers: new Headers(),
      body: undefined,
      redirect: 'manual',
    });
    const value = (await response.json()) as {
      errors: unknown;
      messages: unknown;
      result: unknown[] | { items: unknown[] };
    };
    if (path.endsWith('/versions')) {
      expect(value.errors).toBeNull();
      expect(value.messages).toBeNull();
    }
    return Array.isArray(value.result) ? value.result : value.result.items;
  }
  const initial = await page();
  expect(initial).toHaveLength(1);
  expect(await page(1)).toEqual(initial);
  expect(await page(2)).toEqual([]);
});

it('answers a repeated zone type query with no rows', async () => {
  const response = zoneAuthorityResponse(
    new URL(
      'https://api.cloudflare.com/client/v4/zones?account.id=account&type=full&type=partial',
    ),
    ['zone'],
  );
  expect(await response?.json()).toMatchObject({
    result: [],
    result_info: { total_count: 0 },
  });
});

it('filters a single zone type query', async () => {
  const response = zoneAuthorityResponse(
    new URL(
      'https://api.cloudflare.com/client/v4/zones?account.id=account&type=partial',
    ),
    [
      { id: 'full-zone', type: 'full' },
      { id: 'partial-zone', type: 'partial' },
    ],
  );
  expect(await response?.json()).toMatchObject({
    result: [{ id: 'partial-zone', type: 'partial' }],
    result_info: { total_count: 1 },
  });
});
