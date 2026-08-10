// Node-testable coverage of the dev host's in-memory live-stream hub
// (CI-M-008-005) — the ONE genuinely new piece of logic run-api-dev-plugin.ts
// owns for streaming. Everything else the plugin wires (approvalRouter,
// runRouter and streamRouter) reuses already-tested flowsafe
// primitives and the SAME composition worker.fetch.e2e.test.ts exercises for
// the deployed host, so this file targets createInMemoryHub specifically.
//
// A relative import into the package-root file: run-api-dev-plugin.ts's OWN
// imports must stay relative (it runs in the Vite config bundler before any
// alias exists), but that constraint is about how IT imports things, not
// about how a test imports IT — vitest resolves this relative path with
// ordinary Node module resolution, so this file can live under src/ (the only
// place vitest.config.ts's `include` glob picks up showcase tests) while
// still reaching the root-level module.

import { describe, expect, it } from 'vitest';

import { createInMemoryHub } from '../run-api-dev-plugin.js';

describe('createInMemoryHub', () => {
  it('accumulates deployment events in publish order', async () => {
    // #given
    const hub = createInMemoryHub();
    const stub = hub.namespace.get(hub.namespace.idFromName('deployment'));

    // #when two events publish in sequence
    await stub.fetch('http://hub/internal/event', {
      method: 'POST',
      body: JSON.stringify({ type: 'created', record: { id: 'first' } }),
    });
    await stub.fetch('http://hub/internal/event', {
      method: 'POST',
      body: JSON.stringify({ type: 'decided', record: { id: 'second' } }),
    });

    // #then both are recorded, in order
    expect(hub.published.get('deployment')).toEqual([
      { type: 'created', record: { id: 'first' } },
      { type: 'decided', record: { id: 'second' } },
    ]);
  });

  it('answers 426 for anything other than a POST /internal/event (the WS-upgrade path this dev host cannot serve)', async () => {
    // #given
    const hub = createInMemoryHub();
    const stub = hub.namespace.get(hub.namespace.idFromName('deployment'));

    // #when / #then a GET /subscribe (what a real WS upgrade would hit)
    const subscribe = await stub.fetch('http://hub/subscribe', {
      method: 'GET',
    });
    expect(subscribe.status).toBe(426);

    // #when / #then an unrelated path
    const other = await stub.fetch('http://hub/nope', { method: 'POST' });
    expect(other.status).toBe(426);

    // #then neither call published anything
    expect(hub.published.get('deployment')).toBeUndefined();
  });
});
