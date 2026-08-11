// SPDX-License-Identifier: Apache-2.0
import type { DurableObjectState } from '@cloudflare/workers-types';
import { describe, expect, it } from 'vitest';

import {
  deploymentIdentityDatabase,
  deploymentIdentityRequest,
  TEST_DEPLOYMENT_IDENTITY_SECRET,
} from '../../test-support/deployment-identity.js';
import type { WebSocketLike } from './cf-types.js';
import {
  HUB_INSTANCE_NAME,
  HubDurableObject,
  type HubStreamEvent,
} from './hub-do.js';

interface SocketSpy {
  send(data: string): void;
  deserializeAttachment?(): unknown;
  sent: string[];
}

function socketSpy(attachment?: unknown, throwOnSend = false): SocketSpy {
  const sent: string[] = [];
  return {
    sent,
    send(data: string): void {
      // workerd throws from send() on a CLOSING/CLOSED socket; getWebSockets()
      // can still return it mid-close. The fan-out must survive that.
      if (throwOnSend) throw new Error('socket is closing');
      sent.push(data);
    },
    deserializeAttachment:
      attachment === undefined ? undefined : () => attachment,
  };
}

// The base is abstract (extend, do not instantiate); a bare subclass is the
// per-host binding target (ShowcaseHub / FlowsafeHub / DemoHub, M-008/M-009).
class TestHub extends HubDurableObject {}

function hubWith(name: string | undefined, sockets: SocketSpy[]): TestHub {
  // The WS path never runs in node (no acceptWebSocket), so the stub carries
  // only id.name + getWebSockets — the surface #route/#broadcastPresence read.
  const state = {
    id: { name },
    getWebSockets: () => sockets,
  } as unknown as DurableObjectState;
  return new TestHub(state, {
    DEPLOYMENT_TENANT: 'acme',
    DEPLOYMENT_IDENTITY_SECRET: TEST_DEPLOYMENT_IDENTITY_SECRET,
    DB: deploymentIdentityDatabase(),
  });
}

function postEvent(event: unknown): Request {
  return deploymentIdentityRequest('http://hub/internal/event', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(event),
  });
}

describe('HubDurableObject.fetch', () => {
  it('rejects a cross-deployment caller before fixed-hub fan-out', async () => {
    const socket = socketSpy();
    const hub = hubWith(HUB_INSTANCE_NAME, [socket]);
    const denied = await hub.fetch(
      deploymentIdentityRequest(
        'http://hub/internal/event',
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            type: 'decided',
            record: { id: 'apr-1' },
          }),
        },
        'different-deployment-identity-secret',
      ),
    );

    expect(denied.status).toBe(503);
    expect(socket.sent).toEqual([]);
  });

  it('fans a posted stream event out to every subscribed socket', async () => {
    // #given — two deployment subscribers
    const a = socketSpy();
    const b = socketSpy();
    const hub = hubWith(HUB_INSTANCE_NAME, [a, b]);
    const event: HubStreamEvent = {
      type: 'decided',
      record: { id: 'apr-1' },
    };

    // #when
    const response = await hub.fetch(postEvent(event));

    // #then — 200 and exactly one queue frame per socket
    expect(response.status).toBe(200);
    for (const s of [a, b]) {
      expect(s.sent).toHaveLength(1);
      const frame = JSON.parse(s.sent[0] ?? '{}') as {
        type: string;
        event: HubStreamEvent;
      };
      expect(frame.type).toBe('queue');
      expect(frame.event.record.id).toBe('apr-1');
    }
  });

  it('keeps fanning out when one socket throws on send (per-socket isolation, F2)', async () => {
    // #given — a CLOSING socket (throws on send) ahead of a healthy one, both
    // on the deployment hub. Without per-socket isolation the throw aborts the loop
    // and the healthy subscriber never receives the frame.
    const dead = socketSpy(undefined, true);
    const healthy = socketSpy();
    const hub = hubWith(HUB_INSTANCE_NAME, [dead, healthy]);

    // #when
    const response = await hub.fetch(
      postEvent({ type: 'decided', record: { id: 'apr-1' } }),
    );

    // #then — the throw is swallowed, the healthy subscriber still gets the frame
    expect(response.status).toBe(200);
    expect(healthy.sent).toHaveLength(1);
    const frame = JSON.parse(healthy.sent[0] ?? '{}') as { type: string };
    expect(frame.type).toBe('queue');
  });

  it('re-broadcasts presence past a throwing socket (per-socket isolation, F2)', () => {
    // #given — a departing socket, a throwing (CLOSING) subscriber, and a
    // healthy one. The roster must still reach the healthy subscriber.
    const leaving = socketSpy({ actorId: 'carol', role: 'reviewer' });
    const dead = socketSpy({ actorId: 'dead', role: 'reviewer' }, true);
    const healthy = socketSpy({ actorId: 'erin', role: 'reviewer' });
    const hub = hubWith(HUB_INSTANCE_NAME, [leaving, dead, healthy]);

    // #when — carol's socket closes
    hub.webSocketClose(leaving as unknown as WebSocketLike, 1000, 'bye');

    // #then — erin still receives the roster (excluding carol), dead's throw
    // did not abort the broadcast
    expect(healthy.sent).toHaveLength(1);
    const frame = JSON.parse(healthy.sent[0] ?? '{}') as {
      type: string;
      roster: { actorId: string; role: string }[];
    };
    expect(frame.type).toBe('presence');
    expect(frame.roster).toEqual([
      { actorId: 'dead', role: 'reviewer' },
      { actorId: 'erin', role: 'reviewer' },
    ]);
  });

  it('refuses an event sent to an unsanctioned hub instance', async () => {
    const s = socketSpy();
    const hub = hubWith('other-hub', [s]);

    const response = await hub.fetch(
      postEvent({ type: 'decided', record: { id: 'apr-1' } }),
    );

    expect(response.status).toBe(500);
    expect(s.sent).toHaveLength(0);
  });

  it('refuses a posted event with no record (400)', async () => {
    // #given
    const s = socketSpy();
    const hub = hubWith(HUB_INSTANCE_NAME, [s]);

    // #when — a malformed event body
    const response = await hub.fetch(postEvent({ type: 'decided' }));

    // #then
    expect(response.status).toBe(400);
    expect(s.sent).toHaveLength(0);
  });

  it('fails closed (500) and fans nothing out when the hub has no id.name', async () => {
    // #given — a hub with no named identity
    const s = socketSpy();
    const hub = hubWith(undefined, [s]);

    // #when — a well-formed event
    const response = await hub.fetch(
      postEvent({ type: 'decided', record: { id: 'apr-1' } }),
    );

    // #then — the deployment assertion throws → 500, nothing sent
    expect(response.status).toBe(500);
    expect(s.sent).toHaveLength(0);
  });

  it('returns a 426 non-WS fallback on /subscribe without the hibernation API', async () => {
    // #given — a node hub (no acceptWebSocket). Subscribing is workerd-only and
    // proven by the spike (M-009); off workerd it must degrade, never 500.
    const hub = hubWith(HUB_INSTANCE_NAME, []);

    // #when — a websocket upgrade attempt
    const response = await hub.fetch(
      deploymentIdentityRequest('http://hub/subscribe', {
        headers: { Upgrade: 'websocket' },
      }),
    );

    // #then — 426 Upgrade Required (poll the queue route instead)
    expect(response.status).toBe(426);
  });

  it('re-broadcasts the presence roster on close, excluding the departed socket', () => {
    // #given — two subscribers, each carrying a presence attachment
    const alice = socketSpy({ actorId: 'alice', role: 'reviewer' });
    const bob = socketSpy({ actorId: 'bob', role: 'reviewer' });
    const hub = hubWith(HUB_INSTANCE_NAME, [alice, bob]);

    // #when — bob's socket closes (workerd wakes the DO with this handler)
    hub.webSocketClose(bob as unknown as WebSocketLike, 1000, 'bye');

    // #then — alice receives a presence frame listing only alice; the departing
    // socket is excluded even though getWebSockets() may still return it
    expect(alice.sent).toHaveLength(1);
    const frame = JSON.parse(alice.sent[0] ?? '{}') as {
      type: string;
      roster: { actorId: string; role: string }[];
    };
    expect(frame.type).toBe('presence');
    expect(frame.roster).toEqual([{ actorId: 'alice', role: 'reviewer' }]);

    // #then — the departing socket is not re-sent to
    expect(bob.sent).toHaveLength(0);
  });
});
