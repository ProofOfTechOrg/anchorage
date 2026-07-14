import type { ApprovalApiClient } from '@flowsafe/approval-ui/client';
import { describe, expect, it, vi } from 'vitest';

import { createHubStreamClient } from '@/hub-stream-client';

// A stub ApprovalApiClient exposing only streamTicket (all createHubStreamClient
// touches), returning a canned address so the test pins the URL SHAPING.
function stubClient(serverUrl: string): {
  client: ApprovalApiClient;
  streamTicket: ReturnType<typeof vi.fn>;
} {
  const streamTicket = vi.fn(
    async (_channel: 'hub' | 'run', _runId?: string, _workflowId?: string) => ({
      url: serverUrl,
      ticket: 'TICKET.SIG',
      expiresAt: 1_000,
    }),
  );
  return {
    client: { streamTicket } as unknown as ApprovalApiClient,
    streamTicket,
  };
}

describe('createHubStreamClient', () => {
  it('shapes the hub address so url + ticket is a valid WS URL', async () => {
    // #given the server returns the hub channel url
    const { client, streamTicket } = stubClient('/api/stream/hub');
    const stream = createHubStreamClient(client);

    // #when a hub address is minted
    const address = await stream.hubTicket();

    // #then the hub channel was requested and the address concatenates literally
    expect(streamTicket).toHaveBeenCalledWith('hub');
    expect(address).toEqual({
      url: '/api/stream/hub?ticket=',
      ticket: 'TICKET.SIG',
    });
    expect(`${address.url}${address.ticket}`).toBe(
      '/api/stream/hub?ticket=TICKET.SIG',
    );
  });

  it('passes the workflowId so the server returns the fully-qualified run url', async () => {
    // #given the server (given the workflowId) returns the wf-qualified run url —
    // the route shape is authored server-side, not rebuilt here
    const { client, streamTicket } = stubClient(
      '/api/stream/run/gtm-outbound/demo_abc',
    );
    const stream = createHubStreamClient(client);

    // #when a run address is minted for a known workflow + run
    const address = await stream.runTicket('gtm-outbound', 'demo_abc');

    // #then the run channel was requested WITH the workflowId (server qualifies it)
    expect(streamTicket).toHaveBeenCalledWith(
      'run',
      'demo_abc',
      'gtm-outbound',
    );
    // #then the SPA just appends ?ticket= to the server url (same as hubTicket)
    expect(address.url).toBe('/api/stream/run/gtm-outbound/demo_abc?ticket=');
    expect(`${address.url}${address.ticket}`).toBe(
      '/api/stream/run/gtm-outbound/demo_abc?ticket=TICKET.SIG',
    );
  });
});
