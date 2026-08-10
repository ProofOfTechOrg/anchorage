// The stream addressing thunks the SPA hands to the approval-ui hook and the
// run poll. The authenticated ApprovalApiClient mints a short-lived HMAC ticket
// over /api/stream/ticket (bearer auth, DL-010); this module shapes the returned
// { url, ticket } so `url + ticket` is a valid WebSocket URL, because
// subscribeApprovalStream concatenates the two LITERALLY.
//
// Both channels keep the server's own url and append `?ticket=`. The run channel
// passes the workflowId the client launched under to streamTicket, so the server
// returns the fully-qualified `/api/stream/run/${wf}/${runId}` url — the route
// shape lives once, on the server (stream-router), instead of being rebuilt here.
// Both urls stay relative so the browser resolves them same-origin (http -> ws)
// against the page.

import type { ApprovalApiClient } from '@flowsafe/approval-ui/client';

/** An address whose `url + ticket` concatenation forms the WS URL. */
export interface StreamAddress {
  url: string;
  ticket: string;
}

export interface ShowcaseStreamTickets {
  /** Mint + shape the deployment hub (queue) channel address. */
  hubTicket: () => Promise<StreamAddress>;
  /** Mint + shape a per-run channel address (the server returns the wf-qualified url). */
  runTicket: (workflowId: string, runId: string) => Promise<StreamAddress>;
}

export function createHubStreamClient(
  client: ApprovalApiClient,
): ShowcaseStreamTickets {
  return {
    hubTicket: async () => {
      const { url, ticket } = await client.streamTicket('hub');
      return { url: `${url}?ticket=`, ticket };
    },
    runTicket: async (workflowId, runId) => {
      // Pass the workflowId so the server returns the fully-qualified run WS url;
      // shape it exactly like hubTicket (append `?ticket=`), so the route shape is
      // authored once, on the server, and can never silently drift from this side.
      const { url, ticket } = await client.streamTicket(
        'run',
        runId,
        workflowId,
      );
      return { url: `${url}?ticket=`, ticket };
    },
  };
}
