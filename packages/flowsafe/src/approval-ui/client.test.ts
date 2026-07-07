import { describe, expect, it } from 'vitest';

import {
  ApprovalApiClient,
  ApprovalApiError,
  type FetchLike,
} from './client.js';

interface Call {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: string | undefined;
}

function makeFetch(
  respond: (call: Call) => { status?: number; payload?: unknown } = () => ({}),
): { fetch: FetchLike; calls: Call[] } {
  const calls: Call[] = [];
  const fetch: FetchLike = async (url, init = {}) => {
    const call: Call = {
      url,
      method: init.method ?? 'GET',
      headers: init.headers ?? {},
      body: init.body,
    };
    calls.push(call);
    const { status = 200, payload = {} } = respond(call);
    return {
      ok: status >= 200 && status < 300,
      status,
      json: async () => payload,
    };
  };
  return { fetch, calls };
}

describe('ApprovalApiClient', () => {
  it('lists with a serialized filter query', async () => {
    // #given
    const { fetch, calls } = makeFetch(() => ({ payload: [] }));
    const client = new ApprovalApiClient({ fetch });

    // #when
    await client.list({
      status: ['pending', 'claimed'],
      workflowId: 'wf',
      claimedBy: 'ray',
    });

    // #then
    expect(calls[0]).toMatchObject({
      method: 'GET',
      url: '/api/approvals?status=pending%2Cclaimed&workflowId=wf&claimedBy=ray',
    });
  });

  it('decides with a JSON body and drops empty comments', async () => {
    // #given
    const { fetch, calls } = makeFetch(() => ({ payload: { record: {} } }));
    const client = new ApprovalApiClient({ fetch });

    // #when
    await client.decide('apr-1', 'approve', 'lgtm');
    await client.decide('apr-2', 'reject');

    // #then
    expect(calls[0]).toMatchObject({
      method: 'POST',
      url: '/api/approvals/apr-1/decide',
      body: JSON.stringify({ decision: 'approve', comment: 'lgtm' }),
    });
    expect(calls[1]?.body).toBe(JSON.stringify({ decision: 'reject' }));
  });

  it('sends configured auth headers on every request', async () => {
    // #given
    const { fetch, calls } = makeFetch();
    const client = new ApprovalApiClient({
      fetch,
      baseUrl: 'https://queue.example/api/approvals',
      headers: { authorization: 'Bearer token' },
    });

    // #when
    await client.claim('apr-1');

    // #then
    expect(calls[0]).toMatchObject({
      url: 'https://queue.example/api/approvals/apr-1/claim',
      headers: { authorization: 'Bearer token' },
    });
  });

  it('throws ApprovalApiError carrying the server message and status', async () => {
    // #given
    const { fetch } = makeFetch(() => ({
      status: 409,
      payload: { error: "cannot claim approval 'x' in status 'claimed'" },
    }));
    const client = new ApprovalApiClient({ fetch });

    // #when
    let caught: unknown;
    try {
      await client.claim('x');
    } catch (error) {
      caught = error;
    }

    // #then
    expect(caught).toBeInstanceOf(ApprovalApiError);
    expect(caught).toMatchObject({
      status: 409,
      message: "cannot claim approval 'x' in status 'claimed'",
    });
  });

  it('falls back to a status message when the error body is not JSON', async () => {
    // #given
    const fetch: FetchLike = async () => ({
      ok: false,
      status: 502,
      json: async () => {
        throw new Error('not json');
      },
    });
    const client = new ApprovalApiClient({ fetch });

    // #when / #then
    await expect(client.metrics()).rejects.toMatchObject({
      status: 502,
      message: 'request failed with status 502',
    });
  });

  it('escapes ids in paths', async () => {
    // #given
    const { fetch, calls } = makeFetch();
    const client = new ApprovalApiClient({ fetch });

    // #when
    await client.get('weird/id');

    // #then
    expect(calls[0]?.url).toBe('/api/approvals/weird%2Fid');
  });
});
