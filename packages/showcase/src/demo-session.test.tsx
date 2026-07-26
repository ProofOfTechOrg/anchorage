// @vitest-environment jsdom

import { render } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { DemoActorSwitcher, type DemoTokenSet } from '@/demo-session';

vi.mock('@astryxdesign/core/Timestamp', () => ({
  Timestamp: () => null,
}));

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('DemoActorSwitcher token refresh', () => {
  it('prevents overlapping refreshes and aborts the active request on cleanup', async () => {
    vi.useFakeTimers();
    let refreshSignal: AbortSignal | undefined;
    const fetchMock = vi.fn(
      (_input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
        refreshSignal = init?.signal ?? undefined;
        return new Promise((_resolve, reject) => {
          refreshSignal?.addEventListener(
            'abort',
            () => reject(new DOMException('Aborted', 'AbortError')),
            { once: true },
          );
        });
      },
    );
    vi.stubGlobal('fetch', fetchMock);

    const session: DemoTokenSet = {
      tenantId: 'tenant-a',
      tenantExpiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(),
      tokens: [{ id: 'operator-a', role: 'operator', token: 'operator-token' }],
    };
    const onSelect = vi.fn();
    const onSession = vi.fn();
    const onExpired = vi.fn();
    const narrate = vi.fn();

    const rendered = render(
      <DemoActorSwitcher
        session={session}
        actorToken="operator-token"
        onSelect={onSelect}
        onSession={onSession}
        onExpired={onExpired}
        narrate={narrate}
      />,
    );

    await vi.advanceTimersByTimeAsync(60 * 60 * 1000);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(refreshSignal?.aborted).toBe(false);

    rendered.unmount();
    expect(refreshSignal?.aborted).toBe(true);
    await Promise.resolve();

    expect(onSelect).not.toHaveBeenCalled();
    expect(onSession).not.toHaveBeenCalled();
    expect(onExpired).not.toHaveBeenCalled();
    expect(narrate).not.toHaveBeenCalled();
  });
});
