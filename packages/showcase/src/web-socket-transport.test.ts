import { afterEach, describe, expect, it, vi } from 'vitest';

import { createShowcaseStreamTransport } from '@/web-socket-transport';

// A minimal stand-in for the browser WebSocket global: it records itself so the
// test can drive its on* callbacks, and its close() is a spy.
class MockWebSocket {
  static last: MockWebSocket | null = null;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  readonly url: string;
  readonly close = vi.fn();

  constructor(url: string) {
    this.url = url;
    MockWebSocket.last = this;
  }
}

describe('createShowcaseStreamTransport', () => {
  const original = (globalThis as { WebSocket?: unknown }).WebSocket;

  afterEach(() => {
    (globalThis as { WebSocket?: unknown }).WebSocket = original;
    MockWebSocket.last = null;
  });

  it('opens the browser socket at the url and maps each socket event to its handler', () => {
    // #given a mocked WebSocket global and a set of handler spies
    (globalThis as { WebSocket?: unknown }).WebSocket = MockWebSocket;
    const onOpen = vi.fn();
    const onMessage = vi.fn();
    const onClose = vi.fn();
    const onError = vi.fn();
    const transport = createShowcaseStreamTransport();

    // #when the transport opens a connection
    const connection = transport.open('/api/stream/hub?ticket=t', {
      onMessage,
      onOpen,
      onClose,
      onError,
    });
    const socket = MockWebSocket.last;

    // #then the socket was constructed at the given url
    expect(socket?.url).toBe('/api/stream/hub?ticket=t');

    // #then each socket event routes to its structural handler
    socket?.onopen?.();
    expect(onOpen).toHaveBeenCalledTimes(1);
    socket?.onmessage?.({ data: '{"type":"queue"}' });
    expect(onMessage).toHaveBeenCalledWith('{"type":"queue"}');
    socket?.onerror?.(new Error('boom'));
    expect(onError).toHaveBeenCalledTimes(1);
    socket?.onclose?.();
    expect(onClose).toHaveBeenCalledTimes(1);

    // #then closing the connection closes the underlying socket
    connection.close();
    expect(socket?.close).toHaveBeenCalledTimes(1);
  });

  it('throws a clear error when no WebSocket global is available (non-browser host)', () => {
    // #given no WebSocket on globalThis
    (globalThis as { WebSocket?: unknown }).WebSocket = undefined;
    const transport = createShowcaseStreamTransport();

    // #when / #then opening fails loudly instead of a bare ReferenceError
    expect(() =>
      transport.open('/api/stream/hub?ticket=t', { onMessage: () => {} }),
    ).toThrow(/WebSocket/);
  });
});
