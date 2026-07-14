// SPDX-License-Identifier: Apache-2.0
// The ONE place a browser-WebSocket dependency exists in approval-ui. This is a
// use-*.ts module the workers-typed pass EXCLUDES (see the package tsconfig
// glob), so a WebSocket global is allowed here and nowhere else — client.ts,
// stream.ts, and use-approval-dashboard.ts stay DOM-free and inject this
// transport by construction, exactly as ApprovalApiClient injects fetch.
//
// The socket is read from globalThis and typed through a minimal STRUCTURAL
// shape (like client.ts's FetchLike), not the ambient DOM `WebSocket`: the
// package's other tsc passes compile against @cloudflare/workers-types, whose
// WebSocket differs from lib.dom's, so a structural view is the one that is
// stable across every pass and honest about the members we touch.

import type {
  StreamConnection,
  StreamHandlers,
  StreamTransport,
} from './stream.js';

/** The browser WebSocket members this transport uses — the structural seam. */
interface BrowserWebSocket {
  onopen: (() => void) | null;
  onmessage: ((event: { data: unknown }) => void) | null;
  onclose: (() => void) | null;
  onerror: ((event: unknown) => void) | null;
  send(data: string): void;
  close(): void;
}

interface BrowserWebSocketConstructor {
  new (url: string): BrowserWebSocket;
}

/**
 * A StreamTransport backed by the browser `WebSocket` global (read from
 * globalThis, mirroring ApprovalApiClient's globalThis.fetch default so a
 * non-browser host gets a clear error instead of a ReferenceError). Inject the
 * returned transport into useApprovalDashboard's `stream` option; the host
 * builds the ticket() thunk (showcase, M-008).
 */
export function createWebSocketStreamTransport(): StreamTransport {
  return {
    open(url: string, handlers: StreamHandlers): StreamConnection {
      // Cast the property to unknown first (like client.ts's globalThis.fetch):
      // this pass's ambient WebSocket is workers-types', which does not overlap
      // the structural browser shape, so a direct cast is rejected.
      const WebSocketCtor = (globalThis as { WebSocket?: unknown }).WebSocket as
        | BrowserWebSocketConstructor
        | undefined;
      if (!WebSocketCtor) {
        throw new Error(
          'createWebSocketStreamTransport: no WebSocket available; this transport requires a browser environment',
        );
      }
      const socket = new WebSocketCtor(url);
      socket.onopen = () => handlers.onOpen?.();
      socket.onmessage = (event) => {
        handlers.onMessage(
          typeof event.data === 'string' ? event.data : String(event.data),
        );
      };
      socket.onclose = () => handlers.onClose?.();
      socket.onerror = (event) => handlers.onError?.(event);
      return {
        // send() throws on a non-OPEN socket; the hook's heartbeat wraps this in
        // try/catch and treats a throw as a disconnect, so no guard is needed here.
        send: (data) => socket.send(data),
        close: () => socket.close(),
      };
    },
  };
}
