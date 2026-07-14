// The showcase's browser-WebSocket StreamTransport: the approval-ui library
// factory, behind one showcase seam so the SPA has a single construction site
// (and a test seam). The library reads globalThis.WebSocket and maps its
// open/message/close/error events onto the structural StreamHandlers, so the
// library itself stays DOM-free; the showcase injects the returned transport
// into useApprovalDashboard's `stream` option and into useRunPolling.

import type { StreamTransport } from '@flowsafe/approval-ui/stream';
import { createWebSocketStreamTransport } from '@flowsafe/approval-ui/use-web-socket-transport';

/** The browser-WebSocket StreamTransport the SPA opens its live channels over. */
export function createShowcaseStreamTransport(): StreamTransport {
  return createWebSocketStreamTransport();
}
