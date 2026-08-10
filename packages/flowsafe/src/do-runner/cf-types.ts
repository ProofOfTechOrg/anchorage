// SPDX-License-Identifier: Apache-2.0
// Structural subsets of the Cloudflare Workers runtime types the DO runner
// forwards (D1Database) or reads from (DurableObjectState).
// @cloudflare/workers-types is a devDependency only — its types are ambient,
// never runtime code — so importing it from an exported signature here would
// leak into the published dist/**/*.d.ts and force every consumer to install
// it too (or silently degrade flagship types like D1Database to `any` under
// skipLibCheck, the operative mode). Same posture as SnapshotDatabase in
// d1-storage.ts, IdempotencyDatabase in breakwater, and ApprovalDatabase in
// approval-api: each interface below covers only the members do-runner
// actually reads or forwards, held structurally so tests can back them with
// plain objects and Workers pass the real bindings straight through.

import type { D1Database, DurableObjectState } from '@cloudflare/workers-types';

/**
 * Structural subset of D1Database — the binding init() and createD1Storage
 * forward, opaque, into @mastra/cloudflare-d1's D1Store (see the cast at
 * that boundary in d1-storage.ts). do-runner itself never calls a method on
 * it — D1Store does, internally — so `prepare` is kept only as the one
 * identifying member: it rejects a value that plainly isn't D1-shaped (a
 * string, a KV namespace, ...) at the type level without over-specifying a
 * surface nothing here consumes.
 */
export interface D1DatabaseBinding {
  prepare(query: string): unknown;
}

/**
 * Structural subset of the Hibernatable-WebSocket surface the DO stream
 * channels touch: send() fans a frame to the client, close() ends the socket,
 * and the attachment (de)serializers let the hub hold per-socket presence
 * ACROSS hibernation (workerd persists the attachment, not instance fields).
 * Kept structural — never the workers-types `WebSocket` — so the do-runner
 * module graph stays node/vitest-loadable and no workers-types import reaches
 * the emitted .d.ts (same posture as the rest of this file).
 */
export interface WebSocketLike {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  serializeAttachment?(value: unknown): void;
  deserializeAttachment?(): unknown;
}

/**
 * Structural subset of the `WebSocketPair` runtime global's result — the
 * [client, server] socket pair a DO mints on a 101 upgrade, accessed by the
 * numeric 0/1 keys workerd returns. Referenced only on the workerd WS path,
 * which is guarded by acceptWebSocket presence and never runs in node.
 */
export interface WebSocketPairLike {
  readonly 0: WebSocketLike;
  readonly 1: WebSocketLike;
}

/** Structural key/value and alarm subset shared by the Durable Object hosts. */
export interface DurableKeyValueStorage {
  get<T = unknown>(key: string): Promise<T | undefined>;
  put<T>(key: string, value: T): Promise<void>;
  delete(key: string): Promise<boolean>;
  setAlarm?(scheduledTime: number | Date): Promise<void>;
  deleteAlarm?(): Promise<void>;
}

/**
 * The `WebSocketPair` runtime global (workerd), typed to the structural
 * WebSocketPairLike. Read off globalThis — NOT imported from 'cloudflare:workers'
 * — so this module graph stays node/vitest-loadable; the WS path that calls it
 * is guarded by acceptWebSocket presence and never runs in node, where the
 * global is absent. Mirrors the globalThis-cast idiom in
 * connector-sdk/new-token.ts (crypto) and agent-cli (TextEncoder). Lives here
 * (the owner of WebSocketPairLike) so the per-run runner DO and the hub DO share
 * ONE constructor instead of hand-copying it.
 */
export function newWebSocketPair(): WebSocketPairLike {
  const Ctor = (
    globalThis as unknown as { WebSocketPair: new () => WebSocketPairLike }
  ).WebSocketPair;
  return new Ctor();
}

/**
 * Send a frame to one socket, tolerating a throw. workerd's getWebSockets() can
 * return a CLOSING/CLOSED socket whose send() throws; without per-socket
 * isolation one dead socket would abort a fan-out loop and starve every later
 * subscriber of the frame. A throwing send means the socket is gone and will be
 * reaped, so swallow it. Shared by the hub DO and the per-run runner DO so the
 * fail-safe fan-out lives in one place.
 */
export function safeSend(ws: WebSocketLike, frame: string): void {
  try {
    ws.send(frame);
  } catch {
    // dead/closing socket — reaped by workerd; never starve the other subscribers
  }
}

/**
 * Structural subset of DurableObjectState — the shape DurableObjectRunner
 * reads from. `id.name` (run-identity recovery off the DO's own idFromName
 * address) and `storage` (owner-recovery records and alarms) are always touched;
 * the Hibernatable-WebSocket members are OPTIONAL so node/vitest stubs that
 * set only id.name/storage still satisfy the type and the per-run WS stream
 * route can guard on their presence (absent ⇒ the non-WS 426 fallback).
 */
export interface DurableObjectRunnerState {
  readonly id: {
    readonly name?: string;
  };
  readonly storage: DurableKeyValueStorage;
  /** Make a server socket hibernatable (workerd-only). */
  acceptWebSocket?(ws: WebSocketLike, tags?: string[]): void;
  /** Every hibernatable socket currently attached, optionally by tag. */
  getWebSockets?(tag?: string): WebSocketLike[];
}

/**
 * Structural subset of DurableObjectState the deployment hub DO (hub-do.ts)
 * reads: its OWN idFromName identity — `id.name` IS the fixed
 * HUB_INSTANCE_NAME, with no ':' join and no runId decode, unlike
 * DurableObjectRunner whose name is `${workflowId}:${runId}` — plus the
 * Hibernatable-WebSocket members it fans events out over. The hub holds no
 * D1/DO storage, so `storage` is not required here. Same OPTIONAL-WS,
 * guard-on-presence posture as DurableObjectRunnerState.
 */
export interface HubDurableObjectState {
  readonly id: {
    readonly name?: string;
  };
  acceptWebSocket?(ws: WebSocketLike, tags?: string[]): void;
  getWebSockets?(tag?: string): WebSocketLike[];
}

// Compile-time proof that the real Cloudflare types satisfy the structural
// subsets above, so a host passes env.DB / ctx straight through with no
// adapter. Type-only (erased at build; neither this import nor the
// non-exported aliases below reach the emitted .d.ts, so consumers pull no
// workers-types dependency). The R2 seam uses the same technique in the
// version-specific typecheck fixtures under test-support and scripts.
type AssertTrue<T extends true> = T;
type _D1DatabaseSatisfiesBinding = AssertTrue<
  D1Database extends D1DatabaseBinding ? true : false
>;
type _DurableObjectStateSatisfiesRunnerState = AssertTrue<
  DurableObjectState extends DurableObjectRunnerState ? true : false
>;
type _DurableObjectStateSatisfiesHubState = AssertTrue<
  DurableObjectState extends HubDurableObjectState ? true : false
>;
// The widened WS members are OPTIONAL on the structural states above (so a
// node stub with only id.name/storage still satisfies them and the WS path
// guards on their presence). This REQUIRED-shape pin is what actually catches
// a @cloudflare/workers-types drift: it fails typecheck if DurableObjectState
// ever drops or renames acceptWebSocket/getWebSockets — an optional-member
// `extends` would keep passing even then.
type _DurableObjectStateHasHibernationApi = AssertTrue<
  DurableObjectState extends {
    acceptWebSocket(ws: WebSocketLike, tags?: string[]): void;
    getWebSockets(tag?: string): WebSocketLike[];
  }
    ? true
    : false
>;
