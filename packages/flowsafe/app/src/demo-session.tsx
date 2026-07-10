// Public-demo session handling. The OAuth callback redirects to
// `/#demo-tokens=<base64url(JSON DemoTokenSet)>` — a FRAGMENT, so the token
// set never appears in server logs. This module reads it once (clearing the
// hash), renders the per-role switcher, and silently refreshes the short-TTL
// JWTs while the sandbox (tenant) is live. No token literal exists here: the
// production bundle stays credential-free (scripts/assert-clean-app-bundle).

import { HStack } from '@astryxdesign/core/HStack';
import {
  SegmentedControl,
  SegmentedControlItem,
} from '@astryxdesign/core/SegmentedControl';
import { Timestamp } from '@astryxdesign/core/Timestamp';
import { Token } from '@astryxdesign/core/Token';
import { Tooltip } from '@astryxdesign/core/Tooltip';
import { type ReactElement, useEffect, useRef, useState } from 'react';

import { GLOSSARY } from './glossary.js';
import {
  actorSwitchedEvent,
  type NarrationEvent,
  sessionExpiredEvent,
  sessionExpiringEvent,
  tokenRefreshedEvent,
} from './narration.js';

export interface DemoToken {
  id: string;
  role: string;
  token: string;
}

export interface DemoTokenSet {
  tenantId: string;
  tenantExpiresAt: string;
  tokens: DemoToken[];
}

/** JWTs are minted with a 1h TTL; refresh comfortably inside it. */
const REFRESH_INTERVAL_MS = 30 * 60 * 1000;

export function readDemoTokensFromHash(): DemoTokenSet | null {
  const hash = window.location.hash;
  const marker = '#demo-tokens=';
  if (!hash.startsWith(marker)) return null;
  try {
    // base64url -> base64, RE-PADDED: atob rejects an unpadded string whose
    // length is not a multiple of 4, and base64UrlEncode strips '='. The
    // server-side reader (verifier.ts) pads identically — keep them in step.
    const normalized = hash
      .slice(marker.length)
      .replace(/-/g, '+')
      .replace(/_/g, '/');
    const encoded = normalized + '='.repeat((4 - (normalized.length % 4)) % 4);
    const parsed = JSON.parse(atob(encoded)) as DemoTokenSet;
    if (!Array.isArray(parsed.tokens) || parsed.tokens.length === 0) {
      return null;
    }
    // Tokens read; scrub them from the address bar / history entry.
    window.history.replaceState(null, '', window.location.pathname);
    return parsed;
  } catch {
    return null;
  }
}

/**
 * The public-demo OAuth entry, resolved from the server's /auth/config echo —
 * the single source; the SPA never hardcodes a provider, so swapping the
 * worker's provider needs no client change. Deliberately TRI-state: a plain
 * `string | undefined` conflated "probe in flight" with "no demo configured",
 * forcing the signed-out landing to paint the token-paste layout first and
 * then replace it once the echo landed on every OAuth deployment.
 */
export type DemoSignIn =
  | { status: 'loading' }
  | { status: 'oauth'; provider: string }
  | { status: 'none' };

export function useDemoSignIn(): DemoSignIn {
  const [signIn, setSignIn] = useState<DemoSignIn>({ status: 'loading' });
  useEffect(() => {
    let alive = true;
    fetch('/auth/config')
      .then(async (response) => {
        if (!response.ok) return undefined;
        const config = (await response.json()) as {
          enabled?: boolean;
          provider?: string;
        };
        return config.enabled === true && typeof config.provider === 'string'
          ? config.provider
          : undefined;
      })
      .catch(() => undefined)
      .then((provider) => {
        if (!alive) return;
        setSignIn(
          provider === undefined
            ? { status: 'none' }
            : { status: 'oauth', provider },
        );
      });
    return () => {
      alive = false;
    };
  }, []);
  return signIn;
}

/** Warn this long before the sandbox tenant expires. */
const EXPIRY_WARNING_MS = 15 * 60 * 1000;

export function DemoActorSwitcher({
  session,
  actorToken,
  onSelect,
  onSession,
  onExpired,
  narrate,
}: {
  session: DemoTokenSet;
  actorToken: string | null;
  onSelect: (token: string) => void;
  /** A refresh replaced the whole token set. */
  onSession: (session: DemoTokenSet) => void;
  /** The sandbox (tenant) expired — the caller drops back to sign-in. */
  onExpired: () => void;
  narrate: (events: readonly NarrationEvent[]) => void;
}): ReactElement {
  // Bootstrap: start as the operator (the natural workflow starter);
  // reviewer/admin are one click away for the approve-your-run flow.
  const bootstrap =
    actorToken === null
      ? (session.tokens.find((entry) => entry.role === 'operator') ??
        session.tokens[0])
      : undefined;
  useEffect(() => {
    if (bootstrap) onSelect(bootstrap.token);
  }, [bootstrap, onSelect]);

  // Silent refresh while the tenant is live: the mid-demo "step away, come
  // back, switch to reviewer and approve" flow must survive a JWT expiry.
  // session/actorToken are read through refs so a role switch (or the refresh
  // itself replacing the session) does NOT re-arm the interval — resetting
  // the 30-minute countdown on every switch could outlast the 1h JWT TTL.
  const sessionRef = useRef(session);
  sessionRef.current = session;
  const actorTokenRef = useRef(actorToken);
  actorTokenRef.current = actorToken;
  useEffect(() => {
    const timer = setInterval(() => {
      const current = sessionRef.current.tokens[0];
      if (!current) return;
      void fetch('/auth/refresh', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ token: current.token }),
      })
        .then(async (response) => {
          if (response.status === 401) {
            narrate([sessionExpiredEvent()]);
            onExpired();
            return;
          }
          if (!response.ok) return; // transient — retry next tick
          const next = (await response.json()) as DemoTokenSet;
          onSession(next);
          narrate([tokenRefreshedEvent()]);
          // Keep the selected ROLE across the rotation.
          const selected = sessionRef.current.tokens.find(
            (entry) => entry.token === actorTokenRef.current,
          );
          const replacement = next.tokens.find(
            (entry) => entry.id === selected?.id,
          );
          if (replacement) onSelect(replacement.token);
        })
        // A network blip (or a malformed body) is transient like a non-ok
        // response: swallow and retry next tick — same posture as
        // useDemoSignIn — instead of surfacing an unhandled rejection.
        .catch(() => undefined);
    }, REFRESH_INTERVAL_MS);
    return () => clearInterval(timer);
  }, [onSelect, onSession, onExpired, narrate]);

  // One heads-up before the tenant reaches end of life (key-deduped, so the
  // effect re-arming on session refreshes cannot double-warn).
  useEffect(() => {
    const expiresAtMs = Date.parse(session.tenantExpiresAt);
    if (Number.isNaN(expiresAtMs)) return;
    const delay = expiresAtMs - EXPIRY_WARNING_MS - Date.now();
    if (expiresAtMs <= Date.now()) return;
    const minutesLeft = Math.max(
      1,
      Math.round((expiresAtMs - Date.now()) / 60_000),
    );
    const timer = setTimeout(
      () => narrate([sessionExpiringEvent(Math.min(15, minutesLeft))]),
      Math.max(0, delay),
    );
    return () => clearTimeout(timer);
  }, [session.tenantExpiresAt, narrate]);

  const selectedRole =
    session.tokens.find((entry) => entry.token === actorToken)?.role ?? '';

  function switchRole(role: string): void {
    const entry = session.tokens.find((token) => token.role === role);
    if (!entry || entry.token === actorToken) return;
    onSelect(entry.token);
    narrate([actorSwitchedEvent(entry.id, entry.role)]);
  }

  return (
    <HStack gap={2} align="center" wrap="wrap" aria-label="Acting identity">
      <Tooltip content={GLOSSARY.sandboxTenant}>
        <Token label={`sandbox ${session.tenantId}`} size="sm" color="cyan" />
      </Tooltip>
      <Tooltip content={GLOSSARY.expiry}>
        <HStack gap={1} align="center">
          <Timestamp
            value={session.tenantExpiresAt}
            format="relative"
            isLive
            size="sm"
            color="secondary"
          />
        </HStack>
      </Tooltip>
      <SegmentedControl
        label="Acting role"
        value={selectedRole}
        onChange={switchRole}
        size="sm"
      >
        {session.tokens.map((entry) => (
          <SegmentedControlItem
            key={entry.id}
            value={entry.role}
            label={entry.role}
          />
        ))}
      </SegmentedControl>
    </HStack>
  );
}
