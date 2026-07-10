// Public-demo session handling. The OAuth callback redirects to
// `/#demo-tokens=<base64url(JSON DemoTokenSet)>` — a FRAGMENT, so the token
// set never appears in server logs. This module reads it once (clearing the
// hash), renders the per-role switcher, and silently refreshes the short-TTL
// JWTs while the sandbox (tenant) is live. No token literal exists here: the
// production bundle stays credential-free (scripts/assert-clean-app-bundle).

import { type ReactElement, useEffect, useState } from 'react';

import { useApprovalUIComponents } from '../../src/approval-ui/components.js';

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
 * The configured public-demo OAuth provider name ('google', 'github', …), or
 * undefined when the worker has no demo configured. The server's /auth/config
 * echo is the single source — the SPA never hardcodes a provider, so swapping
 * the worker's provider needs no client change.
 */
export function useDemoSignIn(): string | undefined {
  const [provider, setProvider] = useState<string | undefined>(undefined);
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
      .then((value) => {
        if (alive) setProvider(value);
      });
    return () => {
      alive = false;
    };
  }, []);
  return provider;
}

export function DemoActorSwitcher({
  session,
  actorToken,
  onSelect,
  onSession,
  onExpired,
}: {
  session: DemoTokenSet;
  actorToken: string | null;
  onSelect: (token: string) => void;
  /** A refresh replaced the whole token set. */
  onSession: (session: DemoTokenSet) => void;
  /** The sandbox (tenant) expired — the caller drops back to sign-in. */
  onExpired: () => void;
}): ReactElement {
  const C = useApprovalUIComponents();

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
  useEffect(() => {
    const timer = setInterval(() => {
      const current = session.tokens[0];
      if (!current) return;
      void fetch('/auth/refresh', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ token: current.token }),
      }).then(async (response) => {
        if (response.status === 401) {
          onExpired();
          return;
        }
        if (!response.ok) return; // transient — retry next tick
        const next = (await response.json()) as DemoTokenSet;
        onSession(next);
        // Keep the selected ROLE across the rotation.
        const selected = session.tokens.find(
          (entry) => entry.token === actorToken,
        );
        const replacement = next.tokens.find(
          (entry) => entry.id === selected?.id,
        );
        if (replacement) onSelect(replacement.token);
      });
    }, REFRESH_INTERVAL_MS);
    return () => clearInterval(timer);
  }, [session, actorToken, onSelect, onSession, onExpired]);

  return (
    <C.Section aria-label="Acting identity">
      <C.Stack gap="sm">
        <C.Heading level={2}>Your demo sandbox</C.Heading>
        <C.Text>
          {`Isolated tenant ${session.tenantId} — everything you run and approve here is invisible to every other visitor. Sandbox expires ${new Date(session.tenantExpiresAt).toLocaleString()}.`}
        </C.Text>
        <C.Text>
          Switch identity to see RBAC and separation-of-duties live: start a run
          as the operator, then approve it as the reviewer.
        </C.Text>
        <C.Stack direction="horizontal" gap="sm">
          {session.tokens.map((entry) => (
            <C.Button
              key={entry.id}
              label={entry.role}
              variant={entry.token === actorToken ? 'primary' : 'secondary'}
              pressed={entry.token === actorToken}
              onClick={() => onSelect(entry.token)}
            />
          ))}
        </C.Stack>
      </C.Stack>
    </C.Section>
  );
}
