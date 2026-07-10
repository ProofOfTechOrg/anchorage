// The signed-out landing: wordmark (+ "demo" chip on the public demo),
// tagline, the what/why demo points, the public-demo OAuth entry (when the
// server advertises one), the operator token paste — inline when it is the
// only entry, behind a modal when the OAuth demo leads — and the Proof of
// Tech attribution footer. App-owned panel — imports Astryx directly (the
// slot adapter serves only the library views). The provider name/href derive
// from the server's /auth/config echo; never hardcode a provider here.

import { Button } from '@astryxdesign/core/Button';
import { Card } from '@astryxdesign/core/Card';
import { Dialog } from '@astryxdesign/core/Dialog';
import { Heading } from '@astryxdesign/core/Heading';
import { HStack } from '@astryxdesign/core/HStack';
import { Link } from '@astryxdesign/core/Link';
import { Text } from '@astryxdesign/core/Text';
import { TextInput } from '@astryxdesign/core/TextInput';
import { Token } from '@astryxdesign/core/Token';
import { VStack } from '@astryxdesign/core/VStack';
import { type ReactElement, useState } from 'react';

import type { DemoSignIn } from '@/demo-session';
import { TAGLINE } from '@/glossary';
import { MarkerRow } from '@/marker-row';
import { ProofOfTechMark } from '@/proofoftech-logo';

/** Wordmark-correct display names; anything unlisted gets a plain capitalize. */
const PROVIDER_DISPLAY_NAMES: Record<string, string> = {
  google: 'Google',
  github: 'GitHub',
};

function providerDisplayName(provider: string): string {
  return (
    PROVIDER_DISPLAY_NAMES[provider] ??
    provider.charAt(0).toUpperCase() + provider.slice(1)
  );
}

/**
 * What the public demo proves, in landing-page terms. Bound by the copy
 * truthfulness rules (glossary.ts): never claim a delivery, grants are
 * derived server-side, refusals are real server responses.
 */
const DEMO_POINTS = [
  'Five real agent workflows execute server-side and suspend durably at human approval gates — multi-gate, branching, re-suspension.',
  'Four roles (operator, reviewer, admin, viewer): RBAC and separation of duties are enforced server-side — watch your own requests get refused.',
  'Every approval derives a capability grant bound to that exact suspension; a forged resume finds no grant and fails closed.',
  'Dangerous side effects are simulated — emails and deploys are logged, never delivered. The grants, tenant isolation, and durable suspend/resume are real.',
];

/**
 * Production sign-in: the demo OAuth entry and/or a pasted bearer token. The
 * token is held in memory only; identity and role come back from the server's
 * catalog echo. In dev, main.tsx renders the demo actor switcher instead.
 */
export function TokenGate({
  onSubmit,
  demoSignIn,
}: {
  onSubmit: (token: string) => void;
  /**
   * The public-demo OAuth entry from the server's /auth/config echo.
   * Tri-state: while the probe is in flight the entry section renders
   * NOTHING (the wordmark/tagline paint immediately), so an OAuth
   * deployment never paints the token-paste layout and then replaces it.
   */
  demoSignIn: DemoSignIn;
}): ReactElement {
  const [draft, setDraft] = useState('');
  // The OAuth demo is the landing's one primary action; the operator token
  // paste is a self-hoster escape hatch, so it hides behind a modal instead
  // of competing inline.
  const [operatorOpen, setOperatorOpen] = useState(false);
  // The one predicate for both the entry branch and the dialog mount.
  const oauth =
    demoSignIn.status === 'oauth'
      ? { provider: demoSignIn.provider, href: `/auth/${demoSignIn.provider}` }
      : undefined;

  function submit(): void {
    const token = draft.trim();
    if (token.length === 0) return;
    setDraft('');
    onSubmit(token);
  }

  const tokenForm = (
    <>
      <TextInput
        label="API token"
        value={draft}
        onChange={(next) => setDraft(next)}
        onEnter={submit}
      />
      <Button label="Sign in" variant="primary" onClick={submit} />
    </>
  );

  return (
    <>
      <Card
        variant="default"
        padding={6}
        maxWidth={520}
        style={{ margin: '10vh auto 0' }}
        aria-label="Sign in"
      >
        <VStack gap={4}>
          <HStack gap={2} align="center">
            <Heading level={1} type="display-3">
              Anchorage
            </Heading>
            {oauth ? <Token label="demo" size="sm" color="cyan" /> : null}
          </HStack>
          <Text color="secondary">{TAGLINE}</Text>
          {demoSignIn.status === 'loading' ? null : oauth ? (
            <VStack gap={3}>
              <Text>
                This demo is the full open-source platform running against a
                sandbox — not a video, not a mock — so you can judge the safety
                story by poking at it:
              </Text>
              <VStack gap={2}>
                {DEMO_POINTS.map((point) => (
                  <MarkerRow
                    key={point}
                    marker="•"
                    markerHidden
                    color="secondary"
                  >
                    {point}
                  </MarkerRow>
                ))}
              </VStack>
              <Text>
                You get an isolated throwaway tenant. Nothing you do is visible
                to anyone else, and it self-destructs in about 24 hours.
              </Text>
              <Button
                label={`Sign in with ${providerDisplayName(oauth.provider)}`}
                variant="primary"
                onClick={() => {
                  window.location.href = oauth.href;
                }}
              />
              <Button
                label="Have an operator API token?"
                variant="ghost"
                onClick={() => setOperatorOpen(true)}
              />
            </VStack>
          ) : (
            <>
              <Text size="sm" color="secondary">
                Paste this deployment's API token. Your identity and role are
                resolved server-side; the app ships with no credentials.
              </Text>
              {tokenForm}
            </>
          )}
          <HStack gap={2} align="center">
            <ProofOfTechMark size={18} />
            <Text size="sm" color="secondary">
              An{' '}
              <Link
                href="https://github.com/ProofOfTechOrg/anchorage"
                size="sm"
                isExternalLink
              >
                open-source
              </Link>{' '}
              project by{' '}
              <Link href="https://proofoftech.org" size="sm" isExternalLink>
                Proof of Tech
              </Link>
            </Text>
          </HStack>
        </VStack>
      </Card>
      {oauth ? (
        <Dialog
          isOpen={operatorOpen}
          onOpenChange={setOperatorOpen}
          width={440}
          padding={5}
        >
          <VStack gap={4}>
            <Heading level={2}>Operator sign-in</Heading>
            <Text size="sm" color="secondary">
              Paste an operator API token. Your identity and role are resolved
              server-side; the app ships with no credentials.
            </Text>
            {tokenForm}
          </VStack>
        </Dialog>
      ) : null}
    </>
  );
}

/** The header identity chip for a pasted-token (operator) session. */
export function OperatorIdentityChip({
  onSignOut,
}: {
  onSignOut: () => void;
}): ReactElement {
  return (
    <HStack gap={2} align="center">
      <Text size="sm" color="secondary">
        Token set — identity is verified by the server.
      </Text>
      <Button label="Sign out" variant="ghost" onClick={onSignOut} />
    </HStack>
  );
}
