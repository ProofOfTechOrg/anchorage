// The signed-out landing: wordmark, tagline, the public-demo OAuth entry (when
// the server advertises one), and the operator token paste. App-owned panel —
// imports Astryx directly (the slot adapter serves only the library views).
// The provider name/href derive from the server's /auth/config echo; never
// hardcode a provider here.

import { Button } from '@astryxdesign/core/Button';
import { Card } from '@astryxdesign/core/Card';
import { Divider } from '@astryxdesign/core/Divider';
import { Heading } from '@astryxdesign/core/Heading';
import { HStack } from '@astryxdesign/core/HStack';
import { Text } from '@astryxdesign/core/Text';
import { TextInput } from '@astryxdesign/core/TextInput';
import { VStack } from '@astryxdesign/core/VStack';
import { type ReactElement, useState } from 'react';

import { TAGLINE } from './glossary.js';

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
 * Production sign-in: the demo OAuth entry and/or a pasted bearer token. The
 * token is held in memory only; identity and role come back from the server's
 * catalog echo. In dev, main.tsx renders the demo actor switcher instead.
 */
export function TokenGate({
  onSubmit,
  demoSignInProvider,
}: {
  onSubmit: (token: string) => void;
  /**
   * When the worker has the public demo configured: the OAuth provider name
   * from the server's /auth/config echo. Drives the entry href and label.
   */
  demoSignInProvider?: string;
}): ReactElement {
  const [draft, setDraft] = useState('');
  const demoSignInHref = demoSignInProvider
    ? `/auth/${demoSignInProvider}`
    : undefined;

  function submit(): void {
    const token = draft.trim();
    if (token.length === 0) return;
    setDraft('');
    onSubmit(token);
  }

  return (
    <Card
      variant="default"
      padding={6}
      maxWidth={520}
      style={{ margin: '10vh auto 0' }}
      aria-label="Sign in"
    >
      <VStack gap={4}>
        <Heading level={1} type="display-3">
          Anchorage
        </Heading>
        <Text color="secondary">{TAGLINE}</Text>
        {demoSignInHref && demoSignInProvider ? (
          <VStack gap={3}>
            <Text>
              You get an isolated throwaway tenant. Nothing you do is visible to
              anyone else, and it self-destructs in about 24 hours.
            </Text>
            <Button
              label={`Sign in with ${providerDisplayName(demoSignInProvider)}`}
              variant="primary"
              onClick={() => {
                window.location.href = demoSignInHref;
              }}
            />
            <Divider label="or" />
          </VStack>
        ) : null}
        <Text size="sm" color="secondary">
          {demoSignInHref
            ? 'Paste an operator API token.'
            : "Paste this deployment's API token. Your identity and role are resolved server-side; the app ships with no credentials."}
        </Text>
        <TextInput
          label="API token"
          value={draft}
          onChange={(next) => setDraft(next)}
          onEnter={submit}
        />
        <Button
          label="Sign in"
          variant={demoSignInHref ? 'secondary' : 'primary'}
          onClick={submit}
        />
      </VStack>
    </Card>
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
