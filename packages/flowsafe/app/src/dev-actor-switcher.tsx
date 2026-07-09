// DEV-ONLY. The single app module allowed to import the public demo tokens:
// main.tsx loads it lazily behind `import.meta.env.DEV`, so the production
// bundle contains no token literal (scripts/assert-clean-app-bundle.mjs pins
// that). Identity itself still comes from the server's catalog echo — these
// buttons only choose which PUBLIC dev token to present.

import { type ReactElement, useEffect } from 'react';

import { useApprovalUIComponents } from '../../src/approval-ui/components.js';
import { DEMO_ACTORS } from '../../showcase/demo-actors.js';

export function DevActorSwitcher({
  actorToken,
  onSelect,
}: {
  actorToken: string | null;
  onSelect: (token: string) => void;
}): ReactElement {
  const C = useApprovalUIComponents();

  // Dev bootstrap: start signed in as the first demo actor, preserving the
  // zero-click app:dev experience. Runs once; the selection lives in Root.
  const bootstrapToken = actorToken === null ? DEMO_ACTORS[0]?.token : null;
  useEffect(() => {
    if (bootstrapToken) onSelect(bootstrapToken);
  }, [bootstrapToken, onSelect]);

  return (
    <C.Section aria-label="Acting identity">
      <C.Stack gap="sm">
        <C.Heading level={2}>Acting as</C.Heading>
        <C.Text>
          Switch identity to see RBAC and separation-of-duties live.
          Grant-minting always stays server-side, whichever actor you pick.
        </C.Text>
        <C.Stack direction="horizontal" gap="sm">
          {DEMO_ACTORS.map((actor) => (
            <C.Button
              key={actor.token}
              label={actor.id}
              variant={actor.token === actorToken ? 'primary' : 'secondary'}
              pressed={actor.token === actorToken}
              onClick={() => onSelect(actor.token)}
            />
          ))}
        </C.Stack>
      </C.Stack>
    </C.Section>
  );
}
