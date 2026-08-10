// The showcase's demo identities — ONE source of truth for two consumers:
//
//   1. src/dev-actor-switcher.tsx — the dev actor switcher's buttons
//   2. showcase/.dev.vars.example   — the APPROVAL_ACTOR_TOKENS local-dev secret
//
// (2) cannot import this module (it is a dotenv file the Vite plugin reads), so
// demo-actors.test.ts pins it against demoActorTokensJson() to catch drift.
//
// Deliberately dependency-free: a plain const and a local string-union role, no
// import from approval-api (whose barrel reaches do-runner and therefore
// @cloudflare/workers-types). That keeps this importable from BOTH the browser
// tsconfig (tsconfig.json, lib DOM) and the worker one
// (tsconfig.worker.json, workers-types, no DOM). The role union is mirrored
// by value from ApprovalRole; demo-actors.test.ts asserts the two agree.
//
// These tokens are PUBLIC — they are checked into the repo. They exist so RBAC
// and separation-of-duties are clickable locally. Never seed them as a deployed
// worker's APPROVAL_ACTOR_TOKENS secret: that publishes world-known admin
// credentials.

export type DemoRole = 'admin' | 'builder' | 'operator' | 'reviewer' | 'viewer';

export interface DemoActor {
  /** Bearer token presented as `Authorization: Bearer <token>`. */
  token: string;
  /** The actor id the approval queue attributes decisions to (also the UI label). */
  id: string;
  role: DemoRole;
}

export const DEMO_ACTORS: readonly DemoActor[] = [
  { token: 'demo-admin', id: 'admin', role: 'admin' },
  { token: 'demo-builder', id: 'builder', role: 'builder' },
  { token: 'demo-operator', id: 'operator', role: 'operator' },
  { token: 'demo-reviewer', id: 'reviewer', role: 'reviewer' },
  { token: 'demo-viewer', id: 'viewer', role: 'viewer' },
];

/** The demo actors serialized into the APPROVAL_ACTOR_TOKENS wire shape. */
export function demoActorTokensJson(): string {
  return JSON.stringify(
    Object.fromEntries(
      DEMO_ACTORS.map((actor) => [
        actor.token,
        { id: actor.id, role: actor.role },
      ]),
    ),
  );
}
