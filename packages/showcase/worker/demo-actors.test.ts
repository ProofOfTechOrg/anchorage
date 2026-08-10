// Drift guard for the demo identities. They are declared once in
// demo-actors.ts, but `.dev.vars.example` is a dotenv file wrangler reads — it
// cannot import the const, so nothing but this test stops the two diverging and
// leaving `dev:worker` authenticating against tokens the UI never offers.
//
// Also pins the locally-mirrored DemoRole union against the real ApprovalRole
// set: an actor whose role is not a known role is dropped by parseActorTokens,
// which would silently 401 that identity.

import type { ApprovalRole } from '@proofoftech/flowsafe/approval-api';
import { APPROVAL_ROLES } from '@proofoftech/flowsafe/approval-api';
import { parseActorTokens } from '@proofoftech/flowsafe/host-kit';
import { describe, expect, it } from 'vitest';
import {
  DEMO_ACTORS,
  type DemoRole,
  demoActorTokensJson,
} from '#worker/demo-actors';

// process.getBuiltinModule loads the node builtins without import machinery,
// so this workers-types program (no @types/node — vitest 4 stopped leaking
// Node's ambient types) never sees the `node:` specifiers. Same pattern as
// flowsafe's test-support/sqlite.ts.
const getBuiltin = (
  globalThis as unknown as {
    process?: { getBuiltinModule?: (id: string) => unknown };
  }
).process?.getBuiltinModule;
if (!getBuiltin) {
  throw new Error('node builtins unavailable — tests require node >= 22.3');
}
const { readFileSync } = getBuiltin('node:fs') as {
  readFileSync: (path: string, encoding: 'utf8') => string;
};
const { fileURLToPath } = getBuiltin('node:url') as {
  fileURLToPath: (url: URL) => string;
};

// Compile-time drift guard on the UNION itself. The runtime checks below pin
// the demo DATA against APPROVAL_ROLES, but a bogus member added to DemoRole
// (with DEMO_ACTORS left complete) would slip past them. `Mutual<A, B>` resolves
// to `true` only when the two unions are mutually assignable, so a divergence is
// a typecheck failure, not a silent one.
type Mutual<A, B> = [A] extends [B] ? ([B] extends [A] ? true : never) : never;
const _demoRoleMatchesApprovalRole: Mutual<DemoRole, ApprovalRole> = true;

function devVarsExample(): string {
  // Lives at the package root, beside wrangler.jsonc — where wrangler reads it.
  // import.meta.url is untyped under workers-types (vitest supplies it at run
  // time), hence the structural cast.
  const moduleUrl = (import.meta as unknown as { url: string }).url;
  return readFileSync(
    fileURLToPath(new URL('../.dev.vars.example', moduleUrl)),
    'utf8',
  );
}

/** The value of the APPROVAL_ACTOR_TOKENS assignment, ignoring `#` comments. */
function actorTokensLine(contents: string): string {
  const line = contents
    .split('\n')
    .find((candidate) => candidate.startsWith('APPROVAL_ACTOR_TOKENS='));
  if (line === undefined) {
    throw new Error(
      '.dev.vars.example has no APPROVAL_ACTOR_TOKENS assignment',
    );
  }
  return line.slice('APPROVAL_ACTOR_TOKENS='.length);
}

describe('demo actors', () => {
  it('matches the APPROVAL_ACTOR_TOKENS in .dev.vars.example', () => {
    // #given / #when
    const fromFile = JSON.parse(actorTokensLine(devVarsExample()));

    // #then — compare parsed values, so key order and whitespace do not matter
    expect(fromFile).toEqual(JSON.parse(demoActorTokensJson()));
  });

  it('only uses roles the approval service recognizes', () => {
    // #given — DemoRole mirrors ApprovalRole by value (demo-actors.ts must stay
    // free of the approval-api barrel, which reaches workers-types)
    const roles = new Set(DEMO_ACTORS.map((actor) => actor.role));

    // #then
    for (const role of roles) {
      expect(APPROVAL_ROLES).toContain(role);
    }
  });

  it('survives the production parse path, mapping every token to its actor', () => {
    // #given — the dev backend authenticates through parseActorTokens, which
    // DROPS any entry with an unknown role rather than trusting it
    const parsed = parseActorTokens(demoActorTokensJson());

    // #then — no identity is silently lost
    expect(parsed.size).toBe(DEMO_ACTORS.length);
    for (const actor of DEMO_ACTORS) {
      expect(parsed.get(actor.token)).toEqual({
        id: actor.id,
        role: actor.role,
      });
    }
  });

  it('offers one identity per role, so every RBAC path is clickable', () => {
    // #given / #then
    expect(DEMO_ACTORS.map((actor) => actor.role).sort()).toEqual(
      [...APPROVAL_ROLES].sort(),
    );
  });
});
