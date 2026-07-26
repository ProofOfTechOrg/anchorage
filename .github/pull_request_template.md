## Summary

Describe the problem, the root cause, and the resulting behavior.

## User impact

Explain which package, export, workflow, or deployment is affected and under
what conditions.

## Verification

List the exact commands and scenarios used to verify the change.

## Checklist

- [ ] The change is scoped to the stated problem and follows existing patterns.
- [ ] Tests cover the changed behavior and the relevant failure paths.
- [ ] Public behavior, setup, configuration, and exports are documented.
- [ ] `pnpm docs:check` passes; generated `docs/api/` output is not committed.
- [ ] A changeset is included for a user-visible published-package change, or
      the PR explains why none is required.
- [ ] Security, tenant isolation, capability minting, retention, and unattended
      execution were reviewed where applicable.
- [ ] Any AI-assisted code or prose has been reviewed and verified by the
      contributor.
