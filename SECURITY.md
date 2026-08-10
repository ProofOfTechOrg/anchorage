# Security Policy

Anchorage is a safety and approval layer for AI agent workflows, so security
reports get priority handling.

## Reporting a vulnerability

Report privately through GitHub's
[vulnerability-reporting form](https://github.com/ProofOfTechOrg/anchorage/security/advisories/new)
("Report a vulnerability" on the repository's Security tab). Do not open a
public issue for an exploitable defect.

Include what you can of: the affected package
(`@proofoftech/breakwater` / `@proofoftech/flowsafe`), a reproduction or
proof of concept, affected versions and deployment shape, and the impact as
you understand it. Maintainers will coordinate validation, remediation,
credit, and disclosure in the private advisory. The project does not promise a
fixed response-time SLA.

## Scope

Especially interesting: anything that bypasses or weakens an enforcement path,
including:

- connector grants, dry-run guarantees, egress allowlists and redirect checks,
  idempotency or rate-limit accounting, CLI argument separation, policy-engine
  inspection, RBAC, audit integrity, or metrics attribution;
- approval authorization, separation of duties, batch decisions, live-stream
  tickets, suspension binding, forged-resume rejection, and server-only resume
  targets;
- deployment binding and sentinel verification, internal Durable Object caller
  authentication, per-principal authorization for workflow runs, agent memory,
  signals, goals, schedules, and artifacts, resource-bound authorization for
  provider subscriptions, and trusted-entry controls for notifications,
  background tasks, retention, and deployment decommissioning;
- webhook signature verification, stored request-context barriers, unattended
  run caps, and any path that lets untrusted input mint a capability.

**Physical deployment isolation is the highest-value target.** Any path that
lets one organization's Worker reach another organization's D1 data, Durable
Objects, R2 artifacts, queues, secrets, connector replay cache, or rate-limit
budget is critical. This includes accepting a mismatched or malformed
deployment sentinel, adopting a non-empty unowned database, accepting a
Worker-to-Durable-Object call without the target deployment's internal
credential, or routing more than one organization into the same data-plane
resource set. A path that lets one principal read, write, enumerate, resume, or
delete another principal's protected resource without its documented role is
also in scope. The physical deployment boundary, resource-authorization
invariants, provisioning rules, and audit attribution are stated in
[`docs/security-threat-model.md`](docs/security-threat-model.md).

Out of scope: vulnerabilities in Mastra itself (report upstream at
[mastra-ai/mastra](https://github.com/mastra-ai/mastra)), and issues
requiring an attacker who already controls the trusted computing base
(for example, arbitrary code inside the Worker that mints grants). An Anchorage
integration flaw triggered by otherwise supported Mastra behavior remains in
scope.

## Supported versions

Only the latest published version of each pre-1.0 package is supported.
Security fixes land on `main` and are released as new package versions; older
versions and separate release branches are not maintained.
