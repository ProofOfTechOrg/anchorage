# Security Policy

Anchorage is a safety and approval layer for AI agent workflows, so security
reports get priority handling.

## Reporting a vulnerability

Report privately via GitHub's security advisories:
[https://github.com/ProofOfTechOrg/anchorage/security/advisories/new](https://github.com/ProofOfTechOrg/anchorage/security/advisories/new)
("Report a vulnerability" on the repository's Security tab). Do not open a
public issue for an exploitable defect.

Include what you can of: the affected package
(`@proofoftech/breakwater` / `@proofoftech/flowsafe`), a reproduction or
proof of concept, and the impact as you understand it. You can expect an
acknowledgment within a few days; fixes land as ordinary releases with
credit unless you prefer otherwise.

## Scope

Especially interesting: anything that bypasses an enforcement path —
connector approval grants, network-egress gating, RBAC, policy-engine
output gating, idempotency/rate-limit accounting, approval-queue
authorization, or the fail-closed behavior of forged resumes. The threat
model, trust boundaries, and audit schema live in
[`docs/security-threat-model.md`](docs/security-threat-model.md).

Out of scope: vulnerabilities in Mastra itself (report upstream at
[mastra-ai/mastra](https://github.com/mastra-ai/mastra)), and issues
requiring an attacker who already controls the trusted computing base
(e.g. arbitrary code inside the Worker that mints grants).

## Supported versions

Pre-1.0: fixes land on `main` only. There are no maintained release
branches yet.
