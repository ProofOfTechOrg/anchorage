---
"@proofoftech/breakwater": minor
---

Harden public connector and Agent CLI boundaries for the first public release.
Agent CLI connectors now expose structured, redacted errors, pass workspace-edit
permission flags to Claude Code and Codex, and keep prompts and option values out
of diagnostics and audit events. Connector, policy-evaluator, and actor-lookup
failures now emit static safe audit reasons. Add exhaustive export sentinels and
a packed-tarball consumer test, move Zod to runtime dependencies, and publish
complete package and connector guides.
