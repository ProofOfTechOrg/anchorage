# Guarded agent navigation

- `index.ts`: guarded construction, strict call options, processor validation, and the package-local brand
- `agent.test.ts`: direct execution, ordering, audit, zero-leak, type-surface, and Mastra inventory coverage — read its `forwardClassified` allowlist before any `@mastra/core` bump (the bump prunes every name the new pin exposes)

The public handle is intentionally narrower than the protected Mastra `Agent` subclass. Do not add an unwrap API.
