# Guarded agent navigation

- `index.ts`: guarded construction, strict call options, processor validation and application input processor wrapping, and the package-local brand
- `caller-input.test.ts`, `provider-options.test.ts`, `input-chain.test.ts`: real-loop rows for what the input policies read, the provider options and provider-executed results the bundled adapters send, and the input chain's failures, refusals and application input processors' results
- `agent.test.ts`: direct execution, ordering, audit, zero-leak, type-surface, and Mastra inventory coverage — read its prototype classification and stale-entry assertions before any `@mastra/core` bump

The public handle is intentionally narrower than the protected Mastra `Agent` subclass. Do not add an unwrap API.
