# Guarded agent navigation

- `index.ts`: guarded construction, strict call options, processor validation and application input processor wrapping, and the package-local brand
- `client-tool-output.ts`: guarded mapping of client tool results in caller input and processor-changed messages before input policies, and origin checks of the tool outputs the call supplies
- `agent.test.ts`: direct execution, ordering, audit, zero-leak, type-surface, and Mastra inventory coverage — read its prototype classification and stale-entry assertions before any `@mastra/core` bump

The public handle is intentionally narrower than the protected Mastra `Agent` subclass. Do not add an unwrap API.
