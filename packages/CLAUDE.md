# Package navigation

- [`breakwater/`](breakwater/): published guardrail and connector package
- [`flowsafe/`](flowsafe/): published durable execution and approval package
- [`fleet-control/`](fleet-control/): published fleet provisioning package, importable only from a trusted control plane
- [`showcase/`](showcase/): private public-demo application
- [`agent-starter/`](agent-starter/): private advanced consumer starter, and the source of the Workers for Platforms conformance artifacts

Use each package's `CLAUDE.md` for local navigation and its public `README.md` for product behavior.

```bash
pnpm -r build
pnpm -r typecheck
pnpm test
```
