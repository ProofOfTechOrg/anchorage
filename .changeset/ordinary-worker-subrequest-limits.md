---
'@proofoftech/fleet-control': patch
---

Forward configured `DeploymentSpec.subrequestLimit` values through ordinary-Worker uploads in the direct Cloudflare and Wrangler adapters, including staged versions. The setting was validated and included in the specification digest but omitted from upload requests. It now accompanies `cpuLimitMs`; an omitted setting remains unspecified.

Review existing ordinary-Worker subrequest settings when upgrading because those configured values now reach the provider.
