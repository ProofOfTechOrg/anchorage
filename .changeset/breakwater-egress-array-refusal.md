---
"@proofoftech/breakwater": patch
---

`createConnector()` now throws a `TypeError` naming the connector when `permissions.egress` is present but is not an array, instead of registering each character of a string (or each member of a `Set`) as a declared host; an omitted or `null` `egress` still registers an empty list.
