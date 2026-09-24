---
"@proofoftech/breakwater": patch
---

`createConnector()` now throws a `TypeError` naming the connector when `permissions.egress` is present but is not an array. A string used to be spread into one-character entries, so a string of letters and digits registered its characters as hosts, and a dotted, hyphenated or wildcard host failed on its first `.`, `-` or `*`. A `Set` or another non-array iterable, which registered its members, must now be passed as an array (`[...iterable]`). An omitted or `null` `egress` still registers an empty list.
