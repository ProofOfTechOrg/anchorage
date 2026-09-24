---
"@proofoftech/breakwater": patch
---

`createConnector()`, `networkEgress()` and `egressFetch()` now throw a `TypeError` naming the field and the index when a host-list entry is not a string, and each reads the list once, so the hosts it enforces are the hosts it validated. `networkEgress()` and `egressFetch()` also name the field when the list is not an array.

Security: a non-string entry used to pass hostname validation by string coercion. An entry object with its own string methods could then make the runtime egress guard, the `networkEgress()` policy and `assertConnectorConformance()` accept hosts the entry does not name. A list whose reads change could pass validation with one entry and enforce another in `networkEgress()` and `egressFetch()`.

Migration: pass each host as a plain string. A `String` object and a hole in the list are refused. `egressDomainAllowed()` ignores a non-string entry, which matches no host. It now returns `false` for a domain that is not a string or a list that is not an array, where a non-array list used to throw unless it had its own `map`, and it reads the list by index, so no method of the list decides the match.
