---
'@proofoftech/breakwater': minor
---

Say what the connector conformance harness established about an entry point a case deleted outright.
An `INSTRUMENTATION_REPLACED` finding for a property that is gone at verification now reads
`globalThis.fetch descriptor differs from the one the harness installed: absent property; calls made
after the replacement were not observed`. The finding stopped at `absent property` before, though a
read after the deletion resolves through the prototype chain or to `undefined` and never to the trap.
A finding for a replacement that is itself an accessor still says the harness did not check, and one
for a data property that still holds the trap still carries no such clause.
