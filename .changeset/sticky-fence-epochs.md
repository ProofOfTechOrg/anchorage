---
'@proofoftech/flowsafe': minor
---

Add versioned execution-fence administration with artifact epochs, a sticky epoch requirement, transition revisions, and exact last-command retry receipts. Admin reads and successful transitions return the complete versioned reading without exposing receipts. Legacy commands remain compatible only while the requirement is optional; proof metadata can bind to an admitted epoch and revision.

Upgrade supported legacy fence schemas additively without changing existing state, proof metadata, or timestamps. A missing row in a new-format schema now fails closed instead of reopening the deployment. Administrative metadata alone does not enforce final run or schedule writes; activation requires every writer to support final-write epoch checks.

Add execution-identity and mutation-epoch validation/header helpers through do-runner and host-kit. Identity normalizers return frozen copies, preserve explicit unfenced namespaces, and validate identifiers without granting authority. Preserve the existing execution-fence unreadable error constructor across import paths.

Read additive reservation bindings and D1 proof identities, preserving active fence metadata during schema upgrades. Admin responses omit proof identity and tokens. Current reservation writes remain legacy-null, and Runtime provenance, lifecycle APIs, and run-ID-based predicates retain their existing behavior; automatic generation binding and final-write enforcement are not enabled by these additions.

Add an explicit same-binding D1 initial-admission capability with atomic snapshot, winning reservation and proof writes, exact raw reads, and scoped no-insert evidence. Default and serialized background workflow domains support it while ordinary unscoped writes retain adapter behavior. Built-in Runtime and hosts do not yet activate this primitive; complete recovery and writer integration remain required before artifact-epoch enforcement is enabled.

Add explicit expected-row terminalization to the owned D1 capability. It derives an unknown-effects failure or the stored cancellation/timeout intent, joins the existing background workflow queue, and reports exact conditional-write/readback outcomes without replaying execution or performing cleanup. Keep ordinary admission stamps and Runtime v1 behavior unchanged. Reject exhausted lifecycle revision and resume ordinal increments before persistence or execution while preserving readable maximum-valued counters on no-increment paths.
