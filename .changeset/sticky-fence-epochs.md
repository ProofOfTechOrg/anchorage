---
'@proofoftech/flowsafe': minor
---

Add versioned execution-fence administration with artifact epochs, a sticky epoch requirement, transition revisions, and exact last-command retry receipts. Admin reads and successful transitions return the complete versioned reading without exposing receipts. Legacy commands remain compatible only while the requirement is optional; proof metadata can bind to an admitted epoch and revision.

Upgrade supported legacy fence schemas additively without changing existing state, proof metadata, or timestamps. A missing row in a new-format schema now fails closed instead of reopening the deployment. Administrative metadata alone does not enforce final run or schedule writes; activation requires every writer to support final-write epoch checks.
