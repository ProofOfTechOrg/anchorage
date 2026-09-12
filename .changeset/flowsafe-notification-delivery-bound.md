---
'@proofoftech/flowsafe': minor
---

Bound notification delivery to ten failed attempts by default, configurable through `maxDeliveryAttempts` on tick and thread-route factories. Discard exhausted rows before another send, retain their error/count receipts, and remove them from due scans while preserving retry delays below the bound.

Require conditional failure writes through `NotificationDeliveryStorage` for dispatch. `D1NotificationsStorage` implements the atomic operation; custom stores must adopt it. Preserve newer summary, delivery and content-denial receipts after response loss, and count each local outcome once without inferring unconfirmed success. Ordinary Core notification ingestion remains supported.

Compare notification dates chronologically before bounded selection and retention, including expanded years and numeric offsets. Direct database writers must use ISO dates or explicitly zoned ISO date-times; conditional failure writes reject raw timestamp text outside that grammar, and neither due selection nor retention matches such a value.

Ship the `@mastra/core@1.53.0` patch under `patches/`. Application roots must apply it for own-property-safe summary source counts and source delivery policies; `@mastra/core@1.53.0` otherwise reads inherited `Object.prototype` members at both sites (mastra-ai/mastra#23693, mastra-ai/mastra#23694). The getting-started guide documents the pnpm, npm and Yarn routes. Flowsafe refuses to construct its notification dispatch tick and refuses notification dispatch requests when the installed `@mastra/core` lacks the patch.
