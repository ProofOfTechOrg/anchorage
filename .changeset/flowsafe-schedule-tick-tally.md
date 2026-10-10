---
'@proofoftech/flowsafe': patch
---

The schedule tick's per-pass result counts each fire once, in the pass that writes its trigger, except a fire whose run-cap check or claim throws after its claim is stored, which its pass counts `failed` and the pass that settles the claim counts again. Before this change a fire was counted as `fired`, `skipped` or `failed`, and a reconciled fire also as `reconciled`, before its trigger was written, so a failed write still counted it while its trigger stayed `deferred`, and the pass that later recorded it counted it again. A fire whose trigger write fails now counts as `deferred`. A fire that dispatched nothing, a lost claim and an unadvanceable cron were also counted `failed` a second time when their bookkeeping threw; for them a failed trigger write or audit is now logged as a `schedule-tick-bookkeeping-error` line instead of a `schedule-tick-error` line, without a second count.
