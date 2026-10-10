---
'@proofoftech/fleet-control': minor
---

The maintenance watchdog checks a deployment's run-deadline duty and notification duty the way it checks the SLA sweep and the schedule tick. `MaintenanceHealth` gains `lastDeadlineAt`, `lastDeadlineAttemptAt`, `lastDeadlineError`, `lastNotificationAt`, `lastNotificationAttemptAt` and `lastNotificationError`, read from a FlowSafe maintenance status that reports the duty, and `auditFleetDrift()` reports a `maintenance-stale` finding when that duty's last attempt failed or its last success is older than `staleAfterMs`. A deployment whose FlowSafe does not report a duty is checked as before.

Breaking: the run-deadline duty, which a FlowSafe maintenance object always runs, is now checked on every audit wherever its FlowSafe reports it. An existing deployment whose deadline duty fails, or has not succeeded within `staleAfterMs`, draws a `maintenance-stale` finding, and the audit re-arms its maintenance object under the deployment lease on every audit that finds it, as it does for a failing SLA sweep. A deployment whose notification duty fails draws the same finding and re-arm. Before this change neither duty was checked, so a persistently failing deadline duty, which leaves expired runs running, raised no finding.
