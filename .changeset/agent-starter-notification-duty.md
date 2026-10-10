---
'anchorage-agent-starter': patch
---

The starter dispatches due notifications as their own one-minute maintenance duty instead of after the schedule pass in the same invocation. Before this change a schedule pass that threw, for example because reconciling deferred fires failed, skipped notification delivery for that pass, and a notification failure hid the schedule pass's result. `starterMaintenanceTick` is replaced by `starterScheduleTick` and `starterNotificationTick`. Upgrade a fleet control plane that audits starter deployments to `@proofoftech/fleet-control` 0.6.0 first, so it watches the notification duty.
