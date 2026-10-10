---
'anchorage-agent-starter': patch
---

The conformance state accepts only the runner's `RUN_NOT_SUSPENDED` refusal as proof that a replayed raw resume was refused. Before this change any `409` from the run counted as that refusal, including the conflict a resume meets while a leg of the run is still executing, so the gate could attest a defense the replay never reached. Another refusal now fails the replay step.
