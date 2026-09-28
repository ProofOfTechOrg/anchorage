---
"@proofoftech/flowsafe": patch
---

`FlowsafeDurableAgent` preserves the input of a run that the host start seam did not register by the guarded input chain's verdict, before it refuses the run. Such a run starts when Mastra drains a signal still queued after the run it was delivered to completes, or when a caller streams into the agent directly.

- When no input processor refused the run, its prepared input is saved.
- When the run's input is a created signal, the call carries neither a host ticket nor a request context, and Breakwater's RBAC gate refuses it, the signal is saved as received. That is the drain after a run resumed through `resumeViaRuntime()`, whose options carry no request context: the gate refuses it for its missing actor without reading content, and on a guarded agent before any application input processor runs. Breakwater removes a refused call's input from Mastra's prepared message list, so the signal would otherwise be lost. Host code that calls `stream()` the same way, with a created signal or an object carrying the signal brand as input, has that input saved when the gate refuses it.
- After any other refusal nothing is saved.
- A transient signal is never saved.

Security: the input of an unregistered run that an input policy or processor refused was saved to the thread, where later calls load it as history without the input policies reading it.
