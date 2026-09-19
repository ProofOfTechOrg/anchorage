---
"@proofoftech/flowsafe": patch
---

Keep `flowsafe-provision` inert when imported from stdin, eval or print programs, and virtual workers, even when an entry name aliases the script. Direct execution, file and directory symlinks, and file workers under eval parents continue to invoke the CLI.
