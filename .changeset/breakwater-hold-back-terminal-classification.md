---
"@proofoftech/breakwater": patch
---

Hold-back classifies the remaining text of a streaming segment before releasing it at a channel end or stream finish. A denied segment held with `holdBackChars: Infinity` emits no text.

Security: A denied segment shorter than the classifier cadence could previously be released under hold-back before the result-phase refusal.
