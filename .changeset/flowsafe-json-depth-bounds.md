---
'@proofoftech/flowsafe': minor
---

Breaking: more caller-supplied JSON is refused with `400` when nested more than 256 levels deep: a schedule's `metadata` value on create and update, an approval's `payload` on the HTTP create route, a state signal's `value` or `delta` and a notification signal's `payload` at the thread object, a signal-provider subscription's `metadata` value, a `providerOptions` value of an agent start at the thread host, and an event the trusted audit proxy receives. The HTTP approval create route also refuses a `summary` that is not a string with `400`. A signal-provider delivery whose notification payload nests that deep is now recorded as failed, unless the payload nests too deep for the delivery to serialize, which stays deferred as before. Before this change such a value was stored, and one nested past what the Workers runtime can serialize answered `500` or `502`, in some cases after it was stored; the audit proxy answered `503`.
