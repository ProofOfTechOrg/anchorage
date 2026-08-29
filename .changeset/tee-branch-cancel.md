---
'@proofoftech/fleet-control': patch
---

`FileSystemDatabaseExportStore.write` no longer awaits the reader's cancel in its failure cleanup. When the body is a `tee()` branch, that cancel settles when the tee source is exhausted or the other branch is cancelled, so a write refused by the store's own checks held its rejection and its temporary file until then, and held both indefinitely when nothing drove the source. It now rejects with the store's error and removes the file without awaiting the cancel.
