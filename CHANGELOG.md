# Changelog

## 0.1.0

- Initial typed collection, NeDB-style query/update engine, AVL indexes, and
  memory/disk storage backends.
- Generate chronologically sortable, process-monotonic UUIDv7 document IDs.
- Expose database identifiers as `_id`, leaving `id` available to application
  models.
- Replace the single record log with an extensible snapshot backend contract.
- Move document placement and AVL index ownership into the Memory and Disk
  backends; collections now plan indexed fetches or streaming scans.
- Store disk content and each index in separate immutable segment files with an
  atomically published manifest.
