# Design Notes and Trade-offs

## Decisions
1. **Python + CSV + SQLite:** low setup cost, portable, easy to inspect and test in a local assessment.
2. **Transaction-level quarantine:** if one leg fails validation or the transaction does not balance, valid-looking legs from that same transaction are not published alone.
3. **Decimal arithmetic:** financial amounts are parsed using Decimal and stored as fixed-point strings; no binary floating-point reconciliation.
4. **Stable event key:** `event_id` is the primary key. An identical event is skipped on rerun; conflicting content is quarantined.
5. **Late events:** timestamp regressions in source-file order are measured but not automatically rejected because late arrival is not necessarily invalid.
6. **Traceability:** rejected records retain the source file, source line, raw row, and rejection reason.
7. **Atomic publication:** database writes happen inside a SQLite transaction.

## Limitations
- A local SQLite database is suitable for a take-home assessment, not high-concurrency production workloads.
- The ordering check uses source file order because the input contract does not provide a source sequence number.
- The current summary is rebuilt from accepted events. A production implementation should model transaction lifecycle/completeness and correction/reversal events explicitly.
- This implementation does not convert currencies or infer exchange rates.
- The source contract is CSV-based; an API adapter could be added later.

## Future improvements
- Add a source event sequence number and configurable lateness watermark.
- Add schema versioning and data contracts.
- Add CI workflow and code coverage.
- Add batch-level transaction manifests and source checksums.
- Move to a warehouse or distributed engine only when scale or concurrency requires it.
