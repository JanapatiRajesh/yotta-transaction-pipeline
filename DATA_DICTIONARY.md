# Data Dictionary

## Input: transactions.csv
| Field | Type | Nullable | Rule |
|---|---|---:|---|
| event_id | string | No | Stable primary event key; unique |
| transaction_id | string | No | Groups transaction legs |
| account_id | string | No | Must exist in accounts.csv |
| entry_type | string | No | `debit` or `credit` |
| amount | decimal string | No | Positive, maximum two decimal places |
| currency | string | No | Supported values: INR, USD, EUR, GBP |
| event_timestamp | ISO-8601 string | No | Must include timezone; normalized to UTC |

## Input: accounts.csv
| Field | Type | Nullable | Rule |
|---|---|---:|---|
| account_id | string | No | Unique account reference key |
| account_type | string | Yes | Synthetic descriptive value |

## SQLite: transaction_events
One row per accepted event. `event_id` is the primary key. `transaction_id` relates multiple legs. Amount is stored as a decimal string to avoid SQLite floating-point rounding.

## SQLite: transaction_summary
One row per transaction ID and currency. Contains total debit, total credit, difference, and `is_balanced`.

## SQLite: quarantined_records
One row per distinct rejected record/reason combination. `reject_id` is a deterministic hash. Stores source file, source line, raw record JSON, rejection reason, run ID, and rejection time.

## SQLite: pipeline_runs
One row per pipeline execution, including run ID, timestamps, status, counts, duration, and ordering metrics.

## Naming conventions
- Table and column names use snake_case.
- Timestamps use ISO-8601 UTC representation.
- `run_id` identifies a pipeline execution.
- `source_file` and `source_line` make rejected input traceable.
