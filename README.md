# YottaFlex.ai — Transaction Data Quality Pipeline

## Overview
This project ingests synthetic transaction-leg CSV data, validates schema and business rules, quarantines invalid records, publishes valid balanced transactions to SQLite, and produces reconciliation outputs.

## Architecture
`transactions.csv + accounts.csv -> schema checks -> row checks -> transaction-level debit/credit and currency checks -> SQLite curated tables`
Invalid rows/transactions go to `quarantined_records` and `rejected_transactions.csv`. A JSON report captures run metrics.

## Requirements
- Python 3.10 or later
- pip
- Visual Studio Code (recommended)

## Setup on Windows (PowerShell)
Open this folder in VS Code, then open **Terminal > New Terminal**.

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

If PowerShell blocks activation, use Command Prompt terminal and run:
```bat
.venv\Scripts\activate.bat
```
You can also use `.venv\Scripts\python.exe` directly without activation.

## Generate reproducible sample data
```powershell
python -m src.data_generator --rows 40 --seed 42
```
Generate 100,000 event rows:
```powershell
python -m src.data_generator --rows 100000 --seed 42
```
The generator overwrites the input sample; generate the large sample only after the starter run, or use `--output` and `--accounts-output` to save it elsewhere.

## Run pipeline
```powershell
python -m src.pipeline
```
Or specify paths:
```powershell
python -m src.pipeline --input data/input/transactions.csv --accounts data/input/accounts.csv --output-dir data/output --db data/output/pipeline.db
```

## Run tests
```powershell
python -m pytest -q
```

## Outputs
- `data/output/curated_transactions.csv`: all unique accepted events stored in the SQLite curated table.
- `data/output/transaction_summary.csv`: debit/credit totals grouped by transaction and currency.
- `data/output/rejected_transactions.csv`: quarantined records and reasons.
- `data/output/validation_report.json`: run metrics and reconciliation flags.
- `data/output/pipeline.db`: SQLite tables for events, summaries, quarantine, and run history.
- `logs/pipeline.log`: execution log.

## Schema and business assumptions
- One input row is one debit/credit leg.
- `event_id` is the stable unique event key.
- Amounts must be positive and have at most two decimal places.
- `entry_type` is `debit` or `credit`.
- Supported currencies are INR, USD, EUR, and GBP.
- Every transaction must contain at least one debit and one credit leg, one currency, and equal debit/credit totals.
- An invalid row is rejected. If a transaction's remaining valid legs do not balance, all its otherwise-valid legs are rejected as a transaction group.
- Account IDs must exist in `accounts.csv`.
- Timestamps must be ISO-8601 and include a timezone. They are normalized to UTC.
- A source timestamp earlier than the preceding source row is counted as out-of-order but not automatically rejected. This avoids treating legitimate late-arriving events as invalid.
- Identical event IDs with identical content are idempotently skipped on reruns. Conflicting content for an existing event ID is quarantined.
- Rejection records retain source file, source line, run ID, timestamp, raw row, and reason.

## Recovery and idempotency
SQLite publishing is transactional. The `event_id` primary key prevents duplicate curated events. Rerunning an identical input does not add duplicate events. Rejected records are inserted using a deterministic rejection key so repeated runs do not multiply identical quarantine records.

## Performance
The report includes duration and records/second. Benchmark the same generated input on the same computer for comparisons. Do not claim an optimization without measured results. A reasonable future optimization is batched inserts and vectorized row-level checks.

## Limitations / next steps
- Local single-machine implementation; no distributed processing or scheduler.
- The sample account file is synthetic.
- Event ordering is measured as source-file timestamp regressions; a production design would use source sequence numbers, watermarks, and an explicit late-arrival policy.
- Currency conversion is not performed; different currencies are never netted together.
- This is an assessment project, not a production banking system.
