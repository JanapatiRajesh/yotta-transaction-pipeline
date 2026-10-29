"""Local transaction validation pipeline for the YottaFlex.ai assessment.

Policy summary:
- Each CSV row is one debit/credit leg.
- Amounts must be positive decimal values with at most two decimal places.
- Each transaction must contain at least one debit and one credit, use one currency,
  and have equal debit/credit totals.
- If any leg in a transaction fails validation, the entire transaction is quarantined.
- A source row whose event timestamp is earlier than the preceding source row is
  counted as out-of-order. It is not automatically rejected: late events can be valid.
- event_id is the stable event key. Identical events are idempotently skipped on reruns;
  a reused event_id with different content is quarantined as a conflict.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import sqlite3
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "data" / "input" / "transactions.csv"
DEFAULT_ACCOUNTS = ROOT / "data" / "input" / "accounts.csv"
DEFAULT_OUTPUT = ROOT / "data" / "output"
DEFAULT_DB = ROOT / "data" / "output" / "pipeline.db"

REQUIRED_COLUMNS = [
    "event_id", "transaction_id", "account_id", "entry_type",
    "amount", "currency", "event_timestamp"
]
ALLOWED_CURRENCIES = {"INR", "USD", "EUR", "GBP"}
ALLOWED_ENTRY_TYPES = {"debit", "credit"}
CENT = Decimal("0.01")


def setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_dir / "pipeline.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )


def parse_timestamp(value: str) -> datetime:
    """Parse ISO-8601 timestamps and require a timezone."""
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("event_timestamp must include a timezone, e.g. Z")
    return parsed.astimezone(timezone.utc)


def canonical_timestamp(value: str) -> str:
    return parse_timestamp(value).isoformat().replace("+00:00", "Z")


def decimal_amount(value: str) -> Decimal:
    try:
        amount = Decimal(value.strip())
    except (InvalidOperation, AttributeError):
        raise ValueError("amount must be a valid decimal number")
    if not amount.is_finite():
        raise ValueError("amount must be finite")
    if amount <= 0:
        raise ValueError("amount must be greater than zero")
    if amount.quantize(CENT) != amount:
        raise ValueError("amount must have at most two decimal places")
    return amount.quantize(CENT)


def read_accounts(path: Path) -> set[str]:
    if not path.exists():
        raise FileNotFoundError(f"Account reference file not found: {path}")
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "account_id" not in reader.fieldnames:
            raise ValueError("accounts.csv must contain an account_id column")
        accounts = set()
        for row in reader:
            account = (row.get("account_id") or "").strip()
            if account:
                accounts.add(account)
    return accounts


def read_source(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Return well-shaped rows, malformed rows, and header errors."""
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    good_rows: list[dict[str, Any]] = []
    malformed: list[dict[str, Any]] = []
    header_errors: list[str] = []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, restkey="_extra_fields", restval=None)
        headers = reader.fieldnames or []
        missing_headers = [c for c in REQUIRED_COLUMNS if c not in headers]
        if missing_headers:
            return [], [], [f"Missing required column(s): {', '.join(missing_headers)}"]
        for line_number, row in enumerate(reader, start=2):
            extra = row.pop("_extra_fields", None)
            row["_source_line"] = line_number
            if extra:
                row["_raw_extra_fields"] = extra
                malformed.append({
                    **row,
                    "rejection_reason": "malformed row: too many fields",
                    "source_line": line_number,
                })
            else:
                good_rows.append(row)
    return good_rows, malformed, header_errors


def normalize_and_validate(row: dict[str, Any], accounts: set[str]) -> tuple[dict[str, Any] | None, list[str]]:
    errors: list[str] = []
    normalized = dict(row)
    for field in REQUIRED_COLUMNS:
        value = row.get(field)
        if value is None or not str(value).strip():
            errors.append(f"missing required field: {field}")

    event_id = str(row.get("event_id") or "").strip()
    transaction_id = str(row.get("transaction_id") or "").strip()
    account_id = str(row.get("account_id") or "").strip()
    entry_type = str(row.get("entry_type") or "").strip().lower()
    currency = str(row.get("currency") or "").strip().upper()

    if entry_type and entry_type not in ALLOWED_ENTRY_TYPES:
        errors.append("entry_type must be debit or credit")
    if currency and currency not in ALLOWED_CURRENCIES:
        errors.append(f"unsupported currency: {currency}")
    if account_id and account_id not in accounts:
        errors.append(f"unknown account_id: {account_id}")

    amount: Decimal | None = None
    if row.get("amount") is not None and str(row.get("amount")).strip():
        try:
            amount = decimal_amount(str(row["amount"]))
        except ValueError as exc:
            errors.append(str(exc))

    timestamp: str | None = None
    if row.get("event_timestamp") is not None and str(row.get("event_timestamp")).strip():
        try:
            timestamp = canonical_timestamp(str(row["event_timestamp"]))
        except (ValueError, TypeError, OverflowError):
            errors.append("event_timestamp must be a valid ISO-8601 timestamp with timezone")

    if errors:
        return None, errors

    normalized.update({
        "event_id": event_id,
        "transaction_id": transaction_id,
        "account_id": account_id,
        "entry_type": entry_type,
        "amount": format(amount, ".2f"),
        "currency": currency,
        "event_timestamp": timestamp,
    })
    return normalized, []


def content_hash(row: dict[str, Any]) -> str:
    content = {k: row[k] for k in REQUIRED_COLUMNS}
    payload = json.dumps(content, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS transaction_events (
                event_id TEXT PRIMARY KEY,
                transaction_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                entry_type TEXT NOT NULL CHECK(entry_type IN ('debit','credit')),
                amount TEXT NOT NULL,
                currency TEXT NOT NULL,
                event_timestamp TEXT NOT NULL,
                source_file TEXT NOT NULL,
                run_id TEXT NOT NULL,
                content_hash TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS transaction_summary (
                transaction_id TEXT NOT NULL,
                currency TEXT NOT NULL,
                total_debit TEXT NOT NULL,
                total_credit TEXT NOT NULL,
                difference TEXT NOT NULL,
                is_balanced INTEGER NOT NULL,
                last_run_id TEXT NOT NULL,
                PRIMARY KEY(transaction_id, currency)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS quarantined_records (
                reject_id TEXT PRIMARY KEY,
                event_id TEXT,
                transaction_id TEXT,
                source_file TEXT NOT NULL,
                source_line INTEGER,
                rejection_reason TEXT NOT NULL,
                run_id TEXT NOT NULL,
                rejected_at TEXT NOT NULL,
                raw_record_json TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                source_file TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                status TEXT NOT NULL,
                input_count INTEGER NOT NULL,
                accepted_count INTEGER NOT NULL,
                rejected_count INTEGER NOT NULL,
                duplicate_count INTEGER NOT NULL,
                out_of_order_count INTEGER NOT NULL,
                duration_seconds REAL NOT NULL,
                failure_reason TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_transaction ON transaction_events(transaction_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_quarantine_run ON quarantined_records(run_id)")


def quarantine_id(source_file: str, line: Any, reason: str, raw: dict[str, Any]) -> str:
    stable = json.dumps(
        {"source_file": source_file, "line": line, "reason": reason, "raw": raw},
        sort_keys=True, default=str
    )
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()


def run_pipeline(
    input_path: str | Path = DEFAULT_INPUT,
    accounts_path: str | Path = DEFAULT_ACCOUNTS,
    output_dir: str | Path = DEFAULT_OUTPUT,
    db_path: str | Path = DEFAULT_DB,
) -> dict[str, Any]:
    input_path, accounts_path = Path(input_path), Path(accounts_path)
    output_dir, db_path = Path(output_dir), Path(db_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(output_dir.parent / "logs")
    logger = logging.getLogger(__name__)
    run_id = str(uuid.uuid4())
    start_wall = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    start = time.perf_counter()
    source_name = input_path.name

    try:
        accounts = read_accounts(accounts_path)
        rows, malformed, header_errors = read_source(input_path)
        if header_errors:
            raise ValueError("; ".join(header_errors))
        input_count = len(rows) + len(malformed)
        rejected: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []
        duplicate_count = 0

        for bad in malformed:
            rejected.append({
                "event_id": bad.get("event_id"),
                "transaction_id": bad.get("transaction_id"),
                "source_file": source_name,
                "source_line": bad.get("source_line"),
                "rejection_reason": bad["rejection_reason"],
                "raw_record_json": json.dumps(bad, sort_keys=True, default=str),
            })

        # Detect source-file timestamp regressions. Late records are reported, not
        # automatically rejected, because event time can legitimately lag ingestion time.
        out_of_order_count = 0
        previous_time = None
        for row in rows:
            try:
                current_time = parse_timestamp(str(row.get("event_timestamp") or ""))
                if previous_time is not None and current_time < previous_time:
                    out_of_order_count += 1
                previous_time = current_time
            except (ValueError, TypeError):
                pass

        seen_in_batch: dict[str, str] = {}
        row_errors: dict[str, list[str]] = {}
        normalized_by_line: dict[int, dict[str, Any]] = {}
        for row in rows:
            normalized, errors = normalize_and_validate(row, accounts)
            line = int(row["_source_line"])
            if errors:
                row_errors[str(line)] = errors
                rejected.append({
                    "event_id": row.get("event_id"),
                    "transaction_id": row.get("transaction_id"),
                    "source_file": source_name,
                    "source_line": line,
                    "rejection_reason": "; ".join(errors),
                    "raw_record_json": json.dumps(row, sort_keys=True, default=str),
                })
                continue
            assert normalized is not None
            eid = normalized["event_id"]
            digest = content_hash(normalized)
            if eid in seen_in_batch:
                duplicate_count += 1
                reason = "duplicate event_id within input batch"
                if seen_in_batch[eid] != digest:
                    reason = "conflicting duplicate event_id within input batch"
                rejected.append({
                    "event_id": eid, "transaction_id": normalized["transaction_id"],
                    "source_file": source_name, "source_line": line,
                    "rejection_reason": reason,
                    "raw_record_json": json.dumps(row, sort_keys=True, default=str),
                })
                continue
            seen_in_batch[eid] = digest
            normalized["_source_line"] = line
            normalized["_content_hash"] = digest
            candidates.append(normalized)

        # Validate transactions as a unit. Any invalid leg makes all otherwise-valid
        # legs in that transaction ineligible for curated output.
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in candidates:
            grouped[row["transaction_id"]].append(row)

        valid_events: list[dict[str, Any]] = []
        transaction_status: dict[tuple[str, str], tuple[Decimal, Decimal, bool]] = {}
        for txid, legs in grouped.items():
            currencies = {r["currency"] for r in legs}
            debits = sum((Decimal(r["amount"]) for r in legs if r["entry_type"] == "debit"), Decimal("0.00"))
            credits = sum((Decimal(r["amount"]) for r in legs if r["entry_type"] == "credit"), Decimal("0.00"))
            reasons = []
            if len(currencies) != 1:
                reasons.append("transaction contains multiple currencies")
            if not any(r["entry_type"] == "debit" for r in legs):
                reasons.append("transaction has no debit leg")
            if not any(r["entry_type"] == "credit" for r in legs):
                reasons.append("transaction has no credit leg")
            if debits != credits:
                reasons.append(f"imbalanced transaction: debit={debits:.2f}, credit={credits:.2f}")
            if reasons:
                reason = "; ".join(reasons)
                for r in legs:
                    rejected.append({
                        "event_id": r["event_id"], "transaction_id": txid,
                        "source_file": source_name, "source_line": r["_source_line"],
                        "rejection_reason": reason,
                        "raw_record_json": json.dumps({k: v for k, v in r.items() if not k.startswith("_")},
                                                       sort_keys=True, default=str),
                    })
            else:
                valid_events.extend(legs)
                curr = next(iter(currencies))
                transaction_status[(txid, curr)] = (debits, credits, True)

        init_db(db_path)
        accepted_new = 0
        duplicate_existing = 0
        conflict_existing = []
        # Publish atomically. Identical events from previous runs are skipped.
        with sqlite3.connect(db_path) as conn:
            conn.execute("BEGIN")
            for row in valid_events:
                existing = conn.execute(
                    "SELECT content_hash FROM transaction_events WHERE event_id=?",
                    (row["event_id"],)
                ).fetchone()
                if existing:
                    if existing[0] == row["_content_hash"]:
                        duplicate_existing += 1
                    else:
                        conflict_existing.append(row)
                        rejected.append({
                            "event_id": row["event_id"], "transaction_id": row["transaction_id"],
                            "source_file": source_name, "source_line": row["_source_line"],
                            "rejection_reason": "event_id already exists with different content",
                            "raw_record_json": json.dumps({k: v for k, v in row.items() if not k.startswith("_")},
                                                           sort_keys=True, default=str),
                        })
                    continue
                conn.execute("""
                    INSERT INTO transaction_events
                    (event_id, transaction_id, account_id, entry_type, amount, currency,
                     event_timestamp, source_file, run_id, content_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row["event_id"], row["transaction_id"], row["account_id"], row["entry_type"],
                    row["amount"], row["currency"], row["event_timestamp"], source_name, run_id,
                    row["_content_hash"]
                ))
                accepted_new += 1

            # Rebuild summaries from the persisted curated events to keep summaries consistent.
            conn.execute("DELETE FROM transaction_summary")
            db_rows = conn.execute("""
                SELECT transaction_id, currency, entry_type, amount
                FROM transaction_events
            """).fetchall()
            totals: dict[tuple[str, str], dict[str, Decimal]] = defaultdict(
                lambda: {"debit": Decimal("0.00"), "credit": Decimal("0.00")}
            )
            for txid, currency, entry_type, amount in db_rows:
                totals[(txid, currency)][entry_type] += Decimal(amount)
            for (txid, currency), total in totals.items():
                debit, credit = total["debit"], total["credit"]
                conn.execute("""
                    INSERT INTO transaction_summary
                    (transaction_id, currency, total_debit, total_credit, difference, is_balanced, last_run_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (txid, currency, f"{debit:.2f}", f"{credit:.2f}",
                      f"{debit-credit:.2f}", int(debit == credit), run_id))

            now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            for item in rejected:
                raw_json = item.get("raw_record_json", "{}")
                try:
                    raw_obj = json.loads(raw_json)
                except json.JSONDecodeError:
                    raw_obj = {"raw": raw_json}
                rid = quarantine_id(source_name, item.get("source_line"), item["rejection_reason"], raw_obj)
                conn.execute("""
                    INSERT OR IGNORE INTO quarantined_records
                    (reject_id, event_id, transaction_id, source_file, source_line,
                     rejection_reason, run_id, rejected_at, raw_record_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (rid, item.get("event_id"), item.get("transaction_id"), source_name,
                      item.get("source_line"), item["rejection_reason"], run_id, now, raw_json))
            conn.commit()

        # CSV output is a convenient snapshot of the current curated database.
        with sqlite3.connect(db_path) as conn:
            curated = conn.execute("""
                SELECT event_id, transaction_id, account_id, entry_type, amount, currency,
                       event_timestamp, source_file, run_id
                FROM transaction_events ORDER BY event_timestamp, event_id
            """).fetchall()
            summary = conn.execute("""
                SELECT transaction_id, currency, total_debit, total_credit, difference, is_balanced
                FROM transaction_summary ORDER BY transaction_id, currency
            """).fetchall()
            quarantine_rows = conn.execute("""
                SELECT event_id, transaction_id, source_file, source_line, rejection_reason,
                       run_id, rejected_at, raw_record_json
                FROM quarantined_records ORDER BY rejected_at, reject_id
            """).fetchall()

        def write_csv(path: Path, headers: list[str], data: list[tuple[Any, ...]]) -> None:
            with path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(data)

        write_csv(output_dir / "curated_transactions.csv",
                  ["event_id", "transaction_id", "account_id", "entry_type", "amount", "currency",
                   "event_timestamp", "source_file", "run_id"], curated)
        write_csv(output_dir / "transaction_summary.csv",
                  ["transaction_id", "currency", "total_debit", "total_credit", "difference", "is_balanced"],
                  summary)
        write_csv(output_dir / "rejected_transactions.csv",
                  ["event_id", "transaction_id", "source_file", "source_line", "rejection_reason",
                   "run_id", "rejected_at", "raw_record_json"], quarantine_rows)

        elapsed = round(time.perf_counter() - start, 6)
        accepted_count = len(valid_events) - len(conflict_existing)
        report = {
            "run_id": run_id,
            "source_file": source_name,
            "start_time": start_wall,
            "end_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "status": "SUCCESS_WITH_REJECTS" if rejected else "SUCCESS",
            "input_count": input_count,
            "accepted_input_event_count": accepted_count,
            "newly_published_count": accepted_new,
            "rejected_row_count": len(rejected),
            "duplicate_count": duplicate_count + duplicate_existing,
            "out_of_order_count": out_of_order_count,
            "curated_database_event_count": len(curated),
            "transaction_summary_count": len(summary),
            "duration_seconds": elapsed,
            "records_per_second": round(input_count / elapsed, 2) if elapsed else None,
            "reconciliation": {
                "input_equals_accepted_plus_rejected": input_count == (accepted_count + len(rejected)),
                "curated_events_have_unique_event_ids": len({r[0] for r in curated}) == len(curated),
                "all_published_transaction_summaries_balanced": all(int(r[5]) == 1 for r in summary),
            },
            "notes": [
                "Out-of-order source timestamps are reported but not automatically rejected.",
                "accepted_input_event_count refers to events accepted by this batch before idempotent skipping.",
                "rejected_row_count can include transaction-level rejections for each leg.",
            ],
        }
        # Store run history.
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                INSERT INTO pipeline_runs
                (run_id, source_file, start_time, end_time, status, input_count, accepted_count,
                 rejected_count, duplicate_count, out_of_order_count, duration_seconds, failure_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """, (run_id, source_name, report["start_time"], report["end_time"], report["status"],
                  input_count, accepted_count, len(rejected), report["duplicate_count"],
                  out_of_order_count, elapsed))
            conn.commit()

        (output_dir / "validation_report.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )
        logger.info("run_id=%s status=%s input=%s newly_published=%s rejected=%s",
                    run_id, report["status"], input_count, accepted_new, len(rejected))
        return report

    except Exception as exc:
        elapsed = round(time.perf_counter() - start, 6)
        logger.exception("Pipeline failed: %s", exc)
        failure_report = {
            "run_id": run_id,
            "source_file": source_name,
            "status": "FAILED",
            "failure_reason": str(exc),
            "duration_seconds": elapsed,
        }
        (output_dir / "validation_report.json").write_text(
            json.dumps(failure_report, indent=2), encoding="utf-8"
        )
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate and publish transaction event data.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Input transaction CSV")
    parser.add_argument("--accounts", default=str(DEFAULT_ACCOUNTS), help="Account reference CSV")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT), help="Output directory")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite database path")
    args = parser.parse_args()
    report = run_pipeline(args.input, args.accounts, args.output_dir, args.db)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
