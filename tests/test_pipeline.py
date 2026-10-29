import csv
import json
import sqlite3
from pathlib import Path

from src.data_generator import generate
from src.pipeline import run_pipeline


def make_files(tmp_path: Path, rows: int = 40):
    input_path = tmp_path / "transactions.csv"
    accounts_path = tmp_path / "accounts.csv"
    generate(rows, 42, input_path, accounts_path)
    return input_path, accounts_path


def test_pipeline_writes_expected_outputs(tmp_path):
    input_path, accounts_path = make_files(tmp_path, 40)
    out = tmp_path / "out"
    db = tmp_path / "pipeline.db"
    report = run_pipeline(input_path, accounts_path, out, db)
    assert report["input_count"] == 40
    assert report["rejected_row_count"] > 0
    assert (out / "curated_transactions.csv").exists()
    assert (out / "rejected_transactions.csv").exists()
    assert (out / "transaction_summary.csv").exists()
    assert (out / "validation_report.json").exists()
    assert report["reconciliation"]["curated_events_have_unique_event_ids"] is True


def test_rerun_is_idempotent(tmp_path):
    input_path, accounts_path = make_files(tmp_path, 40)
    out = tmp_path / "out"
    db = tmp_path / "pipeline.db"
    first = run_pipeline(input_path, accounts_path, out, db)
    with sqlite3.connect(db) as conn:
        before = conn.execute("SELECT COUNT(*) FROM transaction_events").fetchone()[0]
    second = run_pipeline(input_path, accounts_path, out, db)
    with sqlite3.connect(db) as conn:
        after = conn.execute("SELECT COUNT(*) FROM transaction_events").fetchone()[0]
    assert before == after
    assert second["newly_published_count"] == 0


def test_empty_input_completes(tmp_path):
    input_path, accounts_path = make_files(tmp_path, 0)
    out = tmp_path / "out"
    report = run_pipeline(input_path, accounts_path, out, tmp_path / "db.sqlite")
    assert report["input_count"] == 0
    assert report["status"] == "SUCCESS"


def test_missing_required_header_fails_clearly(tmp_path):
    input_path = tmp_path / "transactions.csv"
    input_path.write_text("event_id,transaction_id\nE1,T1\n", encoding="utf-8")
    accounts_path = tmp_path / "accounts.csv"
    accounts_path.write_text("account_id\nACC0001\n", encoding="utf-8")
    import pytest
    with pytest.raises(ValueError, match="Missing required column"):
        run_pipeline(input_path, accounts_path, tmp_path / "out", tmp_path / "db.sqlite")


def test_invalid_records_are_traceable(tmp_path):
    input_path, accounts_path = make_files(tmp_path, 40)
    out = tmp_path / "out"
    run_pipeline(input_path, accounts_path, out, tmp_path / "db.sqlite")
    with (out / "rejected_transactions.csv").open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert rows
    assert all(row["rejection_reason"] for row in rows)
    assert all(row["source_file"] == "transactions.csv" for row in rows)


def test_balanced_summary_only(tmp_path):
    input_path, accounts_path = make_files(tmp_path, 40)
    out = tmp_path / "out"
    run_pipeline(input_path, accounts_path, out, tmp_path / "db.sqlite")
    with sqlite3.connect(tmp_path / "db.sqlite") as conn:
        bad = conn.execute("SELECT COUNT(*) FROM transaction_summary WHERE is_balanced != 1").fetchone()[0]
    assert bad == 0


def test_report_reconciles_input_rows(tmp_path):
    input_path, accounts_path = make_files(tmp_path, 40)
    report = run_pipeline(input_path, accounts_path, tmp_path / "out", tmp_path / "db.sqlite")
    assert report["reconciliation"]["input_equals_accepted_plus_rejected"] is True


def test_generated_imbalanced_transactions_do_not_enter_curated_output(tmp_path):
    input_path, accounts_path = make_files(tmp_path, 40)
    out = tmp_path / "out"
    run_pipeline(input_path, accounts_path, out, tmp_path / "db.sqlite")
    with (out / "transaction_summary.csv").open(newline="", encoding="utf-8") as f:
        summaries = list(csv.DictReader(f))
    assert summaries
    assert all(row["is_balanced"] == "1" for row in summaries)
