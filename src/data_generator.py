"""Deterministic synthetic data generator for pipeline testing."""
from __future__ import annotations
import argparse
import csv
import random
from pathlib import Path
from datetime import datetime, timedelta, timezone

ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = ROOT / "data" / "input"
COLUMNS = ["event_id", "transaction_id", "account_id", "entry_type", "amount", "currency", "event_timestamp"]


def generate(rows: int, seed: int, output: Path, accounts_output: Path) -> None:
    if rows < 0:
        raise ValueError("rows must be zero or greater")
    rng = random.Random(seed)
    accounts = [f"ACC{i:04d}" for i in range(1, 101)]
    accounts_output.parent.mkdir(parents=True, exist_ok=True)
    with accounts_output.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["account_id", "account_type"])
        for account in accounts:
            w.writerow([account, "synthetic"])

    records = []
    start = datetime(2026, 10, 29, 9, 0, tzinfo=timezone.utc)
    tx_number = 1
    while len(records) < rows:
        txid = f"TXN{tx_number:07d}"
        amount_cents = rng.randint(100, 250000)
        amount = f"{amount_cents / 100:.2f}"
        curr = rng.choice(["INR", "INR", "INR", "USD"])
        account1, account2 = rng.sample(accounts, 2)
        stamp = start + timedelta(seconds=tx_number * 3)
        # Most transactions are balanced; every 20th generated transaction is imbalanced.
        credit_amount = amount if tx_number % 20 else f"{(amount_cents + 25) / 100:.2f}"
        pair = [
            [f"EVT{tx_number:07d}A", txid, account1, "debit", amount, curr, stamp.isoformat().replace("+00:00", "Z")],
            [f"EVT{tx_number:07d}B", txid, account2, "credit", credit_amount, curr,
             (stamp + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")],
        ]
        records.extend(pair)
        tx_number += 1
    records = records[:rows]

    # Deterministic negative cases are injected only when there are enough rows.
    if len(records) >= 12:
        records[3][4] = "not-a-number"             # malformed amount
        records[5][2] = "ACC9999"                  # invalid account reference
        records[7][5] = "XYZ"                      # unsupported currency
        records[9][0] = records[8][0]              # duplicate event ID
        records[11][6] = "not-a-timestamp"         # malformed timestamp
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLUMNS)
        w.writerows(records)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=40, help="Number of event rows to generate")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=INPUT_DIR / "transactions.csv")
    parser.add_argument("--accounts-output", type=Path, default=INPUT_DIR / "accounts.csv")
    args = parser.parse_args()
    generate(args.rows, args.seed, args.output, args.accounts_output)
    print(f"Generated {args.rows} rows at {args.output}")
    print(f"Generated account reference data at {args.accounts_output}")


if __name__ == "__main__":
    main()
