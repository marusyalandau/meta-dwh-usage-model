"""Data quality checks for the model, run after a load.

Every file in sql/checks/ is a query that returns the rows breaking one rule:
no rows means the check passed. The first comment line of the file describes
the rule. Checks marked `-- severity: warn` only print a warning; any other
failed check makes the script exit with code 1, so a scheduler (Airflow)
marks the run as failed and does not start the tasks after it.

Usage:
    python -m pipeline.checks --date 2026-10-15
    python -m pipeline.checks --start 2026-10-01 --end 2026-10-31
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date
from pathlib import Path

from pipeline.trino_utils import SQL_DIR, connect, render, split_statements

CHECKS_DIR = SQL_DIR / "checks"
MAX_ROWS_SHOWN = 5


def describe(sql: str) -> tuple[str, str]:
    """Return (description, severity) taken from the comments of a check file."""
    first_comment = re.search(r"^--\s*(.+)$", sql, flags=re.MULTILINE)
    description = first_comment.group(1).strip() if first_comment else ""
    severity = "warn" if re.search(r"^--\s*severity:\s*warn\b", sql, flags=re.MULTILINE) else "error"
    return description, severity


def run_check(conn, path: Path, params: dict[str, str]) -> tuple[list[str], list[tuple]]:
    """Run the check query, return (column names, failing rows)."""
    statements = split_statements(render(path.read_text(encoding="utf-8"), params))
    if len(statements) != 1:
        raise ValueError(f"{path.name}: a check must contain exactly one query")
    cur = conn.cursor()
    cur.execute(statements[0])
    rows = cur.fetchall()
    columns = [col[0] for col in cur.description]
    return columns, rows


def print_rows(columns: list[str], rows: list[tuple]) -> None:
    print("        " + " | ".join(columns))
    for row in rows[:MAX_ROWS_SHOWN]:
        print("        " + " | ".join(str(value) for value in row))
    if len(rows) > MAX_ROWS_SHOWN:
        print(f"        ... {len(rows) - MAX_ROWS_SHOWN} more rows")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=date.fromisoformat, help="single date to check")
    parser.add_argument("--start", type=date.fromisoformat, help="period start date")
    parser.add_argument("--end", type=date.fromisoformat, help="period end date (inclusive)")
    args = parser.parse_args()

    if args.date:
        start = end = args.date
    elif args.start and args.end:
        start, end = args.start, args.end
    else:
        parser.error("use --date or --start/--end")

    params = {"start_date": start.isoformat(), "end_date": end.isoformat()}
    conn = connect()
    errors = warnings = 0

    print(f"Running checks for {start} .. {end}")
    for path in sorted(CHECKS_DIR.glob("*.sql")):
        sql = path.read_text(encoding="utf-8")
        description, severity = describe(sql)
        columns, rows = run_check(conn, path, params)

        if not rows:
            status = "PASS"
        elif severity == "warn":
            status = "WARN"
            warnings += 1
        else:
            status = "FAIL"
            errors += 1

        print(f"  {status}  {path.stem}: {description}")
        if rows:
            print_rows(columns, rows)

    print(f"Done: {errors} failed, {warnings} warnings.")
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
