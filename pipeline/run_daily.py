"""Run the daily transformations for one date or a date range (backfill).

In production this would be one Airflow DAG run per date
(the logical date is passed as run_date). Locally:

    python -m pipeline.run_daily --date 2026-11-01
    python -m pipeline.run_daily --start 2026-10-01 --end 2026-10-31   # backfill
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

from pipeline.trino_utils import SQL_DIR, connect, run_file


def daterange(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def run_for_date(conn, run_date: date) -> None:
    print(f"Loading {run_date}...")
    # Files are numbered in dependency order
    for path in sorted((SQL_DIR / "transform").glob("*.sql")):
        run_file(conn, path, {"run_date": run_date.isoformat()})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=date.fromisoformat, help="single date to load")
    parser.add_argument("--start", type=date.fromisoformat, help="backfill start date")
    parser.add_argument("--end", type=date.fromisoformat, help="backfill end date (inclusive)")
    args = parser.parse_args()

    if args.date:
        start = end = args.date
    elif args.start and args.end:
        start, end = args.start, args.end
    else:
        parser.error("use --date or --start/--end")

    conn = connect()
    for run_date in daterange(start, end):
        run_for_date(conn, run_date)


if __name__ == "__main__":
    main()
