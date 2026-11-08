"""Run the example queries from sql/examples/ and print their results.

Each query is printed with the comment written above it, followed by
its result as a table.

Usage:
    python -m pipeline.run_examples                                     # all files
    python -m pipeline.run_examples sql/examples/02_team_resources.sql  # one file
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.trino_utils import ROOT, SQL_DIR, connect

EXAMPLES_DIR = SQL_DIR / "examples"
MAX_ROWS_SHOWN = 30


def split_queries(text: str) -> list[tuple[str, str]]:
    """Split a SQL file into (comment above the query, query) pairs.

    Same simple rule as trino_utils.split_statements:
    no ';' inside string literals or comments.
    """
    queries = []
    for chunk in text.split(";"):
        lines = chunk.strip().splitlines()
        # Comment lines above the query describe it; comments inside the query stay in the SQL
        sql_start = next((i for i, line in enumerate(lines) if not line.lstrip().startswith("--")), len(lines))
        comment = [line.lstrip("- ").rstrip() for line in lines[:sql_start]]
        sql = "\n".join(lines[sql_start:]).strip()
        if sql:
            queries.append(("\n".join(comment), sql))
    return queries


def format_table(columns: list[str], rows: list[tuple]) -> str:
    """Format rows as a plain text table with aligned columns."""
    shown = [[str(value) for value in row] for row in rows[:MAX_ROWS_SHOWN]]
    widths = [max([len(col)] + [len(row[i]) for row in shown]) for i, col in enumerate(columns)]
    lines = [
        " | ".join(col.ljust(w) for col, w in zip(columns, widths)),
        "-+-".join("-" * w for w in widths),
    ]
    lines += [" | ".join(value.ljust(w) for value, w in zip(row, widths)) for row in shown]
    if len(rows) > MAX_ROWS_SHOWN:
        lines.append(f"... {len(rows) - MAX_ROWS_SHOWN} more rows")
    lines.append(f"({len(rows)} row{'' if len(rows) == 1 else 's'})")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="*", type=Path, help="SQL files to run (default: all examples)")
    args = parser.parse_args()

    files = args.files or sorted(EXAMPLES_DIR.glob("*.sql"))
    conn = connect()

    for path in files:
        path = path.resolve()
        print("=" * 80)
        print(path.relative_to(ROOT))
        print("=" * 80)
        for comment, sql in split_queries(path.read_text(encoding="utf-8")):
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [col[0] for col in cur.description]
            print()
            print(comment or "(no comment)")
            print()
            print(format_table(columns, rows))
        print()


if __name__ == "__main__":
    main()
