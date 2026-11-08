"""Small helpers to run SQL files against the local Trino."""

from __future__ import annotations

import os
import re
from pathlib import Path

import trino

ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = ROOT / "sql"


def connect() -> trino.dbapi.Connection:
    """Open a connection to Trino (defaults match docker-compose.yml)."""
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "pipeline"),
        catalog="iceberg",
    )


def split_statements(sql: str) -> list[str]:
    """Split a SQL script into statements.

    Trino accepts one statement per request, so scripts are split on ';'.
    Line comments are removed first. This is intentionally simple:
    do not put ';' or '--' inside string literals in the SQL files.
    """
    without_comments = re.sub(r"--[^\n]*", "", sql)
    return [s.strip() for s in without_comments.split(";") if s.strip()]


def render(sql: str, params: dict[str, str] | None = None) -> str:
    """Replace {{ name }} placeholders with values."""
    for key, value in (params or {}).items():
        sql = re.sub(r"\{\{\s*" + re.escape(key) + r"\s*\}\}", value, sql)
    leftover = re.findall(r"\{\{\s*\w+\s*\}\}", sql)
    if leftover:
        raise ValueError(f"Unfilled placeholders: {leftover}")
    return sql


def execute(conn: trino.dbapi.Connection, statement: str) -> list[tuple]:
    cur = conn.cursor()
    cur.execute(statement)
    return cur.fetchall()


def run_file(conn: trino.dbapi.Connection, path: Path, params: dict[str, str] | None = None) -> None:
    """Run every statement of a SQL file, in order."""
    sql = render(path.read_text(encoding="utf-8"), params)
    for statement in split_statements(sql):
        execute(conn, statement)
    print(f"  ok  {path.relative_to(ROOT)}")
