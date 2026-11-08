"""Table maintenance: retention, compaction and snapshot cleanup.

In production this is a weekly Airflow DAG. Steps, in order:
  1. retention          delete partitions older than the retention period
  2. optimize           compact small data files into larger ones
  3. expire_snapshots   drop old snapshots and the files only they refer to
  4. remove_orphan_files delete files left behind by failed writes

Usage:
    python -m pipeline.maintenance                     # retention counted from today
    python -m pipeline.maintenance --as-of 2027-02-01  # pretend it is another day
    python -m pipeline.maintenance --dry-run           # only print the statements
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

from pipeline.trino_utils import connect, execute

SCHEMAS = ("raw", "meta")

# table -> (date column, days to keep). Tables not listed are kept forever
# (aggregates and dimensions are small and used for long-term trends).
RETENTION = {
    "iceberg.raw.trino_query_completed": ("event_date", 90),
    "iceberg.meta.fact_query": ("query_date", 730),
    "iceberg.meta.fact_query_table_access": ("query_date", 730),
}

# Snapshots younger than this are kept, so a bad load can still be rolled back
# with time travel. Trino refuses values below iceberg.expire-snapshots.min-retention
# (7 days by default).
SNAPSHOT_RETENTION = "7d"


def list_tables(conn) -> list[str]:
    schemas = ", ".join(f"'{s}'" for s in SCHEMAS)
    rows = execute(
        conn,
        "SELECT table_schema, table_name FROM iceberg.information_schema.tables "
        f"WHERE table_schema IN ({schemas}) AND table_type = 'BASE TABLE' "
        "ORDER BY 1, 2",
    )
    return [f"iceberg.{schema}.{table}" for schema, table in rows]


def metadata_table(table: str, suffix: str) -> str:
    """Name of an Iceberg metadata table, e.g. iceberg.meta."fact_query$files"."""
    catalog, schema, name = table.split(".")
    return f'{catalog}.{schema}."{name}${suffix}"'


def count(conn, sql: str) -> int:
    return execute(conn, sql)[0][0]


def apply_retention(conn, as_of: date, dry_run: bool) -> None:
    print(f"1. Retention (as of {as_of})")
    for table, (column, keep_days) in RETENTION.items():
        cutoff = as_of - timedelta(days=keep_days)
        where = f"{column} < DATE '{cutoff.isoformat()}'"
        if dry_run:
            print(f"   DELETE FROM {table} WHERE {where}")
            continue
        old_rows = count(conn, f"SELECT count(*) FROM {table} WHERE {where}")
        if old_rows:
            execute(conn, f"DELETE FROM {table} WHERE {where}")
        print(f"   {table}: keep {keep_days} days, deleted {old_rows} rows before {cutoff}")


def optimize(conn, tables: list[str], dry_run: bool) -> None:
    print("2. Compaction (optimize)")
    for table in tables:
        statement = f"ALTER TABLE {table} EXECUTE optimize"
        if dry_run:
            print(f"   {statement}")
            continue
        files_sql = f"SELECT count(*) FROM {metadata_table(table, 'files')}"
        before = count(conn, files_sql)
        execute(conn, statement)
        after = count(conn, files_sql)
        print(f"   {table}: data files {before} -> {after}")


def expire_snapshots(conn, tables: list[str], dry_run: bool) -> None:
    print(f"3. Expire snapshots older than {SNAPSHOT_RETENTION}")
    for table in tables:
        statement = f"ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '{SNAPSHOT_RETENTION}')"
        if dry_run:
            print(f"   {statement}")
            continue
        snapshots_sql = f"SELECT count(*) FROM {metadata_table(table, 'snapshots')}"
        before = count(conn, snapshots_sql)
        execute(conn, statement)
        after = count(conn, snapshots_sql)
        print(f"   {table}: snapshots {before} -> {after}")


def remove_orphan_files(conn, tables: list[str], dry_run: bool) -> None:
    print(f"4. Remove orphan files older than {SNAPSHOT_RETENTION}")
    for table in tables:
        statement = f"ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '{SNAPSHOT_RETENTION}')"
        if dry_run:
            print(f"   {statement}")
            continue
        execute(conn, statement)
        print(f"   {table}: done")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", type=date.fromisoformat, default=date.today(),
                        help="date to count retention from (default: today)")
    parser.add_argument("--dry-run", action="store_true", help="print statements without running them")
    args = parser.parse_args()

    conn = connect()
    tables = list_tables(conn)
    apply_retention(conn, args.as_of, args.dry_run)
    optimize(conn, tables, args.dry_run)
    expire_snapshots(conn, tables, args.dry_run)
    remove_orphan_files(conn, tables, args.dry_run)
    print("Done.")


if __name__ == "__main__":
    main()
