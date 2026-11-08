"""Generate fake Trino query events into iceberg.raw.trino_query_completed.

Simulates what the Trino event listener would write in production:
a mix of reads (analysts, dashboards), writes (Airflow pipelines),
table maintenance and cheap metadata queries (e.g. DataGrip introspection).

Usage:
    python -m pipeline.generate_events --start 2026-10-01 --days 31 --queries-per-day 200
"""

from __future__ import annotations

import argparse
import random
import uuid
from datetime import date, datetime, time, timedelta, timezone

from pipeline.trino_utils import connect, execute

HUMANS = ["anna", "boris", "clara", "dmitry", "elena", "felix"]
SERVICES = ["svc_airflow", "svc_superset"]

# (catalog, schema, table, columns, typical bytes read)
TABLES = [
    ("iceberg", "product", "events", ["event_id", "user_id", "event_name", "event_time"], 8 * 1024**3),
    ("iceberg", "product", "users", ["user_id", "country", "plan"], 200 * 1024**2),
    ("iceberg", "product", "sessions", ["session_id", "user_id", "duration"], 2 * 1024**3),
    ("iceberg", "marketing", "campaigns", ["campaign_id", "channel", "budget"], 50 * 1024**2),
    ("postgresql", "public", "licenses", ["license_id", "user_id", "product"], 10 * 1024**2),
]
ICEBERG_TABLES = [t for t in TABLES if t[0] == "iceberg"]

# Tables produced by Airflow pipelines (targets of INSERT / MERGE)
OUTPUT_TABLES = [
    ("iceberg", "analytics", "daily_active_users"),
    ("iceberg", "analytics", "sessions_daily"),
    ("iceberg", "marketing", "campaign_performance"),
]

# Trino query types per category, with weights
QUERY_TYPES = {
    "read": (["SELECT"], [1]),
    "write": (["INSERT", "MERGE"], [7, 3]),
    "maintenance": (["ALTER_TABLE_EXECUTE", "ANALYZE"], [8, 2]),
    "metadata": (["DESCRIBE", "EXPLAIN", "DATA_DEFINITION"], [7, 1, 2]),
}

ROW_TYPE = (
    "ROW(catalog_name VARCHAR, schema_name VARCHAR, table_name VARCHAR, "
    "columns ARRAY(VARCHAR), physical_input_bytes BIGINT)"
)


def sql_str(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def sql_ts(value: datetime) -> str:
    return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S.%f')} UTC'"


def pick_profile(user: str) -> tuple[str, str]:
    """Return (source, category) typical for this user."""
    if user == "svc_airflow":
        category = random.choices(["write", "maintenance", "read"], weights=[8, 1, 1])[0]
        return "airflow", category
    if user == "svc_superset":
        return "superset", "read"
    # Humans: DataGrip sends many metadata queries (schema introspection,
    # DESCRIBE on table open), Jupyter notebooks mostly run plain SELECTs.
    source = random.choice(["datagrip", "datagrip", "jupyter"])
    if source == "datagrip":
        category = random.choices(["read", "metadata"], weights=[6, 4])[0]
    else:
        category = random.choices(["read", "metadata"], weights=[9, 1])[0]
    return source, category


def fake_query(day: date) -> str:
    """Return one VALUES tuple describing a random query."""
    user = random.choice(HUMANS + HUMANS + SERVICES)
    source, category = pick_profile(user)
    types, weights = QUERY_TYPES[category]
    query_type = random.choices(types, weights=weights)[0]

    # Which tables are read and which one is written
    if category in ("read", "write"):
        tables = random.sample(TABLES, k=random.choice([1, 1, 1, 2, 2, 3]))
    elif category == "maintenance":
        tables = [random.choice(ICEBERG_TABLES)]  # rewrites the table it reads
    else:
        tables = []  # metadata queries do not read table data

    if category == "write":
        output = random.choice(OUTPUT_TABLES)
    elif category == "maintenance":
        output = tables[0][:3]
    else:
        output = (None, None, None)

    inputs = []
    total_bytes = 0
    for catalog, schema, table, columns, typical_bytes in tables:
        share = 1.0 if category == "maintenance" else random.uniform(0.05, 1.0)
        read_bytes = int(typical_bytes * share)
        total_bytes += read_bytes
        cols = columns if category == "maintenance" else random.sample(columns, k=random.randint(1, len(columns)))
        cols_sql = "ARRAY[" + ", ".join(sql_str(c) for c in cols) + "]"
        inputs.append(
            f"CAST(ROW({sql_str(catalog)}, {sql_str(schema)}, {sql_str(table)}, {cols_sql}, {read_bytes}) AS {ROW_TYPE})"
        )
    inputs_sql = "ARRAY[" + ", ".join(inputs) + "]" if inputs else f"CAST(ARRAY[] AS ARRAY({ROW_TYPE}))"

    if category == "metadata":
        # Cheap: answered from catalog metadata, no data is scanned
        cpu_ms = random.randint(5, 200)
        wall_ms = random.randint(20, 500)
        queued_ms = 0
        peak_memory = random.randint(1, 10) * 1024**2
        output_bytes = random.randint(100, 50_000)
    else:
        # Rough model: ~1 CPU second per 100 MB read, plus noise.
        # Writes and maintenance also spend CPU on encoding new files.
        write_factor = 1.0 if category == "read" else 1.5
        cpu_ms = int(total_bytes / (100 * 1024**2) * 1000 * random.uniform(0.5, 2.0) * write_factor) + 50
        wall_ms = int(cpu_ms / random.uniform(4, 16)) + 100
        queued_ms = random.choice([0, 0, 0, random.randint(100, 30000)])
        peak_memory = int(total_bytes * random.uniform(0.01, 0.2))
        if category == "read":
            output_bytes = random.randint(1_000, 10_000_000)
        else:
            output_bytes = int(total_bytes * random.uniform(0.05, 0.5))  # size of written files

    failed = random.random() < (0.01 if category == "metadata" else 0.05)

    create_time = datetime.combine(day, time.min, tzinfo=timezone.utc) + timedelta(
        seconds=random.randint(0, 86400 - 3600)
    )
    start_time = create_time + timedelta(milliseconds=queued_ms)
    end_time = start_time + timedelta(milliseconds=wall_ms)

    table_names = ", ".join(f"{c}.{s}.{t}" for c, s, t, _, _ in tables)
    out_name = ".".join(output) if output[0] else None
    query_text = {
        "read": f"SELECT ... FROM {table_names}",
        "write": f"INSERT INTO {out_name} SELECT ... FROM {table_names}"
        if query_type == "INSERT"
        else f"MERGE INTO {out_name} USING (SELECT ... FROM {table_names}) ...",
        "maintenance": f"ALTER TABLE {out_name} EXECUTE optimize"
        if query_type == "ALTER_TABLE_EXECUTE"
        else f"ANALYZE {out_name}",
        "metadata": f"{query_type} ...",
    }[category]

    values = [
        sql_str(f"{day:%Y%m%d}_{uuid.uuid4().hex[:12]}"),
        sql_str("FAILED" if failed else "FINISHED"),
        sql_str("EXCEEDED_TIME_LIMIT" if failed else None),
        sql_str(query_type),
        sql_str(user),
        sql_str(source),
        sql_str("global.adhoc" if not user.startswith("svc_") else "global.etl"),
        sql_str(query_text),
        sql_ts(create_time),
        sql_ts(start_time),
        sql_ts(end_time),
        str(cpu_ms),
        str(wall_ms),
        str(queued_ms),
        str(peak_memory),
        str(total_bytes),
        str(total_bytes // 100),  # rows, very rough
        str(output_bytes),
        inputs_sql,
        sql_str(output[0]),
        sql_str(output[1]),
        sql_str(output[2]),
        f"DATE '{end_time.date().isoformat()}'",
    ]
    return "(" + ", ".join(values) + ")"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--queries-per-day", type=int, default=200)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    conn = connect()

    for offset in range(args.days):
        day = args.start + timedelta(days=offset)
        rows = [fake_query(day) for _ in range(args.queries_per_day)]
        for i in range(0, len(rows), args.batch_size):
            batch = rows[i : i + args.batch_size]
            execute(conn, "INSERT INTO iceberg.raw.trino_query_completed VALUES " + ", ".join(batch))
        print(f"  {day}: {len(rows)} queries")


if __name__ == "__main__":
    main()
