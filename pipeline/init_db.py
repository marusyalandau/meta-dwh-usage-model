"""Create schemas and tables, then load seed dimensions.

Usage:
    python -m pipeline.init_db
"""

from pipeline.trino_utils import SQL_DIR, connect, run_file


def main() -> None:
    conn = connect()
    print("Creating tables...")
    for path in sorted((SQL_DIR / "ddl").glob("*.sql")):
        run_file(conn, path)
    print("Loading seed dimensions...")
    for path in sorted((SQL_DIR / "seed").glob("*.sql")):
        run_file(conn, path)
    print("Done.")


if __name__ == "__main__":
    main()
