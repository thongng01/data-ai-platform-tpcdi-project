"""Run the SQL in a file against Trino and print the rows.

The statement travels as a file path rather than as a command-line argument,
because a multi-word SQL string picked apart by shell quoting is the single
most common way to waste time when poking at a running system.

    cat > /tmp/q.sql <<'SQL'
    SHOW TABLES FROM hive.silver
    SQL
    docker compose --profile tools run --rm tools python -m tools.sql_file /q.sql

/q.sql is bind-mounted from misc/ into the container, so write the file inside
the repository, not /tmp.
"""

import sys
from pathlib import Path

import trino

from .smoke import TRINO_HOST, TRINO_PORT, TRINO_USER


def run(statement, catalog="hive", schema="default"):
    """Execute one statement and return (columns, rows).

    Args:
        statement: The SQL text.
        catalog: Trino catalog to run against.
        schema: Trino schema to run against.

    Returns:
        A tuple of column names and a list of row tuples.
    """
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=catalog,
        schema=schema,
        http_scheme="http",
    )
    try:
        cursor = conn.cursor()
        cursor.execute(statement.rstrip().rstrip(";"))
        columns = [d[0] for d in cursor.description] if cursor.description else []
        return columns, cursor.fetchall()
    finally:
        conn.close()


def main(argv):
    """Read a SQL file, run it, and print the result."""
    if not argv:
        print("usage: python -m tools.sql_file <path.sql>")
        return 2

    path = Path(argv[0])
    if not path.is_file():
        print(f"no such file: {path}")
        return 2

    columns, rows = run(path.read_text())
    if columns:
        print(" | ".join(columns))
    for row in rows:
        print(" | ".join("" if v is None else str(v) for v in row))
    print(f"({len(rows)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))