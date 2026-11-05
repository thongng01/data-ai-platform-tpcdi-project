"""Run the SQL in a file against Trino and print the rows.

A file keeps SQL quotes out of the shell command. The file must be visible
inside the tools container. From `infra/`, mount an existing scratch file
from the repository's `local/tmp/` directory read-only:

    docker compose --profile tools run --rm \
        -v "$(pwd)/../local/tmp/q.sql:/q.sql:ro" \
        tools python -m tools.sql_file /q.sql

The file contains one statement. This tool does not split a SQL script into
separate statements.
"""

import sys
from pathlib import Path

import trino

from .smoke import TRINO_HOST, TRINO_PORT, TRINO_USER


def run(statement, catalog="hive", schema="default"):
    """Execute one statement and return its column names and rows.

    Args:
        statement: The SQL text.
        catalog: Trino catalog to run against.
        schema: Trino schema to run against.

    Returns:
        A pair containing the column-name list and the cursor's result rows.
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
        # Trino's statement API does not need a trailing SQL terminator. Remove
        # final semicolons without changing any semicolons inside the statement.
        cursor.execute(statement.rstrip().rstrip(";"))
        # DB-API descriptions hold one metadata tuple per result column.
        # Statements without result columns can leave the description unset.
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
        # Display SQL NULL as an empty field, but keep zero and False visible.
        print(" | ".join("" if v is None else str(v) for v in row))
    print(f"({len(rows)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
