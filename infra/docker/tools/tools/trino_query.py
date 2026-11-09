"""Run one SQL statement against Trino and print the rows.

    docker compose --profile tools run --rm tools \
        python -m tools.trino_query "SHOW TABLES FROM hive.silver"

The statement can also come from the TRINO_SQL environment variable in the
container. Command-line arguments take priority. An environment variable keeps
SQL quoting out of the container command.

Catalog queries show registered tables and locations. Compare them with S3
listings to distinguish missing catalog entries from missing files.
"""

import os
import sys

import trino

from .smoke import TRINO_HOST, TRINO_PORT, TRINO_USER


def main(argv):
    """Execute command-line SQL or TRINO_SQL and print its column names and rows."""
    # An explicit argument takes priority, even if it contains only whitespace.
    # In that case the usage check fails rather than running environment SQL.
    statement = " ".join(argv) if argv else os.environ.get("TRINO_SQL", "")
    if not statement.strip():
        print("usage: python -m tools.trino_query <sql>   (or set TRINO_SQL)")
        return 2

    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog="hive",
        schema="default",
        http_scheme="http",
    )
    try:
        cursor = conn.cursor()
        cursor.execute(statement)
        # The first item in each DB-API metadata tuple is the column name.
        # A statement without result columns can leave description unset.
        columns = [d[0] for d in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
    finally:
        conn.close()

    if columns:
        print(" | ".join(columns))
    for row in rows:
        # SQL NULL prints as an empty field; zero and False keep their values.
        print(" | ".join("" if v is None else str(v) for v in row))
    print(f"({len(rows)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
