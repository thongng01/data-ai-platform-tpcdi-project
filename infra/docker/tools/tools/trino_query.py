"""Run one SQL statement against Trino and print the rows.

    docker compose --profile tools run --rm tools \
        python -m tools.trino_query "SHOW TABLES FROM hive.silver"

The statement may also come from the TRINO_SQL environment variable, which
avoids the shell quoting that a multi-word argument picks up on its way through
docker compose.

Useful for checking what the metastore thinks exists, which is a different
question from what the object store holds. When Spark reports
LOCATION_ALREADY_EXISTS with an empty prefix, the catalog row is the thing to
look at.
"""

import os
import sys

import trino

from .smoke import TRINO_HOST, TRINO_PORT, TRINO_USER


def main(argv):
    """Execute the statement given on the command line and print the rows."""
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
        columns = [d[0] for d in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
    finally:
        conn.close()

    if columns:
        print(" | ".join(columns))
    for row in rows:
        print(" | ".join("" if v is None else str(v) for v in row))
    print(f"({len(rows)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
