"""Print a fresh Airflow Fernet key.

Airflow encrypts Variables and Connections with this key before storing them in
its metadata database. Run this once and paste the result into infra/.env.

    python misc/tools/gen_fernet_key.py
"""

import base64
import os


def main():
    """Print one URL-safe base64 key for a 32-byte Fernet key."""
    print(base64.urlsafe_b64encode(os.urandom(32)).decode())


if __name__ == "__main__":
    main()