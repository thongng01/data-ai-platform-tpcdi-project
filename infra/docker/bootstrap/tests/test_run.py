"""Test bootstrap control flow with fake dependencies and no service access."""

import importlib.util
import os
import sys
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, Mock, call, patch


class ClientError(Exception):
    """Represent an S3 error code without importing botocore."""

    def __init__(self, code):
        """Store the response field that bootstrap uses to classify S3 errors."""
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class DuplicateDatabase(Exception):
    """Represent a database created concurrently by another bootstrap run."""


class OperationalError(Exception):
    """Represent a failed PostgreSQL connection attempt."""


def fake_module(name, **attributes):
    """Create an importable dependency stub with only the requested attributes."""
    module = ModuleType(name)
    module.__dict__.update(attributes)
    return module


@contextmanager
def bootstrap_module():
    """Load bootstrap with dummy settings and restore imports after each test."""
    boto3 = fake_module("boto3", client=Mock())
    pg = fake_module(
        "psycopg2",
        connect=MagicMock(),
        OperationalError=OperationalError,
        errors=SimpleNamespace(DuplicateDatabase=DuplicateDatabase),
        sql=Mock(spec=["SQL", "Identifier"]),
    )
    config = Mock()
    dependencies = {
        "boto3": boto3,
        "botocore": fake_module("botocore"),
        "botocore.config": fake_module("botocore.config", Config=config),
        "botocore.exceptions": fake_module("botocore.exceptions", ClientError=ClientError),
        "psycopg2": pg,
        "psycopg2.extensions": fake_module(
            "psycopg2.extensions", ISOLATION_LEVEL_AUTOCOMMIT=0
        ),
    }
    environment = {
        "S3_ENDPOINT": "http://storage.invalid:8333",
        "S3_ACCESS_KEY": "unit-test-key",
        "S3_SECRET_KEY": "unit-test-secret",
        "PGPASSWORD": "unit-test-password",
    }
    path = Path(__file__).resolve().parents[1] / "bootstrap" / "run.py"
    # Clear the environment so local credentials cannot enter the module.
    # Stub every service dependency before executing its top-level imports.
    with patch.dict(os.environ, environment, clear=True), patch.dict(sys.modules, dependencies):
        spec = importlib.util.spec_from_file_location("bootstrap_under_test", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        yield module, boto3, pg, config


class BootstrapTests(unittest.TestCase):
    """Check bucket setup, database setup, retries, and bootstrap ordering."""

    def setUp(self):
        """Load an isolated module so tests cannot connect to real services."""
        self.run, self.boto3, self.pg, self.config = self.enterContext(bootstrap_module())

    def test_s3_client_uses_endpoint_credentials_and_path_addressing(self):
        """Keep S3 requests on the configured endpoint with bucket path addressing."""
        client = self.run._s3_client()
        self.config.assert_called_once_with(
            s3={"addressing_style": "path"}, signature_version="s3v4"
        )
        self.boto3.client.assert_called_once_with(
            "s3",
            endpoint_url="http://storage.invalid:8333",
            aws_access_key_id="unit-test-key",
            aws_secret_access_key="unit-test-secret",
            region_name="us-east-1",
            config=self.config.return_value,
        )
        self.assertIs(client, self.boto3.client.return_value)

    def test_create_buckets_skips_existing_names(self):
        """Create only buckets absent from the initial listing."""
        client = Mock()
        client.list_buckets.return_value = {"Buckets": [{"Name": "lakehouse"}]}
        self.run.create_buckets(client, ["lakehouse", "landing"])
        client.create_bucket.assert_called_once_with(Bucket="landing")

    def test_create_buckets_accepts_missing_listing_field(self):
        """Treat a response without Buckets as an empty listing."""
        client = Mock()
        client.list_buckets.return_value = {}
        self.run.create_buckets(client, ["lakehouse"])
        client.create_bucket.assert_called_once_with(Bucket="lakehouse")

    def test_create_buckets_accepts_concurrent_creation(self):
        """Continue when another bootstrap run has already created a bucket."""
        for code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            with self.subTest(code=code):
                client = Mock()
                client.list_buckets.return_value = {}
                client.create_bucket.side_effect = ClientError(code)
                self.run.create_buckets(client, ["lakehouse"])

    def test_create_buckets_raises_unexpected_s3_error(self):
        """Stop bucket setup when an error is not an already-exists response."""
        client = Mock()
        client.list_buckets.return_value = {}
        client.create_bucket.side_effect = ClientError("AccessDenied")
        with self.assertRaisesRegex(ClientError, "AccessDenied"):
            self.run.create_buckets(client, ["lakehouse"])

    def test_seed_prefixes_strips_slashes_and_writes_empty_markers(self):
        """Use stable marker keys so repeated writes replace the same objects."""
        client = Mock()
        self.run.seed_prefixes(client, "lakehouse", ["/spark-logs/", "warehouse"])
        self.assertEqual(
            client.put_object.call_args_list,
            [
                call(Bucket="lakehouse", Key="spark-logs/_keep", Body=b""),
                call(Bucket="lakehouse", Key="warehouse/_keep", Body=b""),
            ],
        )

    def test_seed_prefixes_warns_and_continues_after_error(self):
        """Keep attempting later markers after an S3 write fails."""
        client = Mock()
        client.put_object.side_effect = [ClientError("AccessDenied"), {}]
        with self.assertLogs("bootstrap", level="WARNING") as captured:
            self.run.seed_prefixes(client, "lakehouse", ["spark-logs", "warehouse"])
        self.assertIn("could not seed spark-logs", captured.output[0])
        self.assertEqual(client.put_object.call_count, 2)

    def test_create_databases_uses_autocommit_and_identifier_composition(self):
        """Skip existing databases and compose new names as SQL identifiers."""
        conn = self.pg.connect.return_value
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [("warehouse",)]
        self.run.create_databases(["warehouse", 'quoted"name'])
        conn.set_isolation_level.assert_called_once_with(0)
        self.pg.sql.Identifier.assert_called_once_with('quoted"name')
        self.pg.sql.SQL.assert_called_once_with("CREATE DATABASE {}")
        cursor.execute.assert_has_calls(
            [
                call("SELECT datname FROM pg_database"),
                call(self.pg.sql.SQL.return_value.format.return_value),
            ]
        )
        conn.close.assert_called_once_with()

    def test_create_databases_accepts_concurrent_creation(self):
        """Keep the connection usable when another run creates the same database."""
        conn = self.pg.connect.return_value
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = []
        cursor.execute.side_effect = [None, DuplicateDatabase()]
        self.run.create_databases(["warehouse"])
        conn.close.assert_called_once_with()

    def test_create_databases_closes_connection_on_failure(self):
        """Close the maintenance connection when a database statement fails."""
        conn = self.pg.connect.return_value
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = RuntimeError("statement failed")
        with self.assertRaisesRegex(RuntimeError, "statement failed"):
            self.run.create_databases(["warehouse"])
        conn.close.assert_called_once_with()

    def test_wait_for_postgres_retries_and_closes_successful_connection(self):
        """Pause two seconds after a failed connection, then return on success."""
        conn = Mock()
        self.pg.connect.side_effect = [OperationalError("not ready"), conn]
        with patch.object(self.run.time, "time", side_effect=[0, 0, 2]), patch.object(
            self.run.time, "sleep"
        ) as sleep:
            self.run.wait_for_postgres(timeout=5)
        sleep.assert_called_once_with(2)
        conn.close.assert_called_once_with()
        self.assertEqual(self.pg.connect.call_count, 2)

    def test_wait_for_postgres_reports_last_error_at_deadline(self):
        """Raise a timeout when no connection succeeds within the retry window."""
        self.pg.connect.side_effect = OperationalError("not ready")
        with patch.object(self.run.time, "time", side_effect=[0, 0, 5]), patch.object(
            self.run.time, "sleep"
        ):
            with self.assertRaisesRegex(TimeoutError, "after 5s: not ready"):
                self.run.wait_for_postgres(timeout=5)

    def test_main_runs_setup_in_dependency_order(self):
        """Wait for PostgreSQL before creating databases, buckets, and markers."""
        steps = Mock()
        with patch.object(self.run.logging, "basicConfig"), patch.multiple(
            self.run,
            wait_for_postgres=steps.wait,
            create_databases=steps.databases,
            _s3_client=steps.client,
            create_buckets=steps.buckets,
            seed_prefixes=steps.prefixes,
        ):
            self.assertEqual(self.run.main(), 0)
        self.assertEqual(
            steps.mock_calls,
            [
                call.wait(),
                call.databases(self.run.PG_DATABASES),
                call.client(),
                call.buckets(steps.client.return_value, ["lakehouse", "landing"]),
                call.prefixes(
                    steps.client.return_value,
                    "lakehouse",
                    ["spark-logs", "warehouse", "tmp", "bronze", "silver", "gold"],
                ),
            ],
        )


if __name__ == "__main__":
    unittest.main()
