"""Test container tools with fake service clients and no network access."""

import importlib
import io
import os
import sys
import unittest
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, Mock, call, patch


class ClientError(Exception):
    """Represent an S3 client failure without importing botocore."""


def fake_module(name, **attributes):
    """Build a dependency stub that cannot open real service connections."""
    module = ModuleType(name)
    module.__dict__.update(attributes)
    return module


@contextmanager
def tools_modules():
    """Load fresh tools with dummy settings and restore imports after each test."""
    package_name = "_infra_tools_under_test"
    directory = Path(__file__).resolve().parents[1] / "tools"
    transfer_config = Mock()
    boto3 = fake_module("boto3", client=Mock())
    trino = fake_module("trino", dbapi=Mock())
    pg = fake_module("psycopg2", connect=MagicMock())
    arrow = fake_module(
        "pyarrow",
        table=Mock(),
        array=Mock(),
        int64=Mock(),
        string=Mock(),
        float64=Mock(),
        timestamp=Mock(),
    )
    parquet = fake_module("pyarrow.parquet", write_table=Mock(), read_table=Mock())
    dependencies = {
        package_name: fake_module(package_name, __path__=[str(directory)]),
        "boto3": boto3,
        "boto3.s3": fake_module("boto3.s3"),
        "boto3.s3.transfer": fake_module("boto3.s3.transfer", TransferConfig=transfer_config),
        "botocore": fake_module("botocore"),
        "botocore.config": fake_module("botocore.config", Config=Mock()),
        "botocore.exceptions": fake_module("botocore.exceptions", ClientError=ClientError),
        "pyarrow": arrow,
        "pyarrow.parquet": parquet,
        "trino": trino,
        "psycopg2": pg,
    }
    environment = {
        "S3_ENDPOINT": "http://storage.invalid:8333",
        "S3_ACCESS_KEY": "unit-test-key",
        "S3_SECRET_KEY": "unit-test-secret",
        "PGPASSWORD": "unit-test-password",
    }
    # Clear real credentials and stub all external clients before module import.
    # A private package name avoids changing the import path of other tests.
    with patch.dict(os.environ, environment, clear=True), patch.dict(sys.modules, dependencies):
        modules = {
            name: importlib.import_module(f"{package_name}.{name}")
            for name in ("smoke", "ls", "purge", "sql_file", "trino_query")
        }
        yield modules, dependencies


class ToolsTests(unittest.TestCase):
    """Check smoke reporting, prefix handling, SQL execution, and cleanup."""

    def setUp(self):
        """Create isolated tool modules and capture command output."""
        self.modules, self.dependencies = self.enterContext(tools_modules())
        self.smoke = self.modules["smoke"]
        self.output = self.enterContext(redirect_stdout(io.StringIO()))
        # Suppress expected failure reports without changing global logging.
        self.enterContext(patch.object(self.smoke, "LOG"))
        self.enterContext(patch.object(self.smoke.logging, "basicConfig"))

    def test_multipart_uses_managed_upload_with_explicit_mib_limits(self):
        """Force the multipart API path instead of a large single PutObject call."""
        client = Mock()
        client.head_object.return_value = {"ContentLength": 24 * 1024 * 1024}
        with patch.object(self.smoke, "s3_client", return_value=client):
            self.smoke.check_s3_multipart()
        transfer_config = self.dependencies["boto3.s3.transfer"].TransferConfig
        transfer_config.assert_called_once_with(
            multipart_threshold=8 * 1024 * 1024,
            multipart_chunksize=8 * 1024 * 1024,
        )
        client.upload_fileobj.assert_called_once()
        payload, bucket, key = client.upload_fileobj.call_args.args
        self.assertIsInstance(payload, io.BytesIO)
        self.assertEqual(payload.getbuffer().nbytes, 24 * 1024 * 1024)
        self.assertEqual(bucket, self.smoke.BUCKET)
        self.assertTrue(key.startswith("smoke/multipart-"))
        self.assertEqual(
            client.upload_fileobj.call_args.kwargs,
            {"Config": transfer_config.return_value},
        )
        client.put_object.assert_not_called()
        client.head_object.assert_called_once_with(Bucket=bucket, Key=key)
        client.delete_object.assert_called_once_with(Bucket=bucket, Key=key)
        self.assertEqual(
            self.smoke._results,
            [("s3_multipart", True, "24 MiB multipart upload succeeded")],
        )

    def test_multipart_records_incorrect_object_size_as_failure(self):
        """Reject a completed upload when the server reports the wrong byte count."""
        client = Mock()
        client.head_object.return_value = {"ContentLength": 1}
        with patch.object(self.smoke, "s3_client", return_value=client):
            self.smoke.check_s3_multipart()
        name, ok, detail = self.smoke._results[0]
        self.assertEqual(name, "s3_multipart")
        self.assertFalse(ok)
        self.assertIn("AssertionError", detail)
        client.upload_fileobj.assert_called_once()

    def test_multipart_reports_upload_error_without_reading_metadata(self):
        """Record upload failure and avoid a misleading size check afterward."""
        client = Mock()
        client.upload_fileobj.side_effect = ClientError("upload failed")
        with patch.object(self.smoke, "s3_client", return_value=client):
            self.smoke.check_s3_multipart()
        self.assertEqual(
            self.smoke._results,
            [("s3_multipart", False, "ClientError: upload failed")],
        )
        client.head_object.assert_not_called()

    def test_check_decorator_records_failure_then_allows_success(self):
        """Keep a failed check from preventing the next check's result."""
        failing = Mock(side_effect=ValueError("bad response"))
        failing.__name__ = "failing"
        passing = Mock(return_value="ready")
        passing.__name__ = "passing"
        self.smoke.check("failure")(failing)()
        self.smoke.check("success")(passing)()
        self.assertEqual(
            self.smoke._results,
            [
                ("failure", False, "ValueError: bad response"),
                ("success", True, "ready"),
            ],
        )

    def test_main_returns_failure_when_a_check_fails(self):
        """Run all supplied checks and return a nonzero summary status on failure."""
        passing = Mock(return_value="ready")
        passing.__name__ = "passing"
        failing = Mock(side_effect=ValueError("bad response"))
        failing.__name__ = "failing"
        checks = [self.smoke.check("first")(failing), self.smoke.check("second")(passing)]
        with patch.object(self.smoke, "CHECKS", checks):
            self.assertEqual(self.smoke.main(), 1)
        self.assertIn("1/2 checks passed", self.output.getvalue())
        passing.assert_called_once_with()

    def test_main_returns_success_when_all_checks_pass(self):
        """Return zero and print the check summary when no result failed."""
        passing = Mock(return_value="ready")
        passing.__name__ = "passing"
        with patch.object(self.smoke, "CHECKS", [self.smoke.check("only")(passing)]):
            self.assertEqual(self.smoke.main(), 0)
        self.assertIn("1/1 checks passed", self.output.getvalue())

    def test_sql_fetches_rows_and_closes_cursor(self):
        """Return Trino rows and close the cursor after successful retrieval."""
        conn = Mock()
        conn.cursor.return_value.fetchall.return_value = [[1]]
        self.assertEqual(self.smoke.sql(conn, "SELECT 1"), [[1]])
        conn.cursor.return_value.execute.assert_called_once_with("SELECT 1")
        conn.cursor.return_value.close.assert_called_once_with()

    def test_sql_closes_cursor_on_execution_error(self):
        """Release the cursor even when Trino rejects the statement."""
        conn = Mock()
        conn.cursor.return_value.execute.side_effect = RuntimeError("bad SQL")
        with self.assertRaisesRegex(RuntimeError, "bad SQL"):
            self.smoke.sql(conn, "invalid")
        conn.cursor.return_value.close.assert_called_once_with()

    def test_s3_client_uses_path_addressing(self):
        """Send requests to the explicit endpoint without bucket-specific DNS."""
        self.smoke.s3_client()
        config = self.dependencies["botocore.config"].Config
        config.assert_called_once_with(
            s3={"addressing_style": "path"}, signature_version="s3v4"
        )
        self.dependencies["boto3"].client.assert_called_once_with(
            "s3",
            endpoint_url="http://storage.invalid:8333",
            aws_access_key_id="unit-test-key",
            aws_secret_access_key="unit-test-secret",
            region_name="us-east-1",
            config=config.return_value,
        )

    def test_postgres_check_closes_connection_and_reads_catalog(self):
        """Confirm the warehouse catalog lookup uses a value parameter."""
        pg = self.dependencies["psycopg2"]
        conn = pg.connect.return_value
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [("PostgreSQL test, build details",), (1,)]
        self.smoke.check_postgres_query()
        cursor.execute.assert_has_calls(
            [
                call("SELECT version()"),
                call(
                    "SELECT count(*) FROM pg_database WHERE datname = %s",
                    ("warehouse",),
                ),
            ]
        )
        conn.close.assert_called_once_with()
        self.assertEqual(self.smoke._results, [("postgres_query", True, "PostgreSQL test")])

    def test_purge_relists_after_deleting_each_page(self):
        """Delete smaller chunks and start a new listing after each page is gone."""
        client = Mock()
        client.list_objects_v2.side_effect = [
            {"Contents": [{"Key": f"table/{i}"} for i in range(3)], "IsTruncated": True},
            {"Contents": [{"Key": "table/3"}]},
            {},
        ]
        deleted = self.modules["purge"].purge(client, "lakehouse", "table/", batch_size=2)
        self.assertEqual(deleted, 4)
        self.assertEqual(client.list_objects_v2.call_count, 3)
        client.list_objects_v2.assert_has_calls(
            [call(Bucket="lakehouse", Prefix="table/")] * 3
        )
        self.assertEqual(
            client.delete_objects.call_args_list,
            [
                call(
                    Bucket="lakehouse",
                    Delete={"Objects": [{"Key": "table/0"}, {"Key": "table/1"}], "Quiet": True},
                ),
                call(
                    Bucket="lakehouse",
                    Delete={"Objects": [{"Key": "table/2"}], "Quiet": True},
                ),
                call(
                    Bucket="lakehouse",
                    Delete={"Objects": [{"Key": "table/3"}], "Quiet": True},
                ),
            ],
        )

    def test_purge_main_rejects_whole_bucket_targets(self):
        """Prevent both a bare bucket and a bucket with an empty prefix from deletion."""
        purge = self.modules["purge"]
        for target in ("lakehouse", "lakehouse/"):
            with self.subTest(target=target), patch.object(purge, "s3_client"), patch.object(
                purge, "purge"
            ) as delete, patch("sys.stderr", new=io.StringIO()):
                self.assertEqual(purge.main([target]), 2)
                delete.assert_not_called()

    def test_purge_main_returns_failure_on_client_error(self):
        """Report a failed target without processing later targets."""
        purge = self.modules["purge"]
        with patch.object(purge, "s3_client"), patch.object(
            purge, "purge", side_effect=ClientError("denied")
        ) as delete, patch("sys.stderr", new=io.StringIO()):
            self.assertEqual(purge.main(["lakehouse/a/", "lakehouse/b/"]), 1)
        delete.assert_called_once()

    def test_listing_prints_only_200_objects_and_page_warning(self):
        """Keep the listing bounded and warn when S3 reports another page."""
        listing = self.modules["ls"]
        client = Mock()
        client.list_objects_v2.return_value = {
            "Contents": [{"Key": f"table/item-{i}", "Size": i} for i in range(201)],
            "IsTruncated": True,
        }
        with patch.object(listing, "s3_client", return_value=client):
            self.assertEqual(listing.main(["lakehouse", "table/"]), 0)
        output = self.output.getvalue()
        self.assertIn("table/item-199  199 bytes", output)
        self.assertNotIn("table/item-200", output)
        self.assertIn("more pages remain", output)
        client.list_objects_v2.assert_called_once_with(Bucket="lakehouse", Prefix="table/")

    def test_listing_reports_empty_page(self):
        """Display an explicit empty result when S3 returns no Contents field."""
        listing = self.modules["ls"]
        client = Mock()
        client.list_objects_v2.return_value = {}
        with patch.object(listing, "s3_client", return_value=client):
            self.assertEqual(listing.main(["lakehouse", "table/"]), 0)
        self.assertIn("(no objects)", self.output.getvalue())

    def test_sql_file_run_strips_final_terminators_and_closes_connection(self):
        """Keep embedded semicolons while removing final statement terminators."""
        trino = self.dependencies["trino"]
        conn = trino.dbapi.connect.return_value
        cursor = conn.cursor.return_value
        cursor.description = [("value",)]
        cursor.fetchall.return_value = [["a;b"]]
        result = self.modules["sql_file"].run("SELECT 'a;b';;  ")
        self.assertEqual(result, (["value"], [["a;b"]]))
        cursor.execute.assert_called_once_with("SELECT 'a;b'")
        conn.close.assert_called_once_with()

    def test_sql_file_run_closes_connection_on_error(self):
        """Close a file-query connection when statement execution fails."""
        conn = self.dependencies["trino"].dbapi.connect.return_value
        conn.cursor.return_value.execute.side_effect = RuntimeError("bad SQL")
        with self.assertRaisesRegex(RuntimeError, "bad SQL"):
            self.modules["sql_file"].run("invalid")
        conn.close.assert_called_once_with()

    def test_sql_file_main_formats_null_without_hiding_zero_or_false(self):
        """Read the supplied file and print NULL as an empty field."""
        sql_file = self.modules["sql_file"]
        with patch.object(sql_file.Path, "is_file", return_value=True), patch.object(
            sql_file.Path, "read_text", return_value="SELECT values"
        ), patch.object(
            sql_file, "run", return_value=(["a", "b", "c"], [[None, 0, False]])
        ) as run:
            self.assertEqual(sql_file.main(["dummy.sql"]), 0)
        run.assert_called_once_with("SELECT values")
        self.assertIn("a | b | c\n | 0 | False\n(1 rows)", self.output.getvalue())

    def test_sql_file_main_rejects_missing_file_without_running_sql(self):
        """Return a usage status when the supplied path is not a file."""
        sql_file = self.modules["sql_file"]
        with patch.object(sql_file.Path, "is_file", return_value=False), patch.object(
            sql_file, "run"
        ) as run:
            self.assertEqual(sql_file.main(["dummy.sql"]), 2)
        run.assert_not_called()

    def test_trino_query_prefers_arguments_over_environment(self):
        """Execute explicit command arguments instead of the environment fallback."""
        query = self.modules["trino_query"]
        conn = self.dependencies["trino"].dbapi.connect.return_value
        cursor = conn.cursor.return_value
        cursor.description = [("value",)]
        cursor.fetchall.return_value = [[1]]
        with patch.dict(os.environ, {"TRINO_SQL": "SELECT 2"}):
            self.assertEqual(query.main(["SELECT", "1"]), 0)
        cursor.execute.assert_called_once_with("SELECT 1")
        conn.close.assert_called_once_with()
        self.assertIn("value\n1\n(1 rows)", self.output.getvalue())

    def test_trino_query_reads_environment_without_arguments(self):
        """Use TRINO_SQL only when the command has no statement arguments."""
        conn = self.dependencies["trino"].dbapi.connect.return_value
        cursor = conn.cursor.return_value
        cursor.description = None
        cursor.fetchall.return_value = []
        with patch.dict(os.environ, {"TRINO_SQL": "SELECT 2"}):
            self.assertEqual(self.modules["trino_query"].main([]), 0)
        cursor.execute.assert_called_once_with("SELECT 2")
        self.assertEqual(self.output.getvalue(), "(0 rows)\n")

    def test_trino_query_rejects_blank_arguments_without_environment_fallback(self):
        """Treat an explicit blank statement as invalid even if TRINO_SQL is set."""
        with patch.dict(os.environ, {"TRINO_SQL": "SELECT 2"}):
            self.assertEqual(self.modules["trino_query"].main([" "]), 2)
        self.dependencies["trino"].dbapi.connect.assert_not_called()

    def test_trino_query_closes_connection_on_error(self):
        """Release a direct-query connection when statement execution fails."""
        conn = self.dependencies["trino"].dbapi.connect.return_value
        conn.cursor.return_value.execute.side_effect = RuntimeError("bad SQL")
        with self.assertRaisesRegex(RuntimeError, "bad SQL"):
            self.modules["trino_query"].main(["invalid"])
        conn.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
