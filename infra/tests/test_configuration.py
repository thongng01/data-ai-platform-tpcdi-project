"""Check Compose names and client wiring without reading secrets or starting services."""

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRATCH = ROOT / "local/tmp/overnight-comments/configuration"
COMPOSE_FILES = (
    "compose.yaml",
    "compose.compute.yaml",
    "compose.streaming.yaml",
    "compose.orchestration.yaml",
    "compose.governance.yaml",
)


@unittest.skipUnless(shutil.which("docker"), "Docker CLI is needed to render Compose")
class ComposeConfigurationTests(unittest.TestCase):
    """Render only tracked templates in an isolated directory with fake credentials."""

    @classmethod
    def setUpClass(cls):
        """Check the Compose plugin without contacting the Docker daemon."""
        result = subprocess.run(
            ["docker", "compose", "version"], capture_output=True, text=True
        )
        if result.returncode:
            raise unittest.SkipTest("Docker Compose plugin is not available")

    def setUp(self):
        """Copy public Compose inputs and create an isolated placeholder environment."""
        SCRATCH.mkdir(parents=True, exist_ok=True)
        self.scratch = tempfile.TemporaryDirectory(dir=SCRATCH)
        self.addCleanup(self.scratch.cleanup)
        self.infra = Path(self.scratch.name) / "infra"
        self.infra.mkdir()
        for name in COMPOSE_FILES:
            shutil.copyfile(ROOT / "infra" / name, self.infra / name)

        # Explicit --env-file plus the temporary working directory prevents Compose
        # from loading the owner's private infra/.env or mounted credential files.
        env_text = (ROOT / "infra/.env.example").read_text(encoding="utf-8")
        env_text = env_text.replace(
            "AIRFLOW_WEBSERVER_SECRET_KEY=\n", "AIRFLOW_WEBSERVER_SECRET_KEY=test-only\n"
        ).replace("DATAHUB_SECRET=\n", "DATAHUB_SECRET=test-only\n")
        (self.infra / ".env").write_text(env_text, encoding="utf-8")

    def render(self, *profiles):
        """Return Compose's normalized JSON for selected profiles without running it."""
        # Ignore ambient Compose and service variables so a developer's shell
        # cannot override placeholders or select a different project or daemon.
        env = {key: value for key, value in os.environ.items() if key in {"PATH", "HOME"}}
        command = ["docker", "compose", "--env-file", ".env"]
        for profile in profiles:
            command.extend(["--profile", profile])
        command.extend(["config", "--format", "json"])
        result = subprocess.run(
            command, cwd=self.infra, env=env, capture_output=True, text=True
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_supported_profile_combinations_render(self):
        """Accept core and supported profiles, including dbt with its compute dependency."""
        for profiles in (
            (), ("compute",), ("compute", "dbt"), ("stream",), ("airflow",),
            ("governance",), ("init",), ("tools",), ("stream", "governance"), ("*",),
        ):
            with self.subTest(profiles=profiles):
                self.render(*profiles)

    def test_resources_use_project_scope(self):
        """Keep generated resource names and existing logical data-volume keys."""
        config = self.render("*")
        self.assertEqual(config["name"], "brokerage")
        self.assertEqual(set(config["networks"]), {"platform"})
        self.assertEqual(config["networks"]["platform"]["name"], "brokerage_platform")
        self.assertEqual(
            set(config["volumes"]),
            {"postgres-data", "seaweedfs-data", "kafka-data", "airflow-logs", "opensearch-data"},
        )
        for role, service in config["services"].items():
            with self.subTest(role=role):
                self.assertNotIn("container_name", service)
                self.assertEqual(set(service["networks"]), {"platform"})
                if "build" in service or role.startswith("spark-"):
                    self.assertTrue(service["image"].startswith("brokerage/"))

    def test_internal_clients_use_service_dns_and_container_ports(self):
        """Preserve role DNS and distinguish internal ports from published host ports."""
        config = self.render("*")
        services = config["services"]
        self.assertIn("postgres", services)
        self.assertIn("seaweedfs", services)
        self.assertIn("hive-metastore", services)
        self.assertIn("kafka", services)
        self.assertIn("jobmanager", services)
        self.assertEqual(services["bootstrap"]["environment"]["PGHOST"], "postgres")
        self.assertEqual(services["tools"]["environment"]["TRINO_HOST"], "trino")
        self.assertEqual(services["tools"]["environment"]["TRINO_PORT"], "8080")
        self.assertIn(
            "PLAINTEXT://kafka:29092",
            services["kafka"]["environment"]["KAFKA_ADVERTISED_LISTENERS"],
        )
        self.assertIn(
            "jobmanager.rpc.address: jobmanager",
            services["taskmanager"]["environment"]["FLINK_PROPERTIES"],
        )
        self.assertIn("datahub-gms", services["datahub-frontend"]["depends_on"])

    def test_host_port_override_does_not_change_internal_s3_endpoint(self):
        """Keep container S3 connections stable when a host port changes."""
        env_path = self.infra / ".env"
        env_path.write_text(
            env_path.read_text(encoding="utf-8").replace(
                "SEAWEEDFS_S3_PORT=8333", "SEAWEEDFS_S3_PORT=18333"
            ),
            encoding="utf-8",
        )
        services = self.render("*")["services"]
        self.assertEqual(services["seaweedfs"]["ports"][0]["published"], "18333")
        self.assertEqual(services["seaweedfs"]["ports"][0]["target"], 8333)
        self.assertIn("-s3.port=8333", services["seaweedfs"]["command"])
        for role, service in services.items():
            environment = service.get("environment", {})
            if "S3_ENDPOINT" in environment:
                with self.subTest(role=role):
                    self.assertEqual(environment["S3_ENDPOINT"], "http://seaweedfs:8333")

    def test_private_environment_retains_project_and_image_selection(self):
        """Honor explicit local scope and tags without forcing the example defaults."""
        env_path = self.infra / ".env"
        env_path.write_text(
            env_path.read_text(encoding="utf-8").replace(
                "COMPOSE_PROJECT_NAME=brokerage", "COMPOSE_PROJECT_NAME=existing"
            ).replace("SPARK_IMAGE=brokerage/spark:3.5.3", "SPARK_IMAGE=existing/spark:3.5.3"),
            encoding="utf-8",
        )
        config = self.render("compute")
        self.assertEqual(config["name"], "existing")
        self.assertEqual(config["networks"]["platform"]["name"], "existing_platform")
        self.assertEqual(config["services"]["spark-master"]["image"], "existing/spark:3.5.3")
        self.assertEqual(config["volumes"]["postgres-data"]["name"], "existing_postgres-data")

    def test_airflow_connection_uses_internal_gms_port(self):
        """Point the Airflow HTTP client at the same GMS port as the frontend."""
        # The template's final connection is small enough to inspect as text;
        # this avoids adding a YAML dependency just for a static port check.
        connections = (ROOT / "data/airflow/connections.yaml.example").read_text(
            encoding="utf-8"
        )
        gms = connections.split("\ndatahub_gms:\n", 1)[1]
        self.assertIn("host: datahub-gms\n  port: 8080", gms)
        services = self.render("*")["services"]
        self.assertEqual(services["datahub-gms"]["ports"][0]["target"], 8080)
        self.assertEqual(
            services["datahub-frontend"]["environment"]["DATAHUB_GMS_PORT"], "8080"
        )


if __name__ == "__main__":
    unittest.main()
