
import subprocess
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run_command(command):
    """Run a command and return its output; raise an error if it fails."""
    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )

    if result.returncode != 0:
        raise AssertionError(
            f"Command failed: {' '.join(command)}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    return result.stdout


class PipelineSmokeTests(unittest.TestCase):

    def test_required_project_files_exist(self):
        required_files = [
            "airflow/dags/user-automation.py",
            "spark/spark_stream.py",
            "sql/cassandra_schema.cql",
        ]

        for relative_path in required_files:
            with self.subTest(file=relative_path):
                self.assertTrue(
                    (PROJECT_ROOT / relative_path).is_file(),
                    f"Missing required file: {relative_path}",
                )

    def test_docker_compose_configuration(self):
        compose_files = [
            "docker-compose.yml",
            "docker-compose.yaml",
            "compose.yml",
            "compose.yaml",
        ]

        self.assertTrue(
            any((PROJECT_ROOT / name).is_file() for name in compose_files),
            "No Docker Compose configuration file found.",
        )

        run_command(["docker", "compose", "config", "--quiet"])

    def test_kafka_is_accessible(self):
        output = run_command([
            "docker", "compose", "exec", "-T", "kafka",
            "kafka-topics",
            "--bootstrap-server", "localhost:9092",
            "--list",
        ])

        self.assertIn(
            "users_created",
            output.splitlines(),
            "Kafka topic 'users_created' was not found.",
        )

    def test_cassandra_table_exists(self):
        output = run_command([
            "docker", "compose", "exec", "-T", "cassandra",
            "cqlsh", "-e",
            "SELECT table_name FROM system_schema.tables "
            "WHERE keyspace_name='spark_streams';",
        ])

        self.assertIn(
            "created_users",
            output,
            "Cassandra table 'spark_streams.created_users' was not found.",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
