# Real-Time Data Streaming Pipeline

A hands-on data engineering project demonstrating API ingestion, event streaming, stream processing, and NoSQL storage. The workflow uses Apache Airflow to orchestrate ingestion, Apache Kafka to transport records, Apache Spark Structured Streaming to process them, and Apache Cassandra to store the results.

> **Note:** These instructions describe a fresh local installation. Review the Compose configuration before starting it on any machine that already has Kafka, Cassandra, Spark, or Airflow containers. Do not point a fresh setup at another installation's data volumes without a deliberate migration.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Technology Stack](#technology-stack)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Run Airflow](#run-airflow)
- [Run Spark Streaming](#run-spark-streaming)
- [Verify the Pipeline](#verify-the-pipeline)
- [Configure for Another System](#configure-for-another-system)
- [Troubleshooting](#troubleshooting)
- [Stop the Project](#stop-the-project)
- [Data and Safety Notes](#data-and-safety-notes)
- [Future Improvements](#future-improvements)

## Overview

The project retrieves sample user records from the [Random User API](https://randomuser.me/), publishes them to Kafka, processes the stream with Spark, and stores the resulting records in Cassandra.

It is intended as a learning and portfolio project for data engineering concepts: orchestration, event streaming, structured stream processing, NoSQL databases, Docker, and pipeline troubleshooting.

## Architecture

```text
Random User API
      |
      v
Apache Airflow DAG
      |
      v
Apache Kafka topic: users_created
      |
      v
Apache Spark Structured Streaming
      |
      v
Apache Cassandra
keyspace: spark_streams
table: created_users
```

Supporting services can include ZooKeeper, Schema Registry, and Confluent Control Center, depending on the Compose configuration. Schema Registry is not required when the producer sends plain JSON and does not use Schema Registry serialization.

## Features

- Retrieves sample user data from an external API.
- Uses an Airflow DAG to orchestrate ingestion.
- Streams messages through the Kafka topic `users_created`.
- Processes JSON records with Spark Structured Streaming.
- Stores processed records in Cassandra.
- Uses Docker containers to simplify local service deployment.
- Includes PowerShell scripts for common start, stop, and infrastructure-check operations.
- Separates pipeline code, database schema, configuration examples, and tests.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Ingestion and processing code |
| Apache Airflow | Workflow orchestration |
| Apache Kafka | Message streaming |
| Apache Spark Structured Streaming | Stream processing |
| Apache Cassandra | NoSQL storage |
| Docker / Docker Compose | Local service deployment |
| PowerShell | Windows automation |
| Random User API | Sample source data |

## Repository Structure

```text
Realtime Data Streaming/
├── airflow/
│   └── dags/
│       └── user-automation.py
├── config/
│   └── .env.example
├── scripts/
│   ├── start.ps1
│   ├── stop.ps1
│   └── test.ps1
├── spark/
│   └── spark_stream.py
├── sql/
│   └── cassandra_schema.cql
├── tests/
│   └── test_pipeline.py
├── docs/
│   
├── docker-compose.yml
├── .gitignore
└── README.md
```

The `docs` folders can contain diagrams, screenshots, and tutorials as you add them. Do not commit Python cache files, credentials, local logs, or generated Spark checkpoint data.

## Prerequisites

For a fresh setup, install:

1. Docker Desktop with Docker Compose support.
2. Git to clone the repository.
3. Python 3 if you want to run the Python test runner.
4. Enough memory and disk space for Kafka, Spark, Cassandra, and Airflow if running everything locally.

Airflow may be managed by a separate Compose project. If it is, start it separately and ensure its DAG can resolve and reach Kafka. A hostname such as `kafka:9092` is only resolvable from containers attached to a Docker network that includes the Kafka service.

## Quick Start

These instructions are for a **fresh installation**.

### 1. Clone the repository

```powershell
git clone <YOUR_GITHUB_REPOSITORY_URL>
Set-Location "Realtime Data Streaming"
```

Replace `<YOUR_GITHUB_REPOSITORY_URL>` with your repository URL.

### 2. Review the configuration

Inspect `docker-compose.yml` before starting anything. Check service names, ports, networks, volume definitions, and environment variables. Make sure the host ports are available.

If you want a local environment file:

```powershell
Copy-Item .\config\.env.example .\.env
```

Only change values that the Compose file or application code actually reads. Creating `.env` does not automatically configure Python code that does not load those variables.

### 3. Validate Compose

```powershell
docker compose config --quiet
```

Fix any YAML or environment errors before proceeding.

### 4. Start the services

```powershell
docker compose up -d
docker compose ps
```

Wait until Kafka and Cassandra are ready before running the DAG or Spark streaming job. `depends_on` may control startup order without waiting for a service to be ready.

> **Existing installation warning:** If containers with names such as `kafka`, `cassandra`, or `spark-master` already exist, do not blindly start this Compose project. Compose may create another set of services and new named volumes. Review the existing volume and network configuration first.

## Run Airflow

The DAG file is `airflow/dags/user-automation.py`.

Make it available in the `dags` directory mounted by your Airflow deployment. If Airflow is managed by a separate Compose project, copy the DAG into that project's DAG directory or configure a mount to this repository.

Open the Airflow UI using the host port defined by your Airflow Compose file. Find the DAG and trigger it, or enable its schedule if one is configured.

The DAG is expected to publish records to `users_created`. Its Kafka bootstrap address must be reachable from the Airflow container. For example, `kafka:9092` works only if the container shares a Docker network with Kafka and the broker advertises a reachable address.

If the DAG fails, inspect its task logs in the Airflow UI.

## Run Spark Streaming

The streaming application is `spark/spark_stream.py`.

For the sample Spark image and service names, a typical submission command is:

```powershell
docker exec -it spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,com.datastax.spark:cassandra-connector_2.12:3.5.1 `
  --conf spark.cassandra.connection.host=cassandra `
  --conf spark.cassandra.connection.port=9042 `
  /opt/spark/work-dir/spark_stream.py
```

This assumes:
- The container is named `spark-master`.
- The local `spark` directory is mounted at `/opt/spark/work-dir`.
- The connector packages match the Spark and Scala versions in use.
- The Spark container can resolve and reach Kafka and Cassandra over its Docker network.

Adjust the command if your image, paths, service names, or versions differ. Keep the Spark Kafka connector, Cassandra connector, Spark version, and Scala version compatible.

## Verify the Pipeline

### Check Compose services

```powershell
docker compose ps
```

If Airflow is managed separately, check its containers from the directory containing its Compose file.

### Check Kafka topic

```powershell
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic users_created
```

This assumes the Kafka container is named `kafka`.

### Check Cassandra

List keyspaces:

```powershell
docker exec cassandra cqlsh -e "SELECT keyspace_name FROM system_schema.keyspaces;"
```

Check the destination table:

```powershell
docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM spark_streams.created_users;"
```

These commands assume the container is named `cassandra` and the table is `spark_streams.created_users`.

### Run the Python tests

```powershell
python -m unittest discover -s tests -v
```

The current smoke tests expect the configured Compose service names, topic `users_created`, and table `spark_streams.created_users`. Integration tests require the relevant services to be running. A failure can indicate unavailable services or configuration differences rather than a Python syntax error.

For basic infrastructure checks, run:

```powershell
.\scripts\test.ps1
```

## Configure for Another System

When adapting the project to another machine, review:

- **Ports:** make sure host ports are available; change host-side port mappings if needed.
- **Docker networks:** Airflow, Kafka, Spark, and Cassandra must share suitable networks for service-to-service traffic.
- **Kafka addresses:** use a hostname reachable from the client. Container clients may use `kafka:9092`; host clients may need a separate advertised listener.
- **Service names:** update scripts and commands if Compose service/container names change.
- **Volumes:** fresh installations should use their own volumes. Do not reuse another installation's data without a deliberate migration.
- **Connector versions:** match the connector package versions to the Spark and Scala versions.
- **Memory:** Kafka, Spark, Cassandra, and Airflow can use substantial memory. On low-memory machines, reduce resource usage or avoid running every service simultaneously.
- **Environment variables:** `.env.example` is a template. Confirm that Compose and application code actually consume each variable before relying on it.
- **Airflow deployment:** if Airflow is separate, configure its DAG mount and Kafka network access for that environment.

Never commit credentials, tokens, passwords, or a real `.env` file.

## Troubleshooting

### Kafka connection timeout or broker unavailable

Possible causes include Kafka still starting, an incorrect bootstrap address, an unreachable advertised listener, or Airflow/Spark not being connected to the correct Docker network.

```powershell
docker ps -a
docker logs --tail 100 kafka
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Check the actual container name and the bootstrap address used by each client.

### Schema Registry or Control Center exits

These services depend on Kafka. Inspect their logs and confirm Kafka is healthy before restarting dependent services.

```powershell
docker logs --tail 100 schema-registry
docker logs --tail 100 control-center
docker logs --tail 100 kafka
```

### Cassandra is unavailable

Cassandra may still be initializing, have insufficient memory, or have a storage/configuration issue.

```powershell
docker ps -a
docker logs --tail 100 cassandra
```

Do not delete the Cassandra volume as a troubleshooting shortcut; it may contain stored records.

### Spark cannot find a connector

Confirm the Maven package coordinates match your Spark/Scala versions and that the container can access the dependency repositories. The first submission may take longer while dependencies download.

### Spark runs but Cassandra has no new records

Check in this order:

1. The Airflow DAG succeeded.
2. Kafka topic `users_created` contains records.
3. Spark is consuming the correct topic.
4. Spark logs show no JSON parsing, connector, or Cassandra errors.
5. The checkpoint directory is writable.
6. The keyspace and table names match the code and schema.

Spark checkpoints track streaming progress. Deleting or changing a checkpoint can cause records to be reprocessed or a query to restart from a different position. Understand the consequences before changing it.

### Compose cannot find its configuration file

Run Compose from the directory containing `docker-compose.yml`, or specify the file with `-f`.

```powershell
Set-Location "path\to\Realtime Data Streaming"
docker compose ps
```

### PowerShell script errors

Run the saved `.ps1` file instead of pasting its contents into the terminal:

```powershell
.\scripts\test.ps1
```

Python code belongs in `.py` files and should be run with Python.

### Python tests fail

```powershell
python --version
python -m unittest discover -s tests -v
```

Read the first failing test and check whether its required files and services are available before changing application code.

## Stop the Project

For the Compose project in this repository:

```powershell
.\scripts\stop.ps1
```

Or run:

```powershell
docker compose stop
```

This stops the services without removing containers or named volumes. `docker compose down` is a separate action that removes Compose-managed containers and networks. Named volumes are normally retained unless explicitly removed. Avoid `docker compose down -v` unless you intentionally want to delete the Compose-managed volumes.

If Airflow uses a separate Compose project, manage it from its own directory.

## Data and Safety Notes

- Random User API provides sample user data; do not treat it as real customer data.
- Do not commit `.env`, credentials, generated logs, Python caches, or local Spark checkpoints.
- Back up Cassandra data before migrations or destructive Docker operations.
- Keep local development configuration separate from the reusable repository.
- This is an educational/local demonstration. Production deployment requires additional security, observability, access controls, fault tolerance, and operational planning.

## Future Improvements

- Add documented readiness and health checks for services.
- Move hard-coded connection settings into environment-driven configuration.
- Add unit tests for record parsing and validation.
- Add an end-to-end test that confirms a record travels from Kafka through Spark into Cassandra.
- Add architecture diagrams and screenshots under `docs/`.
- Add CI checks for Python syntax, tests, and Compose configuration.
- Add monitoring, data-quality checks, retries, and dead-letter handling.

## License

Add a `LICENSE` file before publishing if you want to specify the terms under which others may use, modify, and distribute the project. Choose a license that matches your intentions.
