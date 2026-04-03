# EXPLANATION.md — Telemetry AI Data Platform Knowledge Base

> **What is this file?**
> This is your personal, complete guide to every single thing built in this project. After each phase, Claude updates this file with detailed explanations of what was done, what every technology means, how the code works, and what you should now understand. By the end of all 7 phases, this file alone should let you explain the entire platform to anyone — without looking at the code.

> **How this file is updated:**
> After every phase, Claude will propose subheadings to add. You confirm them. Then Claude writes the content. Nothing gets added without your approval on the structure first.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Phase 1 — Prerequisites & Environment Setup](#phase-1--prerequisites--environment-setup)
- [Phase 2 — Kafka + Data Ingestion](#phase-2--kafka--data-ingestion)
- [Phase 3 — Spark Processing + Iceberg Storage](#phase-3--spark-processing--iceberg-storage)
- [Phase 4 — Airflow Orchestration](#phase-4--airflow-orchestration)
- [Phase 5 — PyTorch Anomaly Detection](#phase-5--pytorch-anomaly-detection)
- [Phase 6 — FastAPI Backend](#phase-6--fastapi-backend)
- [Phase 7 — React Portal](#phase-7--react-portal)
- [Full System Glossary](#full-system-glossary)
- [How Everything Connects](#how-everything-connects)

---

## Project Overview

### What Is This Platform?

A local, end-to-end data + ML platform that ingests operational telemetry (sensor data, app logs, infrastructure metrics), processes and validates it, stores it in versioned tables, detects anomalies using machine learning, and presents everything through a self-service web portal.

### The Problem It Solves

Operational systems generate massive amounts of raw signals. Without a platform like this:
- Data arrives in chaos — different formats, unreliable, full of errors
- Outages are detected reactively (after users complain) instead of proactively
- Nobody trusts the data because there's no quality enforcement
- ML experiments can't be reproduced because training data changes under you
- Only engineers can access insights — everyone else files tickets and waits

### Tech Stack Summary

| Technology       | Role                          | Layer          |
| ---------------- | ----------------------------- | -------------- |
| Apache Kafka     | Event streaming / ingestion   | Ingestion      |
| Apache Spark     | Data processing / ETL         | Processing     |
| Apache Iceberg   | Versioned table storage       | Storage        |
| Apache Airflow   | Workflow orchestration        | Orchestration  |
| PyTorch          | Anomaly detection ML          | ML Training    |
| Python + FastAPI | Backend API services          | Serving        |
| SQL              | Analytical queries            | Analytics      |
| React            | Web portal UI                 | Frontend       |
| MinIO            | Local S3-compatible storage   | Infrastructure |
| Docker           | Container runtime             | Infrastructure |

### Data Flow (Plain English)

```
Sensors/Apps generate events
    → Kafka catches every event without dropping any
        → Spark reads events, validates them, cleans them
            → Clean data goes to Iceberg tables (versioned, queryable)
            → Bad data goes to a dead-letter table for investigation
                → Airflow schedules and monitors this entire flow
                    → PyTorch trains on clean data to learn "normal" patterns
                        → FastAPI serves results to the React portal
                            → You browse datasets, lineage, ML results in a web UI
```

---

## Phase 1 — Prerequisites & Environment Setup

> ✅ **Status:** Complete

### What Was Done

1. **Homebrew** — already installed, used to install all other tools
2. **Docker Desktop** — already installed (v29.3.1), confirmed running with Docker Compose v5.1.1
3. **Python 3.11.15** — installed via `brew install python@3.11`
4. **Java 17.0.18 (OpenJDK)** — installed via `brew install openjdk@17`, added to PATH and JAVA_HOME in `~/.zshrc`
5. **Node.js v25.8.1** — already installed (exceeds the v20 LTS requirement)
6. **Python virtual environment** — created at `.venv/` using Python 3.11
7. **Core Python packages** — installed all 14+ packages: kafka-python, avro-python3, pyspark 3.5.3, pyiceberg, fastapi, uvicorn, torch, torchvision, pandas, numpy, scikit-learn, mlflow, httpx
8. **PyTorch MPS** — confirmed Apple Silicon GPU acceleration is available
9. **.gitignore** — created to exclude venv, caches, IDE files, ML artifacts, node_modules, Spark/Airflow temp files
10. **Git repo** — initialized, remote set to GitHub, initial commit pushed
11. **GitHub CLI** — authenticated as Charan0622 with repo/workflow scopes

### Step-by-Step Changes

1. Checked existing tools: Docker installed but not running, Python 3.9.6 (system), Node v25.8.1, no Java, Homebrew present
2. Started Docker Desktop (manually via GUI — required for macOS)
3. Verified Docker daemon responded to `docker info`
4. Fixed Homebrew directory permissions (`sudo chown -R` on `/opt/homebrew`)
5. Ran `brew install python@3.11` — installed Python 3.11.15
6. Ran `brew install openjdk@17` — installed OpenJDK 17.0.18
7. Added Java to PATH: `echo 'export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"' >> ~/.zshrc`
8. Set JAVA_HOME: `echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 17)' >> ~/.zshrc`
9. Created venv: `python3.11 -m venv .venv`
10. Activated venv: `source .venv/bin/activate`
11. Upgraded pip: `pip install --upgrade pip`
12. Installed all packages: `pip install kafka-python avro-python3 pyspark==3.5.3 pyiceberg fastapi uvicorn torch torchvision pandas numpy scikit-learn mlflow httpx`
13. Ran all 6 verification checks — all passed
14. Created `.gitignore`
15. Ran `git init`, added remote, committed, pushed to GitHub
16. Authenticated `gh` CLI via `gh auth login`

### Concepts & Definitions

**Homebrew** — A package manager for macOS. Think of it like an app store for developer tools, but you use the terminal instead of clicking buttons. Command: `brew install <package>`. Why we need it: it's the easiest way to install Python, Java, and other tools on a Mac.

**Docker** — A tool that runs lightweight virtual computers called "containers" on your machine. Each container is isolated — it has its own filesystem, network, and processes. Why we need it: Kafka, MinIO, and Airflow will each run inside their own container so they don't mess with your Mac's system. Think of containers like apartments in a building — they share the building (your Mac) but each has its own space.

**Docker Compose** — A tool that lets you define and run multiple Docker containers at once using a single YAML file (`docker-compose.yml`). Instead of starting each container manually, you write a recipe and run `docker compose up`. Why we need it: our platform has 4+ services that need to talk to each other.

**Python Virtual Environment (venv)** — An isolated Python installation inside your project folder. Packages installed in a venv don't affect other projects, and other projects don't affect yours. Created with `python3.11 -m venv .venv`, activated with `source .venv/bin/activate`. The `.venv/` folder contains a copy of the Python interpreter and all installed packages.

**pip** — Python's package installer. `pip install <package>` downloads and installs a library from PyPI (Python Package Index), which is like npm for Python. We used it to install all our data engineering and ML libraries.

**JVM (Java Virtual Machine)** — The runtime that executes Java (and Scala) code. Apache Spark is written in Scala which runs on the JVM. When we use PySpark (Python API for Spark), Python sends instructions to Spark's JVM process. So Java must be installed even though we write zero Java code. Think of it as the engine under Spark's hood.

**JAVA_HOME** — An environment variable that tells other programs where Java is installed. Spark looks for this variable at startup. We set it in `~/.zshrc` so it's available every time you open a terminal.

**MPS (Metal Performance Shaders)** — Apple's GPU framework for M-series chips. PyTorch can use MPS to accelerate neural network training on your M4's GPU instead of running everything on CPU. This is why we run PyTorch natively (not in Docker) — Docker containers can't access the Mac's GPU. We verified it with `torch.backends.mps.is_available()` returning `True`.

**`.zshrc`** — A config file that runs every time you open a new terminal. We added Java's PATH and JAVA_HOME here so they persist across sessions. It lives at `~/.zshrc`.

**.gitignore** — A file that tells git which files/folders to NOT track. We exclude `.venv/` (huge, machine-specific), `__pycache__/` (Python bytecode), `node_modules/` (npm packages), `.env` (secrets), and ML checkpoints (large binary files). These don't belong in version control.

### Architecture Notes

Phase 1 doesn't build any application components — it prepares the foundation everything else sits on:

```
Your Mac (16GB RAM, M4 chip)
├── Docker Desktop (8GB allocated)
│   └── Will host: Kafka, MinIO, Airflow (Phases 2-4)
├── Python 3.11 venv (.venv/)
│   ├── PySpark → needs Java 17 (JVM) → for Phase 3
│   ├── kafka-python → for Phase 2
│   ├── PyTorch + MPS → for Phase 5 (runs natively, NOT in Docker)
│   ├── FastAPI → for Phase 6
│   └── pyiceberg, pandas, numpy, scikit-learn, mlflow → Phases 3-6
├── Node.js → for Phase 7 (React portal)
└── Git + GitHub → version control throughout
```

Key architectural decision: **PyTorch runs natively** while most other services run in Docker. This is because Apple Silicon GPU (MPS) is only accessible from native macOS processes, not from inside Linux containers.

### Key Code Explained

**Creating the virtual environment:**
```bash
python3.11 -m venv .venv
```
- `python3.11` — use specifically Python 3.11 (not the system 3.9)
- `-m venv` — run the `venv` module (built into Python) to create a virtual environment
- `.venv` — name of the directory to create (dot prefix = hidden by default in file browsers)

**Activating the venv:**
```bash
source .venv/bin/activate
```
- `source` — execute the script in the current shell (not a subprocess)
- After activation, `python3` and `pip` point to the venv's copies, not the system ones
- Your terminal prompt shows `(.venv)` to confirm it's active

**Setting JAVA_HOME:**
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```
- `$()` — runs the command inside and substitutes its output
- `/usr/libexec/java_home -v 17` — macOS utility that finds where Java 17 is installed
- `export` — makes the variable available to child processes (like Spark)

**Checking MPS availability:**
```python
import torch
print(torch.backends.mps.is_available())  # True = GPU ready
```
- `torch.backends.mps` — PyTorch's interface to Apple's Metal GPU framework
- Returns `True` only when running natively on Apple Silicon with a compatible PyTorch version

### What Could Go Wrong

| Problem | Cause | Fix |
|---------|-------|-----|
| `brew install` fails with permission errors | `/opt/homebrew` owned by root or another user | `sudo chown -R $(whoami) /opt/homebrew` |
| `python3.11` not found after install | Homebrew didn't link it to PATH | `brew link python@3.11` or use full path `/opt/homebrew/opt/python@3.11/bin/python3.11` |
| `java -version` says "not found" after install | OpenJDK not in PATH | Add to `~/.zshrc`: `export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"` then `source ~/.zshrc` |
| `torch.backends.mps.is_available()` returns `False` | Running inside Docker, or outdated PyTorch | Ensure running natively (not Docker). Run `pip install --upgrade torch` |
| `pip install` fails with "externally managed" | System Python on newer macOS blocks global pip | Always use a venv — never `pip install` outside of one |
| Docker commands fail | Docker Desktop not running | Open Docker Desktop app from Applications |
| `git push` fails with 403 | No GitHub credentials cached | Run `gh auth login` and follow browser flow |

### What I Should Be Able to Explain

- [ ] What is a virtual environment and why every Python project should use one
- [ ] Why we need Java installed for a Python data project (JVM → Spark)
- [ ] What Docker does and why we use containers instead of installing services directly
- [ ] What MPS is and why PyTorch runs natively instead of in Docker
- [ ] What `.gitignore` does and why `.venv/` and `node_modules/` shouldn't be committed
- [ ] The difference between `pip` (Python packages) and `brew` (system tools)
- [ ] What JAVA_HOME is and why Spark needs it

---

## Phase 2 — Kafka + Data Ingestion

> ✅ **Status:** Complete

### What Was Done

1. **`docker-compose.yml`** — Created with Kafka service definition (Apache Kafka 3.9.0, KRaft mode, ARM64). This is the master file that will grow to include all our containerized services.
2. **`ingestion/schemas/telemetry.avsc`** — Avro schema defining the exact structure of a telemetry event: event_id, timestamp, device_id, metric_name (enum of 5 types), value (nullable double), unit, and location.
3. **`ingestion/fake_producer.py`** — Python script that generates fake telemetry events from 50 simulated devices and sends them to Kafka topic `telemetry.raw` every 0.5 seconds. Injects ~5% anomalous events (null values, extreme readings, future timestamps, negative values, unknown metrics).
4. **`ingestion/test_consumer.py`** — Test script that reads events back from Kafka to verify the pipeline works. Reads from the beginning of the topic and identifies anomalous events.

### Step-by-Step Changes

1. Created `docker-compose.yml` with Kafka service — initially tried `bitnami/kafka:3.7` image
2. Attempted to start Kafka — failed because Bitnami images are no longer available on Docker Hub
3. Investigated alternatives — found that `apache/kafka:3.9.0` is the official replacement with ARM64 support
4. Rewrote `docker-compose.yml` with `apache/kafka:3.9.0` — different env var format (`KAFKA_` prefix instead of Bitnami's `KAFKA_CFG_`), added `CLUSTER_ID` (required for KRaft), configured dual listeners (internal on 29092 for future inter-container traffic, external on 9092 for localhost)
5. Ran `docker compose up kafka -d` — image pulled (~200MB), container started successfully
6. Verified Kafka health: container running, using 364MB of 1GB limit
7. Created directory structure: `mkdir -p ingestion/schemas`
8. Wrote `ingestion/schemas/telemetry.avsc` — Avro schema with 7 fields
9. Wrote `ingestion/fake_producer.py` — producer with 50 devices, 5 metric types, 10 locations, 5 anomaly modes
10. Tested producer — ran for 8 seconds, sent 15 events successfully
11. Wrote `ingestion/test_consumer.py` — consumer that reads from topic beginning
12. Tested consumer — read back all 15 events from Kafka, confirming durable storage
13. Ran extended test (30 events) — confirmed anomaly injection works (negative value detected)
14. All 4 Phase 2 verification checks passed

### Concepts & Definitions

**Apache Kafka** — A distributed event streaming platform. Think of it like a **post office with perfect memory**. Producers drop messages into named mailboxes called "topics." Consumers read from those mailboxes. Unlike a regular queue, messages don't disappear after being read — they stick around, so multiple consumers can read the same messages independently, and you can replay old messages. In our project, Kafka is the entry point: all telemetry data flows through it before anything else touches it. Why not just write directly to a database? Because Kafka decouples producers from consumers — the producer doesn't need to know or care who reads the data, and consumers can be added/removed without affecting producers.

**Topic** — A named channel in Kafka where messages are stored. Think of it like a TV channel — producers broadcast to it, consumers tune in. Our topic is `telemetry.raw` — the word "raw" signals this is unprocessed data straight from sensors. Topics are durable (messages persist on disk) and can be partitioned across multiple servers for parallelism (though we only use 1 partition locally).

**Producer** — A program that sends messages to a Kafka topic. Our `fake_producer.py` is a producer. In a real system, producers would be agents running on actual servers, sending real CPU/memory/temperature readings. The producer doesn't wait for consumers — it's "fire and forget."

**Consumer** — A program that reads messages from a Kafka topic. Our `test_consumer.py` is a consumer. Consumers track their position in the topic using an **offset** (a number like "I've read up to message 42"). This means consumers can stop and restart without missing messages.

**Offset** — A sequential number assigned to each message in a topic. Message 0, message 1, message 2, etc. Consumers use offsets as bookmarks. `auto_offset_reset='earliest'` means "start from offset 0" (the very first message). `'latest'` would mean "only read new messages from now on."

**KRaft (Kafka Raft)** — Kafka's built-in consensus protocol that replaced ZooKeeper. Older Kafka needed a separate ZooKeeper service to coordinate which broker is the leader, track topic metadata, etc. KRaft does all of this inside Kafka itself — one less container to run, one less thing to break. "Raft" refers to the Raft consensus algorithm, which is how multiple nodes agree on who's in charge.

**CLUSTER_ID** — A unique identifier for a Kafka cluster in KRaft mode. It's just a 22-character base64 string. Every node in the cluster must share the same CLUSTER_ID. For our single-node dev setup, any valid string works.

**Listeners and Advertised Listeners** — Kafka has a two-step connection process. First, a client connects to any broker. The broker responds with the "advertised listener" addresses — the real addresses clients should use for ongoing communication. We configure two listeners: `PLAINTEXT_HOST` on port 9092 (for Python scripts on localhost) and `PLAINTEXT` on port 29092 (for future Docker containers that talk to Kafka by container name).

**Avro Schema** — A data format specification that defines the exact structure of a message: field names, types, defaults, and documentation. Think of it as a **contract between producer and consumer** — "every telemetry event WILL have these 7 fields in these exact types." Without a schema, producers might silently change the data format and break downstream consumers. Avro supports schema evolution (adding/removing fields safely), which is why it's popular in data pipelines.

**Serialization / Deserialization** — Converting data between formats. Our producer **serializes** Python dicts into JSON bytes before sending to Kafka (because Kafka only transports raw bytes). Our consumer **deserializes** those bytes back into Python dicts. The `value_serializer` and `value_deserializer` parameters in kafka-python handle this automatically.

**Docker Compose `mem_limit`** — A hard cap on how much RAM a container can use. We set Kafka to `1g` (1 gigabyte). If Kafka tries to use more, Docker kills it. This protects our 16GB laptop from a single runaway container eating all the RAM.

**`platform: linux/arm64`** — Tells Docker to pull the ARM64 version of the image, not x86_64. Required on Apple Silicon Macs. Without this, Docker might pull an x86 image and emulate it with Rosetta, which is slower and uses more memory.

### Architecture Notes

Kafka is the **first component in our data pipeline** — the front door where all data enters:

```
Phase 2 architecture:

fake_producer.py (Python on localhost)
    │
    │ sends JSON events every 0.5s
    │ via kafka-python library
    │
    ▼
┌─────────────────────────────┐
│  Kafka (Docker container)    │
│  Topic: telemetry.raw        │
│  Port: 9092 (localhost)      │
│  Port: 29092 (Docker net)    │
│  Memory: ≤1GB               │
└─────────────────────────────┘
    │
    │ reads events back
    │
    ▼
test_consumer.py (Python on localhost)
```

In later phases, this grows:
- **Phase 3:** Spark replaces test_consumer — reads from Kafka, validates, writes to Iceberg
- **Phase 4:** Airflow orchestrates when Spark runs
- **Phase 5:** PyTorch reads from Iceberg tables (not directly from Kafka)

The Avro schema (`telemetry.avsc`) isn't enforced by Kafka itself — it's documentation and a reference for code that serializes/deserializes events. In production systems, you'd use a Schema Registry to enforce schemas at the broker level.

### Key Code Explained

**Producer — connecting to Kafka and serializing data:**
```python
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
```
- `bootstrap_servers` — the address of at least one Kafka broker. The client connects here first, then discovers the full cluster topology. For us it's just `localhost:9092`.
- `value_serializer` — a function that converts each message value before sending. `json.dumps(v)` turns a Python dict into a JSON string, `.encode("utf-8")` turns that string into bytes. Kafka only transports bytes.

**Producer — sending a message:**
```python
producer.send(TOPIC, value=event)
```
- `.send()` is **non-blocking** — it queues the message in an internal buffer and returns immediately. Kafka batches messages for efficiency.
- `producer.flush()` at the end forces all buffered messages to be sent before the program exits.

**Anomaly injection — corrupting data intentionally:**
```python
if random.random() < ANOMALY_RATE:  # 5% chance
    event = create_anomalous_event()
```
- `random.random()` returns a float between 0 and 1. If it's less than 0.05 (5%), we generate a bad event.
- Five anomaly types mirror real-world data quality issues: sensor failures (null), glitches (extreme values), clock drift (future timestamps), hardware errors (negative values), and misconfiguration (unknown metrics).

**Consumer — reading from the beginning:**
```python
consumer = KafkaConsumer(
    TOPIC,
    auto_offset_reset="earliest",
    consumer_timeout_ms=10000,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)
```
- `auto_offset_reset="earliest"` — if this consumer has never read from this topic before, start from the very first message. Without this, it would default to `latest` and only see new messages.
- `consumer_timeout_ms=10000` — if no new messages arrive within 10 seconds, stop iterating. This prevents the consumer from hanging forever.
- `value_deserializer` — the reverse of the producer's serializer: bytes → JSON string → Python dict.

**Docker Compose — dual Kafka listeners:**
```yaml
KAFKA_LISTENERS: PLAINTEXT://:29092,CONTROLLER://:9093,PLAINTEXT_HOST://:9092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
```
- Three listeners, each for a different purpose:
  - `PLAINTEXT_HOST` on 9092 — for our Python scripts running on the Mac (advertised as `localhost:9092`)
  - `PLAINTEXT` on 29092 — for future Docker containers like Spark (advertised as `kafka:29092` — the container hostname)
  - `CONTROLLER` on 9093 — for Kafka's internal KRaft coordination (not advertised to clients)

### What Could Go Wrong

| Problem | Cause | Fix |
|---------|-------|-----|
| `bitnami/kafka:3.7` image not found | Bitnami pulled their images from Docker Hub | Use `apache/kafka:3.9.0` instead (the official Apache image) |
| `kafka-python` can't connect to Kafka | Kafka takes ~10-15 seconds to fully start | Wait 15 seconds after `docker compose up`, or add retry logic |
| Port 9092 already in use | Another service (or old Kafka) is using it | `lsof -i :9092` to find what's using it, then stop it |
| Consumer reads 0 messages | Topic doesn't exist yet (no messages sent) | Run the producer first, then the consumer |
| Consumer hangs forever | `consumer_timeout_ms` not set | Add `consumer_timeout_ms=10000` to stop after 10s of silence |
| Events are garbled/corrupted | Serializer/deserializer mismatch | Ensure producer uses `json.dumps().encode()` and consumer uses `json.loads(.decode())` |
| Docker container uses too much memory | No `mem_limit` set | Always set `mem_limit` in docker-compose.yml |
| Kafka container restarts in a loop | Bad CLUSTER_ID or missing env vars | Check `docker compose logs kafka` for the exact error |

### What I Should Be Able to Explain

- [ ] What Kafka is and why we use it instead of writing directly to a database
- [ ] What a topic is and how it relates to producers and consumers
- [ ] What an offset is and why `auto_offset_reset='earliest'` matters
- [ ] What KRaft is and why it replaced ZooKeeper
- [ ] What serialization means and why Kafka requires it
- [ ] Why we have two listeners (9092 for localhost, 29092 for Docker containers)
- [ ] What an Avro schema is and why data contracts matter in pipelines
- [ ] Why we inject anomalous data on purpose (to test validation in Phase 3)
- [ ] What `mem_limit` does and why it's critical on a 16GB laptop

---

## Phase 3 — Spark Processing + Iceberg Storage

> ✅ **Status:** Complete

### What Was Done

1. **`docker-compose.yml` (updated)** — Added MinIO (S3-compatible object storage) and a one-time `minio-setup` helper that creates the `telemetry-warehouse` bucket.
2. **`storage/catalog.py`** — Reusable module that creates a SparkSession pre-configured for Iceberg + MinIO. Every Spark job imports from here so configuration is centralized. Includes automatic JAVA_HOME detection.
3. **`processing/stream_processor.py`** — Spark Structured Streaming job: reads raw events from Kafka, validates every field against 4 rules, writes valid events to `clean_events` Iceberg table, routes invalid events to `dead_letter` table with rejection reasons.
4. **`processing/batch_etl.py`** — Spark batch job: reads `clean_events`, computes hourly aggregations (avg, min, max, stddev, count) per device per metric, writes to `hourly_aggregates` table.
5. **`processing/quality_checks.py`** — Runs 4 SQL-based data quality checks against `clean_events`: row count threshold, null rates per column, value range validation, and duplicate detection. Returns pass/fail per check.

### Step-by-Step Changes

1. Added MinIO service to `docker-compose.yml` — `minio/minio:latest`, ports 9000 (S3 API) and 9001 (web console), 512MB memory limit
2. Added `minio-setup` service — runs `mc mb local/telemetry-warehouse` once to create the storage bucket, then exits
3. Started MinIO: `docker compose up minio minio-setup -d` — bucket created successfully, MinIO using only 74MB RAM
4. Created `storage/catalog.py` — SparkSession factory with Iceberg catalog config pointing to MinIO
5. Added automatic JAVA_HOME detection in catalog.py (subprocess call to `/usr/libexec/java_home -v 17`) so Spark works even when `~/.zshrc` hasn't been sourced
6. Tested catalog — created a test table, inserted rows, read them back, dropped it. Confirmed Iceberg + MinIO working.
7. Discovered missing `hadoop-aws` JAR — Spark couldn't find `S3AFileSystem` class. Added `org.apache.hadoop:hadoop-aws:3.3.4` to packages.
8. Created `processing/stream_processor.py` — Structured Streaming with `foreachBatch` for validation routing
9. First stream processor test failed: "Failed to find data source: kafka" — Spark needs the Kafka connector JAR
10. Added `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` to catalog.py packages
11. Cleared stale checkpoints: `rm -rf checkpoints/`
12. Ran stream processor for 120 seconds — successfully processed all Kafka events: 80 valid → `clean_events`, 3 rejected → `dead_letter`
13. Queried `dead_letter` table — confirmed rejection reasons: "negative value: -146.17", "unknown metric: unknown_sensor_xyz", "negative value: -279.42"
14. Created `processing/batch_etl.py` — hourly window aggregation job
15. Ran batch ETL — computed 69 aggregation rows from 80 events, written to `hourly_aggregates`
16. Created `processing/quality_checks.py` — 4 SQL-based quality checks
17. Ran quality checks — all 4 passed (row count, null rates, value ranges, duplicates)
18. Verified Iceberg time-travel: queried `clean_events.snapshots`, then ran `VERSION AS OF` query against snapshot ID — returned all 80 rows from that point in time
19. All 7 Phase 3 verification checks passed

### Concepts & Definitions

**Apache Spark** — A distributed computation engine that processes data in parallel across CPU cores. Even on one laptop, it splits work across all available cores for speed. Think of it like hiring 8 workers (your 8 CPU cores) to sort mail instead of one person doing it alone. We use Spark to read raw events from Kafka, validate them, and write clean data to Iceberg tables. Spark is written in Scala (runs on the JVM) but we use **PySpark** — the Python API that sends instructions to Spark's JVM engine.

**PySpark** — Python's interface to Apache Spark. You write Python code, PySpark translates it into operations that run on Spark's JVM engine. This is why we need Java installed — when you run `python stream_processor.py`, PySpark starts a Java process in the background.

**Structured Streaming** — Spark's way of processing data that arrives continuously. Instead of processing all data at once (batch), it processes small chunks as they arrive from Kafka. You write the same code as batch processing — `SELECT`, `WHERE`, `GROUP BY` — but Spark runs it repeatedly on new data. Think of it like a conveyor belt at an airport: bags keep arriving, and the scanner (Spark) checks each batch as it comes.

**Micro-batch** — The small chunk of data Spark processes at a time in Structured Streaming. We configured `trigger(processingTime="10 seconds")`, meaning Spark reads all new Kafka messages every 10 seconds, processes them as a batch, then waits for the next 10 seconds. The trade-off: smaller intervals = lower latency but more overhead; larger intervals = higher efficiency but more delay.

**foreachBatch** — A Structured Streaming output mode where Spark calls YOUR function for each micro-batch. We use it because we need custom logic: validate each row, then split into two different tables (clean vs. dead letter). The alternative, built-in sinks, can only write to one destination.

**Apache Iceberg** — A table format that makes regular files (Parquet) behave like a proper database table. It adds: (1) **Transactions** — writes either fully succeed or fully fail, no half-written data. (2) **Schema enforcement** — data must match the table structure. (3) **Snapshots** — every write creates a new version. You can query any past version (time travel). (4) **Partition evolution** — change how data is organized without rewriting everything. Think of Iceberg as the "table of contents" and "version history" for your data files.

**Snapshot** — An Iceberg concept. Every time you write data to a table, Iceberg creates a snapshot — a point-in-time record of what the table looked like. Each snapshot has a unique ID and timestamp. You can query any past snapshot using `VERSION AS OF <snapshot_id>`. This is called **time travel**. It's powerful for reproducibility: "Train my ML model on the data as it existed at 2pm yesterday."

**Time Travel** — The ability to query a table as it existed at a specific point in time. Iceberg makes this possible by keeping all snapshots and their associated data files. `SELECT * FROM clean_events VERSION AS OF 6664508234308964044` returns the table exactly as it was when that snapshot was committed. Old data files are never deleted until you explicitly run garbage collection.

**Parquet** — A columnar file format optimized for analytics. Instead of storing data row-by-row (like CSV), it stores data column-by-column. This means if you query `SELECT avg(temperature) FROM events`, Spark only reads the temperature column — not every column in the table. This makes analytical queries 10-100x faster. Iceberg stores all data as Parquet files under the hood.

**MinIO** — An S3-compatible object storage server that runs locally. Amazon S3 is THE standard for storing data in the cloud. MinIO speaks the exact same API (protocol), so any tool that works with S3 works with MinIO — including Spark and Iceberg. We use it as our "data lake" — the central place where all Iceberg table data (Parquet files) is physically stored. You can browse files in the MinIO web console at http://localhost:9001 (login: minioadmin/minioadmin).

**S3A** — The Hadoop filesystem connector for S3-compatible storage. When Spark writes to `s3a://telemetry-warehouse/...`, Hadoop's S3A connector translates that into HTTP requests to MinIO's S3 API. The `hadoop-aws` JAR provides this connector. Without it, Spark throws "Class org.apache.hadoop.fs.s3a.S3AFileSystem not found."

**Object Storage / Bucket** — Object storage organizes data into flat "buckets" (top-level containers) containing "objects" (files). Unlike a filesystem with nested folders, object storage uses flat keys that look like paths: `telemetry-warehouse/telemetry_db/clean_events/data/00001.parquet`. Our bucket is `telemetry-warehouse`, and Iceberg organizes the data files inside it.

**Dead Letter Pattern** — When a data record fails validation, instead of silently dropping it (losing evidence), you route it to a separate "dead letter" table with a reason explaining why it was rejected. Think of the post office returning undeliverable mail stamped "wrong address" instead of throwing it away. Our `dead_letter` table stores the original JSON and the rejection reason (e.g., "negative value: -146.17"). This lets you investigate data quality issues, fix the source, and potentially reprocess the records.

**Checkpointing** — Structured Streaming saves its progress to disk — specifically, which Kafka offsets it has already processed. If Spark crashes and restarts, it reads the checkpoint and picks up exactly where it left off. No duplicates, no missed messages. This is called **exactly-once semantics**. Checkpoints are stored in the `checkpoints/` directory. Important: if you change the streaming query structure, you must delete old checkpoints or Spark will error.

**ETL (Extract, Transform, Load)** — The three steps of a data pipeline: Extract data from a source (Kafka), Transform it (validate, clean, aggregate), Load it into a destination (Iceberg tables). Our stream processor does ETL continuously. Our batch job does ETL on a schedule.

**Window Aggregation** — Grouping data by time windows. `F.window(F.col("timestamp"), "1 hour")` creates 1-hour buckets (00:00-01:00, 01:00-02:00, etc.) and groups events into the bucket matching their timestamp. Then we compute aggregate functions (avg, min, max, stddev) within each bucket. This turns millions of raw events into compact hourly summaries.

**Data Quality Checks / Quality Gate** — Automated SQL queries that verify data health: "Are there enough rows? Any unexpected nulls? Values in range? Duplicates?" A quality gate in a pipeline says "don't proceed to the next step unless all checks pass." This prevents bad data from propagating downstream — if the stream processor has a bug, the quality gate catches it before the data reaches ML training or the dashboard.

**SparkSession** — The entry point to all Spark functionality. Creating a SparkSession starts the Spark engine (JVM, executor threads, etc.). All Spark operations — reading data, running SQL, writing tables — go through the SparkSession. We configure it once in `catalog.py` and reuse it in every job.

**Spark Catalog** — A registry that maps table names to their physical storage locations and metadata. When you write `spark.sql("SELECT * FROM telemetry_db.clean_events")`, the catalog resolves `telemetry_db.clean_events` to `s3a://telemetry-warehouse/telemetry_db/clean_events/`. We use Iceberg's Hadoop catalog, which stores this mapping as files in MinIO itself.

### Architecture Notes

Phase 3 adds the core processing layer between Kafka (input) and the data lake (storage):

```
Phase 3 architecture:

fake_producer.py
    │
    │ sends JSON to telemetry.raw
    ▼
┌──────────────┐
│    Kafka     │  (Docker, port 9092)
│  topic: raw  │
└──────┬───────┘
       │
       │ Spark reads via Structured Streaming
       │ (readStream format "kafka")
       ▼
┌──────────────────────────────────────┐
│   stream_processor.py (PySpark)      │
│                                      │
│   Validation Rules:                  │
│   1. timestamp not null/future       │
│   2. device_id in known list         │
│   3. value not null/negative         │
│   4. metric_name in allowed set      │
│                                      │
│   ┌──────────┐    ┌──────────────┐   │
│   │  Valid    │    │   Invalid    │   │
│   └────┬─────┘    └──────┬───────┘   │
└────────┼─────────────────┼───────────┘
         │                 │
         ▼                 ▼
┌──────────────┐  ┌──────────────────┐
│ clean_events │  │   dead_letter    │
│ (Iceberg)    │  │   (Iceberg)      │
│ 80 rows      │  │   3 rows         │
│ + snapshots  │  │   + reasons      │
└──────┬───────┘  └──────────────────┘
       │
       │ batch_etl.py reads clean data
       ▼
┌──────────────────┐     ┌─────────────────┐
│hourly_aggregates │     │ quality_checks  │
│ (Iceberg)        │     │ 4 SQL checks    │
│ 69 rows          │     │ all PASS        │
└──────────────────┘     └─────────────────┘

       All Iceberg tables stored as Parquet files in:
       ┌──────────────────────────────────┐
       │  MinIO (Docker, ports 9000/9001) │
       │  Bucket: telemetry-warehouse     │
       └──────────────────────────────────┘
```

**Key architectural decisions:**
- Spark runs **natively** (not in Docker) to access all CPU cores and avoid Docker memory overhead. The 2GB driver memory limit keeps it laptop-friendly.
- Iceberg uses a **Hadoop catalog** — metadata stored as files in MinIO. No external metastore DB needed.
- The stream processor uses **foreachBatch** (not built-in Iceberg sink) because we need to split valid/invalid records into two different tables.
- **Checkpoints** are stored locally in `checkpoints/` — not in MinIO — for faster access during streaming.

### Key Code Explained

**Stream processor — reading from Kafka as a stream:**
```python
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)
```
- `.readStream` — tells Spark this is a streaming (continuous) source, not a one-time read
- `.format("kafka")` — use the Kafka connector (from `spark-sql-kafka-0-10` JAR)
- `startingOffsets: "earliest"` — on first run, read all existing messages. After that, checkpoints track the position.
- `.load()` returns a streaming DataFrame with columns: key, value, topic, partition, offset, timestamp. Our events are in the `value` column as raw bytes.

**Stream processor — parsing JSON from Kafka bytes:**
```python
parsed = batch_df.select(
    F.from_json(
        F.col("value").cast("string"),
        StructType([...])
    ).alias("data"),
    F.col("value").cast("string").alias("raw_value")
)
```
- `F.col("value").cast("string")` — Kafka sends bytes; cast to string first
- `F.from_json(string, schema)` — parse the JSON string into structured columns using our defined schema
- `.alias("data")` — name the parsed struct so we can access fields like `data.device_id`
- We keep `raw_value` (original JSON) so dead letter entries have the complete original event

**Stream processor — validation routing:**
```python
for row in rows:
    is_valid, reason = validate_event(row)
    if is_valid:
        valid_rows.append({...})
    else:
        invalid_rows.append({...})
```
- We collect the micro-batch to the driver (small enough — just a few seconds of data)
- Each row runs through 4 validation rules
- Valid rows get a `processed_at` timestamp and go to `clean_events`
- Invalid rows get a `rejection_reason` string and go to `dead_letter`
- This split-routing is why we use `foreachBatch` instead of a simple `.writeStream`

**Batch ETL — window aggregation:**
```python
aggregated = (
    clean_events
    .groupBy(
        F.window(F.col("timestamp"), "1 hour").alias("time_window"),
        F.col("device_id"),
        F.col("metric_name"),
    )
    .agg(
        F.round(F.avg("value"), 2).alias("avg_value"),
        F.round(F.min("value"), 2).alias("min_value"),
        ...
    )
)
```
- `F.window(col, "1 hour")` — creates hourly buckets. An event at 14:37 goes into the 14:00-15:00 window.
- `.groupBy(window, device_id, metric_name)` — one aggregation row per (hour, device, metric) combination
- `.agg(F.avg, F.min, F.max, F.stddev, F.count)` — compute all statistics in one pass
- `.withColumn("hour_window", F.col("time_window.start"))` — extract just the window start time for readability

**Quality checks — null rate check:**
```python
SUM(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END) as null_event_id
```
- `CASE WHEN ... IS NULL THEN 1 ELSE 0` — creates a 1/0 flag for each row
- `SUM(...)` counts how many nulls exist in that column
- Dividing by total rows gives the null rate as a percentage
- We check every column independently — one bad column doesn't mask another

**Catalog — centralized Spark configuration:**
```python
.config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
.config("spark.hadoop.fs.s3a.path.style.access", "true")
```
- `fs.s3a.endpoint` — tells Hadoop's S3A connector to talk to MinIO instead of real AWS S3
- `path.style.access` — use `http://localhost:9000/bucket/key` format instead of `http://bucket.localhost:9000/key` (MinIO doesn't support virtual-hosted style)

### What Could Go Wrong

| Problem | Cause | Fix |
|---------|-------|-----|
| "Failed to find data source: kafka" | Missing `spark-sql-kafka-0-10` JAR | Add `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` to `spark.jars.packages` |
| "Class S3AFileSystem not found" | Missing Hadoop AWS connector | Add `org.apache.hadoop:hadoop-aws:3.3.4` to `spark.jars.packages` |
| "Unable to locate Java Runtime" | JAVA_HOME not set in current shell | `export JAVA_HOME=$(/usr/libexec/java_home -v 17)` or let catalog.py auto-detect |
| Spark OOM (out of memory) | Driver memory too high or too many shuffle partitions | Set `spark.driver.memory=2g` and `spark.sql.shuffle.partitions=8` |
| "Bucket does not exist" | `minio-setup` didn't run or failed | Run `docker compose up minio-setup` or manually create via MinIO console |
| Stream processor processes 0 events | No data in Kafka topic, or stale checkpoint | Run `fake_producer.py` first; delete `checkpoints/` directory and restart |
| "Table already exists" error | Previous run created the table | Use `CREATE TABLE IF NOT EXISTS` (already in our code) |
| Py4J errors on shutdown | Spark JVM shutting down during SIGINT | Harmless — the JVM is cleaning up. Data was already written. |
| Batch ETL shows 0 aggregation rows | `clean_events` is empty | Run the stream processor first to populate clean_events |
| MinIO "access denied" | Wrong credentials | Use `minioadmin/minioadmin` — check `MINIO_ROOT_USER` in docker-compose.yml |
| First run is very slow | Spark downloading JARs from Maven Central | Normal — JARs are cached in `~/.ivy2/` after first download |
| Checkpoint error after code change | Old checkpoint incompatible with new query | Delete `checkpoints/` directory and restart stream processor |

### What I Should Be Able to Explain

- [ ] What Spark is and why it's faster than processing data row-by-row in plain Python
- [ ] What Structured Streaming is and how micro-batches work
- [ ] What Iceberg adds on top of regular Parquet files (transactions, snapshots, schema enforcement)
- [ ] What a snapshot is and how time-travel queries work
- [ ] What MinIO is and why we use it instead of the local filesystem
- [ ] What the dead letter pattern is and why dropping bad data silently is dangerous
- [ ] What checkpointing does and why it enables exactly-once processing
- [ ] What ETL means and the difference between streaming ETL and batch ETL
- [ ] What a window aggregation is and why hourly summaries are useful
- [ ] What data quality checks are and why they should gate the pipeline
- [ ] Why Spark needs Java and the Kafka/Hadoop/AWS JARs to function
- [ ] How `catalog.py` centralizes configuration so all Spark jobs stay consistent

---

## Phase 4 — Airflow Orchestration

> ✅ **Status:** Complete

### What Was Done

1. **`docker-compose.yml` (updated)** — Added Airflow service: `apache/airflow:2.9.3-python3.11`, ARM64, port 8080, SQLite backend, SequentialExecutor, 1.5GB memory limit. Command initializes the DB, creates admin user, starts both scheduler and webserver.
2. **`infra/airflow/dags/telemetry_pipeline_dag.py`** — Main pipeline DAG with 8 tasks: check Kafka lag → run stream processor → run quality checks → quality gate (branch) → batch aggregation → update metadata → notify success. Failure path: quality gate → alert. Scheduled every 15 minutes.
3. **`infra/airflow/dags/ml_training_dag.py`** — ML training DAG with 6 tasks: snapshot dataset → prepare training data → train model → evaluate → register model → generate report. Scheduled daily. Tasks are placeholders — actual ML code comes in Phase 5.
4. **Installed `kafka-python`** inside the Airflow container so the Kafka health check task works.

### Step-by-Step Changes

1. Added Airflow service to `docker-compose.yml` — SQLite backend (no Postgres needed), SequentialExecutor, DAGs mounted from `./infra/airflow/dags`
2. Created `infra/airflow/dags/` directory
3. Started Airflow: `docker compose up airflow -d` — pulled image (~700MB), container started
4. Waited 30 seconds for DB initialization and admin user creation
5. Verified web UI: `curl http://localhost:8080/health` → HTTP 200
6. Checked resource usage: Airflow using 948MB of 1.5GB limit. Total Docker: ~1.6GB
7. Wrote `telemetry_pipeline_dag.py` — 8 tasks with BranchPythonOperator for quality gate
8. Verified DAG parses: `docker exec airflow python -c "from airflow.models import DagBag; ..."` — 8 tasks, no errors
9. Installed `kafka-python` in container: `docker exec --user root airflow bash -c "python -m pip install kafka-python"`
10. Unpaused and triggered: `airflow dags unpause telemetry_pipeline && airflow dags trigger telemetry_pipeline`
11. Verified run: all 7 happy-path tasks succeeded, `alert_quality_failure` correctly skipped
12. Wrote `ml_training_dag.py` — 6 tasks in linear chain, XCom passing between tasks
13. Verified ML DAG parses: 6 tasks, no errors
14. Triggered ML DAG: all 6 tasks succeeded
15. Verified task logs show meaningful output (PIPELINE COMPLETE, etc.)
16. All 6 Phase 4 verification checks passed

### Concepts & Definitions

**Apache Airflow** — A workflow orchestration platform. You define workflows as Python code (DAGs), and Airflow handles scheduling, running, retrying, and monitoring them. Think of it as a **robot manager**: instead of you manually running scripts at 3am, Airflow does it on schedule, handles failures gracefully, and shows you a dashboard of what's happening. You open the web UI at http://localhost:8080 (login: admin/admin) to see all your pipelines.

**DAG (Directed Acyclic Graph)** — A workflow definition in Airflow. "Directed" = tasks flow in one direction (A → B → C, never backwards). "Acyclic" = no loops (C can't trigger A again — that would create an infinite cycle). "Graph" = a network of connected nodes. Each node is a task, each edge is a dependency. Our telemetry pipeline DAG has 8 nodes connected in a chain with one branch.

**Task** — A single unit of work in a DAG. Each task does one thing: check Kafka, run quality checks, send an alert, etc. Tasks are defined using Operators (see below). Airflow tracks each task's state: `pending`, `running`, `success`, `failed`, `skipped`.

**Operator** — A template for creating a task. Airflow provides many operator types:
- `PythonOperator` — runs a Python function
- `BashOperator` — runs a shell command
- `BranchPythonOperator` — runs a Python function that returns the task_id of the next task to execute (used for if/else logic in workflows)
We used `PythonOperator` for most tasks and `BranchPythonOperator` for the quality gate.

**BranchPythonOperator** — A special operator that enables decision-making in a DAG. Its Python function must return the `task_id` of the next task to run. All other downstream branches are automatically **skipped**. In our quality gate: if checks pass, return `"run_batch_aggregation"` (happy path); if they fail, return `"alert_quality_failure"` (failure path).

**XCom (Cross-Communication)** — Airflow's mechanism for passing small pieces of data between tasks. One task "pushes" data: `context["ti"].xcom_push(key="snapshot_id", value="abc123")`. A downstream task "pulls" it: `context["ti"].xcom_pull(task_ids="snapshot_dataset", key="snapshot_id")`. XCom is for metadata (IDs, status, small results) — NOT for large datasets. Think of it like sticky notes between coworkers: "Here's the snapshot ID you need."

**SequentialExecutor** — Airflow's simplest executor: runs one task at a time, in order. Perfect for a laptop where we don't want multiple Spark jobs competing for RAM. Production systems use CeleryExecutor (distributed across multiple workers) or KubernetesExecutor (one pod per task). The executor is configured via `AIRFLOW__CORE__EXECUTOR` environment variable.

**Scheduling and `schedule_interval`** — How often Airflow runs a DAG. `timedelta(minutes=15)` means every 15 minutes. `"@daily"` means once per day at midnight. `None` means manual-only. Airflow creates a "DAG run" for each scheduled interval. You can also trigger runs manually from the UI or CLI.

**`catchup`** — When set to `False`, Airflow doesn't create runs for intervals that already passed. Without this, if you define a DAG with `start_date=2026-04-01` and first enable it on April 3rd, Airflow would try to run all the missed intervals (April 1st, 2nd, etc.). `catchup=False` says "just start from now."

**`start_date`** — The earliest date Airflow will schedule a run. It does NOT mean "start running now." If `start_date=April 1` and `schedule_interval=daily`, the first run covers April 1-2 and actually executes on April 2. This confuses everyone at first — just remember: the run for interval X executes AFTER interval X ends.

**Idempotency** — A task is idempotent if running it multiple times produces the same result as running it once. This is critical because Airflow retries failed tasks. If a task writes 100 rows and then fails on step 2, retrying shouldn't write another 100 rows (200 total). Our tasks use `IF NOT EXISTS` for table creation and `APPEND` for writes — safe to retry.

**`trigger_rule`** — Controls when a task runs relative to its upstream tasks. Default is `all_success` (all parents must succeed). We use `none_failed` on `notify_success` because the BranchPythonOperator skips some upstream tasks — `all_success` would prevent it from running since skipped != success.

**`default_args`** — A dictionary of settings applied to every task in a DAG unless overridden. Includes owner (for filtering), retries (how many times to retry on failure), retry_delay (wait between retries), etc. Saves you from repeating the same config on every task.

**Quality Gate** — A task in a pipeline that decides whether to proceed or halt based on data quality. Our `quality_gate` task pulls results from `run_quality_checks` and branches: pass → continue processing, fail → send alert and stop. This prevents bad data from flowing downstream into ML training or dashboards. It's the "taste test before serving."

**`host.docker.internal`** — A special DNS name that Docker provides to containers, pointing to the host machine's network. When Airflow (inside Docker) needs to connect to Kafka (also in Docker but exposed on localhost:9092), it uses `host.docker.internal:9092` instead of `localhost` (which inside a container refers to the container itself).

### Architecture Notes

Phase 4 adds the orchestration layer that ties all previous phases together:

```
Phase 4 architecture — Airflow as the central coordinator:

┌─────────────────────────────────────────────────────────────┐
│                    AIRFLOW (Docker, port 8080)               │
│                                                              │
│  telemetry_pipeline (every 15 min):                         │
│    check_kafka_lag                                           │
│         │                                                    │
│    run_stream_processor  ←── triggers Spark on host          │
│         │                                                    │
│    run_quality_checks                                        │
│         │                                                    │
│    quality_gate ─── PASS ──→ run_batch_aggregation           │
│         │                         │                          │
│         └── FAIL ──→ alert       update_metadata             │
│                                   │                          │
│                              notify_success                  │
│                                                              │
│  ml_training_pipeline (daily):                              │
│    snapshot → prepare → train → evaluate → register → report │
│                                                              │
└─────────────────────────────────────────────────────────────┘
         │              │              │
         ▼              ▼              ▼
    ┌─────────┐   ┌──────────┐   ┌──────────┐
    │  Kafka  │   │  Spark   │   │  MinIO   │
    │ (Docker)│   │ (Native) │   │ (Docker) │
    └─────────┘   └──────────┘   └──────────┘
```

**Key architectural decisions:**
- Airflow runs inside Docker with a mounted DAGs volume — edit DAG files on your Mac, Airflow picks them up automatically (every 30 seconds via `DAG_DIR_LIST_INTERVAL`).
- Spark jobs run **natively on the host**, not inside the Airflow container. The DAG tasks simulate triggering them. In production, you'd use `BashOperator` with `spark-submit` or `SSHOperator`.
- SQLite backend — single file, no separate Postgres container. Fine for single-user local dev.
- SequentialExecutor — one task at a time. Prevents multiple Spark jobs from competing for our limited RAM.

**Service resource summary after Phase 4:**

| Service | Container | RAM Usage | RAM Limit |
|---------|-----------|-----------|-----------|
| Kafka   | kafka     | ~558MB    | 1GB       |
| MinIO   | minio     | ~112MB    | 512MB     |
| Airflow | airflow   | ~948MB    | 1.5GB     |
| **Total Docker** | | **~1.6GB** | **3GB** |

Still well within the 8GB Docker allocation, leaving room for Spark (native, ~2GB) when needed.

### Key Code Explained

**DAG definition — the container for the workflow:**
```python
with DAG(
    dag_id="telemetry_pipeline",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["telemetry", "pipeline"],
) as dag:
```
- `with DAG(...) as dag:` — context manager that assigns all tasks defined inside to this DAG
- `dag_id` — unique name shown in the Airflow UI
- `schedule_interval` — how often to run (every 15 min)
- `catchup=False` — don't run for dates that already passed
- `tags` — labels for filtering in the UI (like Gmail labels)

**BranchPythonOperator — the quality gate:**
```python
def quality_gate(**context):
    all_passed = context["ti"].xcom_pull(
        task_ids="run_quality_checks", key="all_passed"
    )
    if all_passed:
        return "run_batch_aggregation"   # Happy path
    else:
        return "alert_quality_failure"    # Failure path
```
- `**context` — Airflow injects runtime context (execution date, task instance, etc.)
- `context["ti"]` — the TaskInstance object, used to read/write XCom
- `.xcom_pull(task_ids=..., key=...)` — read data pushed by an upstream task
- Returns a **task_id string** — Airflow runs that task and skips all other branches

**XCom — passing data between tasks:**
```python
# In run_quality_checks:
context["ti"].xcom_push(key="quality_results", value=results)

# In quality_gate (downstream):
all_passed = context["ti"].xcom_pull(task_ids="run_quality_checks", key="all_passed")
```
- `.xcom_push(key, value)` — store a value that downstream tasks can read
- `.xcom_pull(task_ids, key)` — retrieve a value stored by a specific upstream task
- XCom data is serialized to JSON and stored in Airflow's database
- Keep XCom values small (IDs, flags, summaries) — not large DataFrames

**Task dependencies — defining the DAG shape:**
```python
t1_check_kafka >> t2_stream >> t3_quality >> t4_gate
t4_gate >> t5_batch >> t6_metadata >> t7_success  # Happy path
t4_gate >> t_alert                                  # Failure path
```
- `>>` is Airflow's "set downstream" operator. `A >> B` means "B runs after A"
- You can chain: `A >> B >> C` (A, then B, then C)
- Branching: `t4_gate >> t5_batch` AND `t4_gate >> t_alert` creates two paths from the gate
- BranchPythonOperator picks which path to follow at runtime

**trigger_rule — handling skipped tasks:**
```python
t7_success = PythonOperator(
    task_id="notify_success",
    trigger_rule="none_failed",
)
```
- Default `trigger_rule="all_success"` requires ALL upstream tasks to succeed
- When BranchPythonOperator skips a path, those tasks have state "skipped" (not "success")
- `"none_failed"` means: run as long as no upstream task actually failed (skipped is OK)
- Without this, `notify_success` would never run because `alert_quality_failure` is always skipped on the happy path

### What Could Go Wrong

| Problem | Cause | Fix |
|---------|-------|-----|
| DAG not showing in Airflow UI | Syntax error in DAG file | `docker exec airflow python /opt/airflow/dags/your_dag.py` to see the error |
| "No module named 'kafka'" in Airflow | `kafka-python` not installed in container | `docker exec --user root airflow bash -c "python -m pip install kafka-python"` |
| Airflow container exits immediately | DB initialization failed or port conflict | Check `docker compose logs airflow` for the exact error |
| Tasks stay in "queued" state forever | Scheduler not running | Verify scheduler is running: `docker exec airflow ps aux \| grep scheduler` |
| DAG runs for old dates on first enable | `catchup=True` (default) | Set `catchup=False` in DAG definition |
| XCom pull returns None | Wrong `task_ids` or `key` in xcom_pull | Double-check the task_id string matches exactly; ensure the push happened first |
| BranchPythonOperator skips wrong tasks | Function returns wrong task_id | The returned string must exactly match a downstream task's `task_id` |
| "Permission denied" when installing pip packages | Airflow runs as non-root user | Use `docker exec --user root airflow bash -c "python -m pip install ..."` |
| Airflow using too much memory | Scheduler + webserver + workers | `mem_limit: 1536m` caps it; use SequentialExecutor to avoid extra workers |
| Task succeeds in Airflow but nothing happens on host | DAG tasks are simulated | Expected for local dev — actual Spark jobs run natively, not via Airflow |

### What I Should Be Able to Explain

- [ ] What Airflow is and why manual script execution doesn't scale
- [ ] What a DAG is and why it must be acyclic (no loops)
- [ ] What operators are and the difference between PythonOperator, BashOperator, and BranchPythonOperator
- [ ] What XCom is and when to use it (small metadata, not large datasets)
- [ ] What SequentialExecutor means and why we use it on a laptop
- [ ] What `catchup=False` does and why it matters
- [ ] What a quality gate is and how BranchPythonOperator implements it
- [ ] What `trigger_rule="none_failed"` means and why it's needed after a branch
- [ ] What idempotency means and why it matters for retries
- [ ] How Airflow's scheduling works (start_date, schedule_interval, execution order)
- [ ] Why the Airflow container needs `kafka-python` separately from the host's venv

---

## Phase 5 — PyTorch Anomaly Detection

> ✅ **Status:** Complete

### What Was Done

1. **`ml/dataset.py`** — Loads clean_events from Iceberg (optionally from a specific snapshot), pivots metrics into feature vectors (1 row = 1 time window with 5 metric columns), normalizes to [0,1] with MinMaxScaler, splits chronologically into train/val/test, returns PyTorch DataLoaders.
2. **`ml/model.py`** — PyTorch autoencoder: Encoder (5→64→32→16) compresses inputs to a 16-dim bottleneck, Decoder (16→32→64→5) reconstructs them. ReLU activations, Dropout regularization, Sigmoid output. 5,973 trainable parameters.
3. **`ml/train.py`** — Training loop: MSE loss, Adam optimizer, early stopping (patience=5), MPS GPU acceleration. Saves model checkpoint and full experiment log (hyperparams, losses, timing, dataset info) to `experiments/{experiment_id}/`.
4. **`ml/evaluate.py`** — Loads trained model and test data, computes per-sample reconstruction error, sets anomaly threshold at 95th percentile of training errors, generates synthetic anomalies for testing, calculates precision/recall/F1, saves evaluation report JSON.

### Step-by-Step Changes

1. Stopped Airflow container to free ~1.2GB RAM for PyTorch: `docker stop airflow`
2. Needed more data for ML — ran `fake_producer.py` for 2 minutes (238 events) then stream processor (228 valid → clean_events)
3. Created `ml/dataset.py` with pivot approach: per-device-per-window
4. First test: 0 feature vectors — data too sparse (50 devices × 5 metrics, few time windows have all 5)
5. Ran producer for 5 more minutes (595 events), processed through stream (888 valid events total)
6. Changed pivot strategy: aggregate across ALL devices per 1-minute window instead of per-device. Fill missing metrics with column mean
7. Re-tested: 14 feature vectors — small but workable for a learning build
8. Lowered minimum threshold from 10 to 5 vectors
9. Created `ml/model.py` — autoencoder architecture, tested forward pass with dummy data
10. Created `ml/train.py` — training loop with MPS support
11. Ran training: 50 epochs, loss decreased 0.113→0.058 (train), 0.090→0.067 (val), saved to `experiments/exp_20260402_225105/`
12. Confirmed MPS GPU was used (output: "Using device: MPS (Apple Silicon GPU)")
13. Created `ml/evaluate.py` — evaluation with synthetic anomalies
14. Ran evaluation: precision=1.00, recall=0.60, F1=0.75, zero false positives on normal data
15. All 7 Phase 5 verification checks passed

### Concepts & Definitions

**Autoencoder** — A neural network that learns to compress data and then reconstruct it. Imagine describing a photo with only 3 words, then someone recreating it from those words. If the photo is a "normal" scene you've practiced with, the reconstruction is close. If it's bizarre, the reconstruction fails badly. The gap between original and reconstruction is the **reconstruction error** — high error signals an anomaly. Our autoencoder compresses 5 telemetry metrics into 16 numbers (the bottleneck), then expands back to 5.

**Encoder** — The first half of the autoencoder: compresses input down to a small representation. Our encoder: 5 features → 64 neurons → 32 → 16 (bottleneck). Each layer has fewer neurons, forcing the network to keep only the most essential patterns — like summarizing a novel into a paragraph.

**Decoder** — The second half: reconstructs the original input from the compressed representation. Our decoder: 16 → 32 → 64 → 5 features. It's the mirror image of the encoder.

**Bottleneck (Latent Space)** — The narrowest layer in the autoencoder (16 neurons in our case). This is where the "essence" of normal data is captured. If you forced someone to describe every meal they ate using only 16 numbers, they'd learn to encode the most important aspects. The bottleneck forces the network to learn which patterns matter most.

**Reconstruction Error** — The difference between the autoencoder's input and output, measured as Mean Squared Error (MSE). For each sample: average of (original - reconstructed)² across all 5 features. Normal data: low error (model learned this pattern). Anomalous data: high error (model never learned this pattern).

**MSE (Mean Squared Error)** — The loss function we use: average of squared differences between predicted and actual values. Why squared? Squaring makes large errors count much more than small errors, which is exactly what we want — anomalies should produce dramatically higher errors. Formula: MSE = mean((predicted - actual)²).

**Adam Optimizer** — The algorithm that adjusts model weights to reduce loss. "Adam" stands for Adaptive Moment Estimation. It's the most popular optimizer because it adapts the learning rate per-parameter — features that rarely update get bigger steps, frequent ones get smaller steps. We set `lr=1e-3` (learning rate = 0.001), meaning each weight update step is 0.1% of the gradient.

**Epoch** — One complete pass through all training data. In our case, 9 training samples viewed once = 1 epoch. We run up to 50 epochs, showing the model the same data repeatedly so it can gradually refine its weights. Each epoch, the loss should decrease (if the model is learning).

**Early Stopping** — A technique to prevent overfitting. If validation loss hasn't improved for `patience` epochs (5 in our case), stop training. Without this, the model might memorize the training data perfectly but perform terribly on new data. It's like studying flashcards — at some point you've learned the concepts; further study just makes you memorize specific card wordings.

**Overfitting** — When a model learns the training data too well, including its noise and quirks, instead of learning general patterns. An overfitted anomaly detector would only recognize the exact patterns it trained on, flagging anything slightly different as anomalous (too many false alarms). Dropout and early stopping both combat overfitting.

**Dropout** — A regularization technique: during training, randomly set 20% of neurons to zero in each forward pass. This prevents any single neuron from becoming too important and forces the network to learn redundant representations. Think of it like training a team where random members are absent each day — the team learns to function without depending on any one person. Dropout is disabled during evaluation (`model.eval()`).

**ReLU (Rectified Linear Unit)** — An activation function: output = max(0, x). If the input is positive, pass it through unchanged. If negative, output zero. Simple, fast, and effective. It adds "non-linearity" to the network — without it, stacking layers would be no different from a single layer (because linear operations compose into linear operations).

**Sigmoid** — An activation function that squashes any input to the range [0, 1]. We use it on the decoder's output because our normalized input features are in [0, 1], and we want the reconstruction to be in the same range.

**MPS (Metal Performance Shaders)** — Apple's GPU framework for M-series chips. PyTorch can offload tensor operations to the M4's GPU via MPS, making training faster. This only works when running natively on macOS (not inside Docker, which runs Linux ARM64 and can't access the Mac GPU). We check `torch.backends.mps.is_available()` and use `torch.device("mps")`.

**Feature Engineering** — Transforming raw data into a format suitable for ML. Our raw data is one-metric-per-row; the autoencoder needs a fixed-size numeric vector per sample. We pivot the data so each row has all 5 metrics for one time window, then normalize values to [0, 1]. Good feature engineering often matters more than model architecture.

**Pivoting** — Reshaping data from "long" format (many rows, one metric per row) to "wide" format (one row per entity, one column per metric). Like a spreadsheet: instead of 5 rows for device_001's readings, we get 1 row with 5 columns. We use `pandas.pivot_table()` with `aggfunc='mean'` to handle duplicate readings.

**MinMaxScaler (Normalization)** — Scales each feature to the [0, 1] range: `x_scaled = (x - min) / (max - min)`. Why? Neural networks train better when all inputs are on a similar scale. Without normalization, CPU usage (0-100) would dominate network latency (0.5-200ms) simply because of its larger numeric range. The scaler parameters are saved for use during inference.

**Time-based Split** — Splitting data chronologically: oldest 70% for training, next 15% for validation, newest 15% for testing. This simulates reality: you train on historical data and predict on future data. Random splits would leak future information into training (the model could learn temporal patterns that only exist because it "saw" later data).

**Precision** — Of everything the model flagged as anomalous, what percentage actually was? Precision = TP / (TP + FP). High precision = few false alarms. Our model achieved 1.00 precision — every flag was a real anomaly.

**Recall** — Of all actual anomalies, what percentage did the model catch? Recall = TP / (TP + FN). High recall = few missed detections. Our model achieved 0.60 recall — caught 3 of 5 synthetic anomalies.

**F1 Score** — The harmonic mean of precision and recall: F1 = 2 × (P × R) / (P + R). Balances both metrics. Our F1 of 0.75 reflects high precision but moderate recall — the model is conservative (rarely cries wolf, but misses some anomalies).

**Anomaly Threshold** — The reconstruction error boundary above which we flag a sample as anomalous. We set it at the 95th percentile of training errors — "anything with higher error than 95% of normal training data is suspicious." Lower threshold = catch more anomalies but more false alarms. Higher threshold = fewer false alarms but miss more anomalies.

**Experiment Tracking** — Recording everything about a training run: hyperparameters, dataset snapshot, loss curves, evaluation metrics, model checkpoint. This makes experiments reproducible: "I can reload the exact data and model from experiment exp_20260402_225105 and get the same results." We save everything to JSON files in the `experiments/` directory.

### Architecture Notes

Phase 5 adds the ML layer that consumes data from Iceberg and produces anomaly detection results:

```
Phase 5 architecture:

┌─────────────────────────────────────────────────────┐
│  Iceberg Tables (MinIO)                              │
│  telemetry_db.clean_events (1196 events)             │
│  ↓ snapshot_id for reproducibility                   │
└──────────────────┬──────────────────────────────────┘
                   │
                   │ dataset.py loads via Spark
                   │ (pivot → normalize → split)
                   ▼
┌─────────────────────────────────────────────────────┐
│  PyTorch (NATIVE on Mac, using MPS GPU)              │
│                                                      │
│  ┌────────────┐    ┌────────────┐    ┌───────────┐  │
│  │ Train set  │    │  Val set   │    │ Test set  │  │
│  │ (70%)      │    │  (15%)     │    │ (15%)     │  │
│  └─────┬──────┘    └─────┬──────┘    └─────┬─────┘  │
│        │                 │                 │         │
│        ▼                 ▼                 ▼         │
│  ┌─────────────────────────────────────────────┐    │
│  │  TelemetryAutoencoder (5,973 params)        │    │
│  │  Encoder: 5 → 64 → 32 → 16 (bottleneck)    │    │
│  │  Decoder: 16 → 32 → 64 → 5                 │    │
│  └─────────────────────────────────────────────┘    │
│        │                                             │
│        ▼                                             │
│  ┌─────────────────────────────────────────────┐    │
│  │  experiments/{experiment_id}/                │    │
│  │    model.pt          (trained weights)       │    │
│  │    experiment.json   (hyperparams, losses)   │    │
│  │    evaluation.json   (P/R/F1, thresholds)    │    │
│  └─────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

**Key architectural decisions:**
- PyTorch runs **natively** (not in Docker) to access MPS GPU acceleration. Docker containers run Linux and can't access the Mac's Metal GPU.
- Stopped Airflow container during training to free ~1.2GB RAM — per CLAUDE.md's memory management plan, never run Spark + Airflow + PyTorch simultaneously.
- Data is loaded via Spark (to read Iceberg tables), converted to Pandas, then to PyTorch tensors. This Spark→Pandas handoff is fine for our dataset size.
- Experiment tracking uses simple JSON files instead of MLflow's full tracking server — lightweight and sufficient for local dev.
- Pivoting aggregates across all devices per time window (not per-device) because our small dataset is too sparse for per-device vectors.

### Key Code Explained

**Autoencoder forward pass:**
```python
def forward(self, x):
    latent = self.encoder(x)       # 5 features → 16-dim compressed
    reconstructed = self.decoder(latent)  # 16-dim → 5 features
    return reconstructed
```
- Input `x` is a batch of samples, shape (batch_size, 5)
- `self.encoder(x)` passes through 3 layers: Linear→ReLU→Dropout→Linear→ReLU→Dropout→Linear→ReLU
- The 16-dim `latent` vector is the compressed representation
- `self.decoder(latent)` mirrors the encoder, ending with Sigmoid to keep output in [0, 1]
- The model learns to minimize the difference between `x` and `reconstructed`

**Training one epoch — the core learning step:**
```python
reconstructed = model(batch_input)         # Forward pass
loss = criterion(reconstructed, batch_target)  # Measure error
optimizer.zero_grad()  # Clear old gradients
loss.backward()        # Compute new gradients
optimizer.step()       # Update weights
```
- `criterion(reconstructed, batch_target)` — MSE between reconstruction and original
- `optimizer.zero_grad()` — PyTorch accumulates gradients by default; we must clear them each step
- `loss.backward()` — backpropagation: compute how much each weight contributed to the error
- `optimizer.step()` — adjust each weight in the direction that reduces error

**Early stopping logic:**
```python
if val_loss < best_val_loss:
    best_val_loss = val_loss
    epochs_without_improvement = 0
    torch.save(model.state_dict(), "model.pt")  # Save best model
else:
    epochs_without_improvement += 1

if epochs_without_improvement >= patience:
    break  # Stop training
```
- We only save the model when validation loss improves (not the final model)
- If validation loss stagnates for `patience` epochs, training stops
- This prevents overfitting: the saved model is from the epoch with best generalization

**Reconstruction error for anomaly detection:**
```python
batch_errors = torch.mean((batch_input - reconstructed) ** 2, dim=1)
```
- `(batch_input - reconstructed) ** 2` — element-wise squared difference for each feature
- `torch.mean(..., dim=1)` — average across features (dim=1), giving one error per sample
- High error = model couldn't reconstruct this sample = potential anomaly

**Setting the anomaly threshold:**
```python
threshold_95 = np.percentile(train_errors, 95)
predicted_anomalies = test_errors > threshold_95
```
- Training data is assumed to be "normal" (it went through validation in Phase 3)
- 95th percentile means: 95% of normal data has error below this value
- Anything above this is flagged as anomalous
- It's a trade-off: lower percentile = more sensitive (catches more but more false alarms)

### What Could Go Wrong

| Problem | Cause | Fix |
|---------|-------|-----|
| "Only 0 feature vectors after pivoting" | Data too sparse — devices don't have all 5 metrics per window | Aggregate across all devices per time window, or use fillna for missing metrics |
| MPS not available | Running inside Docker, or outdated PyTorch | Run natively on Mac. `pip install --upgrade torch` |
| OOM during training | Dataset too large for GPU memory | Reduce batch_size, or switch to CPU for large datasets |
| Loss not decreasing | Learning rate too high or too low | Try `lr=1e-4` (slower) or `lr=1e-2` (faster). Check data normalization. |
| Loss is NaN | Extreme input values or too-high learning rate | Ensure MinMaxScaler is applied. Reduce learning rate. |
| Checkpoint won't load | Model architecture changed since training | Ensure model.py matches the architecture used during training |
| Low recall (missing anomalies) | Threshold too high or model undertrained | Lower threshold percentile (90th instead of 95th), or train on more data |
| High false positive rate | Threshold too low or noisy training data | Raise threshold percentile (99th instead of 95th) |
| Spark error during data loading | MinIO or Kafka not running | `docker compose up kafka minio -d` before running ML code |
| Training too slow on CPU | MPS not detected | Check `torch.backends.mps.is_available()`. Reinstall PyTorch if False. |

### What I Should Be Able to Explain

- [ ] What an autoencoder is and how the bottleneck forces it to learn "normal" patterns
- [ ] Why reconstruction error is high for anomalies and low for normal data
- [ ] What MSE loss measures and why we use it for autoencoders
- [ ] What an epoch is and why we train for multiple epochs
- [ ] What early stopping does and why it prevents overfitting
- [ ] What dropout does and why it's only active during training
- [ ] What MPS is and why PyTorch must run natively (not in Docker) to use it
- [ ] Why we split by time instead of randomly for time series data
- [ ] What normalization (MinMaxScaler) does and why it matters for neural networks
- [ ] What precision, recall, and F1 measure and the trade-off between them
- [ ] How the anomaly threshold is set and why the 95th percentile is a reasonable default
- [ ] Why we track experiments (snapshot_id, hyperparams, metrics) for reproducibility

---

## Phase 6 — FastAPI Backend

> ⏳ **Status:** Not started
> 📝 Claude will fill this section after completing Phase 6

### What Was Done
_To be written after phase completion_

### Step-by-Step Changes
_To be written after phase completion_

### Concepts & Definitions
_To be written after phase completion_

### Architecture Notes
_To be written after phase completion_

### Key Code Explained
_To be written after phase completion_

### What Could Go Wrong
_To be written after phase completion_

### What I Should Be Able to Explain
_To be written after phase completion_

---

## Phase 7 — React Portal

> ⏳ **Status:** Not started
> 📝 Claude will fill this section after completing Phase 7

### What Was Done
_To be written after phase completion_

### Step-by-Step Changes
_To be written after phase completion_

### Concepts & Definitions
_To be written after phase completion_

### Architecture Notes
_To be written after phase completion_

### Key Code Explained
_To be written after phase completion_

### What Could Go Wrong
_To be written after phase completion_

### What I Should Be Able to Explain
_To be written after phase completion_

---

## Full System Glossary

> 📝 This section is built up progressively. Each phase adds its terms here so you have one master reference.

| Term | Definition | Where It's Used |
| ---- | ---------- | --------------- |
| _To be populated phase by phase_ | | |

---

## How Everything Connects

> 📝 After all 7 phases, Claude will write a final synthesis section here that maps every file to every other file, every data flow, and every dependency — the complete mental model of the entire platform.

_To be written after all phases are complete_
