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

> ⏳ **Status:** Not started
> 📝 Claude will fill this section after completing Phase 3

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

## Phase 4 — Airflow Orchestration

> ⏳ **Status:** Not started
> 📝 Claude will fill this section after completing Phase 4

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

## Phase 5 — PyTorch Anomaly Detection

> ⏳ **Status:** Not started
> 📝 Claude will fill this section after completing Phase 5

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
