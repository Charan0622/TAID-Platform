# BUILD_GUIDE.md — Telemetry AI Data Platform

## Who This Is For

A developer on a MacBook Air M4 with 16GB RAM who has never used Apache tools (Kafka, Spark, Iceberg, Airflow) before. Every instruction assumes zero prior experience with these tools. Every concept is explained before use. No steps are skipped.

---

## What We Are Building

A local, fully functional data platform that:

1. Ingests telemetry events (fake sensor/app data) through Kafka
2. Processes and validates them with Spark
3. Stores clean data in Iceberg tables (versioned, queryable)
4. Orchestrates all pipelines with Airflow
5. Trains anomaly detection models with PyTorch
6. Serves results through a FastAPI backend
7. Displays everything in a React portal

This is a LEARNING BUILD — optimized for a single laptop, not production scale. Every component runs locally via Docker or native installs.

---

## Hardware Constraints & Rules

| Resource       | Limit                                                              |
| -------------- | ------------------------------------------------------------------ |
| RAM            | 16GB total — Docker gets 8GB max, leave 8GB for OS + IDE + tools  |
| CPU            | M4 chip (ARM64) — all Docker images MUST be arm64/aarch64 compatible |
| Disk           | Keep total project under 15GB including Docker images              |
| Network        | All services run on localhost, no cloud accounts needed            |

### Critical Rules for This Machine

- NEVER run Kafka + Spark + Airflow + PyTorch training simultaneously. Stagger them.
- ALWAYS use `platform: linux/arm64` in every Docker Compose service.
- ALWAYS use lightweight base images (slim, alpine) where possible.
- NEVER allocate more than 2GB to any single container.
- If RAM gets tight, run `docker system prune` to reclaim space.
- Prefer native Python (via venv) for PyTorch over Docker — Apple Silicon MPS acceleration only works natively.

---

## Project Structure

```
telemetry-platform/
├── BUILD_GUIDE.md               ← This file (master instructions)
├── docker-compose.yml           ← Runs Kafka, Spark, Airflow, MinIO
├── .env                         ← Environment variables (ports, passwords)
│
├── infra/                       ← Infrastructure configs
│   ├── kafka/
│   │   └── create-topics.sh     ← Auto-create Kafka topics on startup
│   ├── airflow/
│   │   ├── dags/                ← Airflow DAG definitions
│   │   └── airflow.cfg          ← Custom Airflow config
│   └── minio/
│       └── init.sh              ← Create S3-compatible buckets
│
├── ingestion/                   ← Data producers
│   ├── fake_producer.py         ← Generates fake telemetry → Kafka
│   └── schemas/
│       └── telemetry.avsc       ← Avro schema for telemetry events
│
├── processing/                  ← Spark jobs
│   ├── stream_processor.py      ← Structured Streaming: Kafka → Iceberg
│   ├── batch_etl.py             ← Batch aggregation jobs
│   └── quality_checks.py        ← Data quality validation SQL
│
├── storage/                     ← Iceberg catalog configs
│   └── catalog.py               ← PyIceberg catalog setup
│
├── ml/                          ← Machine learning
│   ├── dataset.py               ← Load data from Iceberg snapshots
│   ├── model.py                 ← PyTorch autoencoder definition
│   ├── train.py                 ← Training loop + experiment tracking
│   ├── evaluate.py              ← Evaluation + metrics reporting
│   └── experiments/             ← Saved experiment logs and checkpoints
│
├── backend/                     ← FastAPI service
│   ├── main.py                  ← API entry point
│   ├── routers/
│   │   ├── datasets.py          ← Dataset catalog endpoints
│   │   ├── lineage.py           ← Lineage graph endpoints
│   │   └── ml_results.py        ← ML experiment result endpoints
│   └── requirements.txt
│
├── portal/                      ← React frontend
│   ├── package.json
│   ├── src/
│   │   ├── App.jsx
│   │   ├── pages/
│   │   │   ├── DataCatalog.jsx
│   │   │   ├── LineageGraph.jsx
│   │   │   ├── PipelineHealth.jsx
│   │   │   └── MLDashboard.jsx
│   │   └── components/
│   └── vite.config.js
│
└── scripts/                     ← Helper scripts
    ├── setup.sh                 ← One-command full setup
    ├── teardown.sh              ← Stop everything and clean up
    ├── health_check.sh          ← Verify all services are running
    └── seed_data.sh             ← Generate initial test data
```

---

## Build Phases

Build this project in 7 sequential phases. NEVER skip ahead. Each phase must be verified working before moving to the next.

---

### PHASE 1 — Prerequisites & Environment Setup

**Goal:** Install all required tools on the Mac. Verify everything works.

**Steps:**

1. Install Homebrew (if not installed):
   ```
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```

2. Install Docker Desktop for Mac (Apple Silicon):
   - Download from https://www.docker.com/products/docker-desktop/
   - Open Docker Desktop → Settings → Resources → set Memory to 8GB, CPUs to 4
   - Enable "Use Virtualization Framework" and "VirtioFS"

3. Install Python 3.11+ (via Homebrew):
   ```
   brew install python@3.11
   ```

4. Install Node.js 20 LTS (via Homebrew):
   ```
   brew install node@20
   ```

5. Install Java 17 (needed for Spark):
   ```
   brew install openjdk@17
   echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 17)' >> ~/.zshrc
   source ~/.zshrc
   ```

6. Create project directory and Python virtual environment:
   ```
   mkdir -p ~/telemetry-platform && cd ~/telemetry-platform
   python3.11 -m venv .venv
   source .venv/bin/activate
   ```

7. Install core Python packages:
   ```
   pip install kafka-python avro-python3 pyspark==3.5.3 pyiceberg fastapi uvicorn torch torchvision pandas numpy scikit-learn mlflow httpx
   ```

**Verification Checklist (all must pass before Phase 2):**
- [ ] `docker --version` prints version
- [ ] `docker compose version` prints version
- [ ] `python3 --version` shows 3.11+
- [ ] `node --version` shows 20+
- [ ] `java -version` shows 17+
- [ ] `python3 -c "import torch; print(torch.backends.mps.is_available())"` prints `True`

---

### PHASE 2 — Kafka + Data Ingestion

**Goal:** Run Kafka locally. Write a Python producer that sends fake telemetry events. Verify messages arrive.

**What is Kafka (for beginners):**
Kafka is a message queue on steroids. Producers send messages to "topics" (like named mailboxes). Consumers read from topics. Messages are stored durably and can be replayed. It handles millions of messages per second.

**What to build:**

1. `docker-compose.yml` with:
   - Kafka (use `bitnami/kafka:3.7` — arm64 compatible, includes KRaft mode so no ZooKeeper needed)
   - One Kafka topic: `telemetry.raw`
   - Expose port 9092 to localhost

2. `ingestion/fake_producer.py`:
   - Generates fake telemetry JSON events every 0.5 seconds
   - Event schema: `{ timestamp, device_id, metric_name, value, unit, location }`
   - device_id: random from 50 fake devices
   - metric_name: one of [cpu_usage, memory_usage, temperature, disk_io, network_latency]
   - value: realistic float ranges per metric (cpu: 0-100, temp: 15-95, etc.)
   - Occasionally (5% of events) inject anomalies: extreme values, nulls, malformed data
   - Sends to Kafka topic `telemetry.raw`

3. `ingestion/schemas/telemetry.avsc` — Avro schema defining the event structure

**Docker Compose rules for Kafka:**
```yaml
services:
  kafka:
    image: bitnami/kafka:3.7
    platform: linux/arm64
    ports:
      - "9092:9092"
    environment:
      - KAFKA_CFG_NODE_ID=0
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@kafka:9093
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
    mem_limit: 1g
```

**Verification Checklist:**
- [ ] `docker compose up kafka -d` starts without errors
- [ ] Running `fake_producer.py` prints "Sent event to telemetry.raw" messages
- [ ] A test consumer script reads events back from the topic
- [ ] Anomalous events (nulls, extremes) appear ~5% of the time

---

### PHASE 3 — Spark Processing + Iceberg Storage

**Goal:** Spark reads from Kafka, validates data, writes clean records to Iceberg tables. Bad records go to a dead-letter table.

**What is Spark (for beginners):**
Spark is a distributed computation engine. Even on one laptop, it processes data in parallel across CPU cores. "Structured Streaming" means it continuously reads new Kafka messages in small batches and processes them like SQL tables.

**What is Iceberg (for beginners):**
Iceberg makes regular files (Parquet) behave like a proper database table — with transactions, schema enforcement, and versioning. Every time you write data, it creates a "snapshot." You can query any past snapshot (time travel).

**What to build:**

1. Add MinIO to docker-compose (S3-compatible local storage for Iceberg):
   ```yaml
   minio:
     image: minio/minio:latest
     platform: linux/arm64
     ports:
       - "9000:9000"
       - "9001:9001"
     command: server /data --console-address ":9001"
     environment:
       MINIO_ROOT_USER: minioadmin
       MINIO_ROOT_PASSWORD: minioadmin
     mem_limit: 512m
   ```

2. `processing/stream_processor.py`:
   - PySpark Structured Streaming job
   - Reads from Kafka topic `telemetry.raw`
   - Validation rules:
     - Reject if timestamp is null or in the future
     - Reject if device_id is not in known device list
     - Reject if value is null or negative (for metrics that shouldn't be)
     - Reject if metric_name is not in allowed list
   - Valid records → write to Iceberg table `telemetry_db.clean_events`
   - Invalid records → write to Iceberg table `telemetry_db.dead_letter` with rejection reason
   - Checkpoint to local directory for exactly-once processing

3. `processing/batch_etl.py`:
   - Spark batch job (runs on-demand or via Airflow)
   - Reads from `telemetry_db.clean_events`
   - Computes hourly aggregations: avg, min, max, stddev per device per metric
   - Writes to `telemetry_db.hourly_aggregates`

4. `processing/quality_checks.py`:
   - Spark SQL queries that check:
     - Row count in last hour > minimum threshold
     - Null rate per column < 1%
     - Value distributions within expected ranges
     - No duplicate event IDs
   - Returns pass/fail per check with details

**Spark + Iceberg configuration notes:**
- Use PySpark 3.5.3 with `iceberg-spark-runtime` JAR
- Iceberg catalog type: `hadoop` or `rest` (use hadoop for simplicity)
- Warehouse path: `s3a://telemetry-warehouse/` pointing to MinIO
- Spark master: `local[*]` (use all CPU cores)
- Set `spark.driver.memory=2g` — do NOT exceed this on 16GB machine
- Set `spark.sql.shuffle.partitions=8` (default 200 is way too many for local)

**Verification Checklist:**
- [ ] MinIO console accessible at http://localhost:9001
- [ ] `stream_processor.py` reads Kafka events and writes to Iceberg tables
- [ ] Querying `telemetry_db.clean_events` returns validated rows
- [ ] `telemetry_db.dead_letter` contains rejected events with reasons
- [ ] `batch_etl.py` produces `hourly_aggregates` table
- [ ] `quality_checks.py` returns pass/fail results
- [ ] Time-travel query on Iceberg table returns older snapshot data

---

### PHASE 4 — Airflow Orchestration

**Goal:** Airflow schedules and monitors all pipelines. DAGs chain ingestion checks → Spark processing → quality gates → notifications.

**What is Airflow (for beginners):**
Airflow is a job scheduler with a web UI. You define workflows as Python code (DAGs — Directed Acyclic Graphs). Each node is a task (run a script, check a condition, send an alert). Airflow handles scheduling, retries, dependencies, and monitoring.

**What to build:**

1. Add Airflow to docker-compose:
   - Use `apache/airflow:2.9.3-python3.11` image
   - SQLite backend (local dev only — no need for Postgres)
   - Mount local `infra/airflow/dags/` into container
   - Expose web UI on port 8080
   - Set `AIRFLOW__CORE__LOAD_EXAMPLES=false`
   - mem_limit: 1.5g

2. `infra/airflow/dags/telemetry_pipeline_dag.py`:
   - Schedule: every 15 minutes
   - Task chain:
     1. `check_kafka_lag` — verify Kafka consumer isn't falling behind
     2. `run_stream_processor` — trigger Spark streaming micro-batch
     3. `run_quality_checks` — execute quality_checks.py
     4. `quality_gate` — BranchPythonOperator: if checks pass → continue, if fail → alert
     5. `run_batch_aggregation` — trigger batch_etl.py
     6. `update_metadata` — refresh dataset catalog metadata
     7. `notify_success` — log completion

3. `infra/airflow/dags/ml_training_dag.py`:
   - Schedule: daily (or manual trigger)
   - Task chain:
     1. `snapshot_dataset` — record current Iceberg snapshot ID
     2. `prepare_training_data` — export dataset from snapshot to Parquet
     3. `train_model` — run ml/train.py
     4. `evaluate_model` — run ml/evaluate.py
     5. `register_model` — save model artifact + metadata
     6. `generate_report` — create evaluation report

**Airflow rules for this machine:**
- Use `SequentialExecutor` (not Celery/Kubernetes) — one task at a time is fine locally
- Keep DAG file parsing lightweight — no heavy imports at module level
- Use `BashOperator` or `PythonOperator` — avoid KubernetesOperator locally
- All tasks should be idempotent (safe to re-run)

**Verification Checklist:**
- [ ] Airflow web UI accessible at http://localhost:8080
- [ ] `telemetry_pipeline_dag` visible and parseable (no import errors)
- [ ] Manual trigger runs full task chain successfully
- [ ] Quality gate correctly branches on pass/fail
- [ ] `ml_training_dag` visible and can be triggered manually
- [ ] Task logs show meaningful output

---

### PHASE 5 — PyTorch Anomaly Detection

**Goal:** Train an autoencoder on clean telemetry data. Use it to score new data and flag anomalies. Track experiments.

**What is an Autoencoder (for beginners):**
An autoencoder compresses data into a small representation, then reconstructs it. If trained on "normal" data, it reconstructs normal patterns well but fails on anomalies. High reconstruction error = anomaly.

**IMPORTANT: Run PyTorch NATIVELY, not in Docker.** Apple M4 has a GPU (MPS backend) that PyTorch can use, but only from native macOS Python — not from inside Docker (which runs Linux ARM64).

**What to build:**

1. `ml/dataset.py`:
   - Connects to Iceberg via PyIceberg
   - Accepts a snapshot_id parameter for reproducibility
   - Loads `clean_events` table into a Pandas DataFrame
   - Feature engineering:
     - Pivot: rows = (device_id, time_window), columns = metric values
     - Normalize all features to [0, 1] using MinMaxScaler
     - Save scaler parameters for inference
   - Train/validation/test split by time (not random — this is time series)
   - Returns PyTorch DataLoaders

2. `ml/model.py`:
   - PyTorch autoencoder:
     - Encoder: Input(N) → 64 → 32 → 16 (latent)
     - Decoder: 16 → 32 → 64 → Output(N)
     - ReLU activations, dropout for regularization
   - N = number of metric features (5 in our case, expandable)
   - Model should be small — this is a laptop, not a GPU cluster

3. `ml/train.py`:
   - Training loop:
     - Loss: MSE (reconstruction error)
     - Optimizer: Adam, lr=1e-3
     - Epochs: 50 (configurable)
     - Early stopping on validation loss (patience=5)
   - Use MPS device if available: `torch.device("mps")`
   - Experiment tracking (save to JSON file — lightweight MLflow alternative):
     - Record: snapshot_id, hyperparameters, epoch losses, best validation loss, training time
   - Save model checkpoint: `experiments/{experiment_id}/model.pt`

4. `ml/evaluate.py`:
   - Load trained model and test dataset
   - Compute reconstruction error for every test sample
   - Set anomaly threshold at 95th or 99th percentile of training errors
   - Metrics: precision, recall, F1 (using injected anomalies as ground truth)
   - Generate evaluation report as JSON:
     - Metrics summary
     - Confusion matrix values
     - Error distribution statistics
     - Link to training snapshot_id and experiment_id

**Verification Checklist:**
- [ ] `dataset.py` loads data from Iceberg and returns DataLoaders
- [ ] `python3 -c "import torch; print(torch.device('mps'))"` works
- [ ] `train.py` runs, loss decreases, and checkpoint is saved
- [ ] Training uses MPS (check: `Activity Monitor → GPU History` shows usage)
- [ ] `evaluate.py` produces metrics report
- [ ] Experiments directory contains reproducible logs
- [ ] Re-running with same snapshot_id produces same results

---

### PHASE 6 — FastAPI Backend

**Goal:** REST API that serves dataset metadata, lineage information, ML results, and pipeline health to the frontend.

**What to build:**

1. `backend/main.py`:
   - FastAPI app with CORS middleware (allow localhost:5173 for React dev server)
   - Mount routers for each domain
   - Startup event: load metadata caches
   - Health check endpoint: `GET /health`

2. `backend/routers/datasets.py`:
   - `GET /api/datasets` — list all Iceberg tables with metadata (row count, last updated, snapshot count, schema)
   - `GET /api/datasets/{table_name}` — detailed table info including column stats, quality scores, recent snapshots
   - `GET /api/datasets/{table_name}/snapshots` — list all snapshots with timestamps (for time-travel UI)
   - `GET /api/datasets/{table_name}/sample?snapshot_id=X` — return 100 sample rows from a specific snapshot

3. `backend/routers/lineage.py`:
   - `GET /api/lineage` — return full lineage graph as nodes + edges JSON
   - Nodes: Kafka topics, Spark jobs, Iceberg tables, ML models
   - Edges: data flow connections with transformation descriptions
   - This is MANUALLY DEFINED metadata (not auto-discovered) — store in a JSON config file

4. `backend/routers/ml_results.py`:
   - `GET /api/ml/experiments` — list all experiment runs with summary metrics
   - `GET /api/ml/experiments/{id}` — full experiment detail (hyperparameters, metrics, training curve data, dataset snapshot link)
   - `GET /api/ml/latest-model` — info about the most recent production model

5. `backend/routers/health.py`:
   - `GET /api/health/pipelines` — Airflow DAG statuses (call Airflow REST API)
   - `GET /api/health/kafka` — consumer lag metrics
   - `GET /api/health/storage` — Iceberg table freshness (time since last write)

**Backend rules:**
- All responses must be JSON
- Use Pydantic models for every response schema
- Add proper error handling — never return raw Python tracebacks
- Endpoints must respond within 2 seconds
- Run with: `uvicorn backend.main:app --reload --port 8000`

**Verification Checklist:**
- [ ] `uvicorn backend.main:app --reload` starts without errors
- [ ] `GET http://localhost:8000/health` returns OK
- [ ] `GET http://localhost:8000/api/datasets` returns table metadata
- [ ] `GET http://localhost:8000/api/lineage` returns graph structure
- [ ] `GET http://localhost:8000/api/ml/experiments` returns experiment list
- [ ] OpenAPI docs accessible at http://localhost:8000/docs

---

### PHASE 7 — React Portal

**Goal:** Web UI for dataset discovery, lineage visualization, ML dashboard, and pipeline monitoring.

**What to build:**

1. Initialize React project:
   ```
   cd portal && npm create vite@latest . -- --template react
   npm install axios react-router-dom recharts @xyflow/react tailwindcss @tailwindcss/vite
   ```

2. `portal/src/pages/DataCatalog.jsx`:
   - Table listing all datasets from `/api/datasets`
   - Each row shows: table name, row count, last updated, quality score (color-coded)
   - Click a row → expand to show schema, recent snapshots, sample data preview
   - Search/filter bar at top

3. `portal/src/pages/LineageGraph.jsx`:
   - Interactive node graph using React Flow (@xyflow/react)
   - Fetch graph from `/api/lineage`
   - Nodes colored by type (blue=source, orange=processing, cyan=storage, red=ML, purple=API)
   - Click a node → sidebar shows details
   - Zoom, pan, auto-layout

4. `portal/src/pages/PipelineHealth.jsx`:
   - Dashboard showing:
     - Airflow DAG run statuses (last 10 runs per DAG, color-coded green/red/yellow)
     - Kafka consumer lag gauge
     - Data freshness indicators per table
   - Auto-refresh every 30 seconds

5. `portal/src/pages/MLDashboard.jsx`:
   - List of experiment runs from `/api/ml/experiments`
   - Click an experiment → show:
     - Training loss curve (Recharts line chart)
     - Evaluation metrics (precision, recall, F1) as stat cards
     - Hyperparameters table
     - Link to dataset snapshot used
   - Compare mode: select two experiments side by side

**React rules:**
- Use Tailwind CSS for all styling
- Use React Router for navigation
- Fetch data with axios in useEffect hooks
- Show loading states and error states for every API call
- Keep components small and single-responsibility
- Run with: `npm run dev` (Vite dev server on port 5173)

**Verification Checklist:**
- [ ] `npm run dev` starts without errors
- [ ] Data Catalog page lists datasets from API
- [ ] Lineage Graph renders interactive node diagram
- [ ] Pipeline Health shows DAG statuses and freshness
- [ ] ML Dashboard shows experiments and training curves
- [ ] All pages handle loading and error states gracefully

---

## Service Port Map

| Service          | Port  | URL                          |
| ---------------- | ----- | ---------------------------- |
| Kafka            | 9092  | localhost:9092               |
| MinIO API        | 9000  | http://localhost:9000        |
| MinIO Console    | 9001  | http://localhost:9001        |
| Airflow          | 8080  | http://localhost:8080        |
| FastAPI          | 8000  | http://localhost:8000/docs   |
| React Portal     | 5173  | http://localhost:5173        |

---

## Memory Management Plan

Since we only have 16GB, follow this plan strictly:

| Scenario                            | What to Run                                  | Est. RAM  |
| ----------------------------------- | -------------------------------------------- | --------- |
| **Ingestion Development**           | Docker (Kafka + MinIO) + fake_producer.py    | ~3GB      |
| **Spark Development**               | Docker (Kafka + MinIO) + PySpark locally     | ~6GB      |
| **Airflow Development**             | Docker (Kafka + MinIO + Airflow)             | ~5GB      |
| **ML Training**                     | Stop Spark/Airflow containers, run PyTorch   | ~4GB      |
| **Full Stack Demo**                 | All Docker + FastAPI + React (no training)   | ~7GB      |

**Commands to manage:**
```bash
# Start only what you need
docker compose up kafka minio -d          # Minimal: ingestion work
docker compose up kafka minio airflow -d  # Pipeline work
docker compose down                        # Stop everything

# Check memory usage
docker stats --no-stream

# Nuclear option if things get slow
docker system prune -a --volumes
```

---

## Execution Rules

When the user asks to build any phase, follow these rules:

1. **Always verify prerequisites first.** Before writing code for Phase N, confirm Phase N-1 is working.

2. **One file at a time.** Write one file, test it, confirm it works, then move to the next. Never dump 5 files at once.

3. **Explain before coding.** Before writing any file, explain in 2-3 sentences what this file does and why it exists in the architecture.

4. **Test immediately.** After every file, provide the exact command to test it and what output to expect.

5. **Handle errors patiently.** When something breaks (it will), debug step by step. Check Docker logs first (`docker compose logs kafka`), then Python tracebacks, then network connectivity.

6. **Respect the RAM.** Before starting any phase, tell the user which containers to start and which to stop.

7. **Arm64 always.** Every Docker image must work on Apple Silicon. Verify before using. If an image doesn't exist for arm64, find an alternative.

8. **Beginner-friendly comments.** Every Python file must have a module-level docstring explaining what it does. Complex lines get inline comments.

9. **No unnecessary dependencies.** If the standard library or an already-installed package can do it, don't add another dependency.

10. **Checkpoint progress.** At the end of each phase, tell the user to commit to git:
    ```
    git add -A && git commit -m "Phase N complete: [description]"
    ```

---

## Troubleshooting Quick Reference

| Problem                                  | Fix                                                                 |
| ---------------------------------------- | ------------------------------------------------------------------- |
| Kafka container won't start              | Check Docker has 8GB memory allocated. Run `docker compose down -v` then retry. |
| `kafka-python` can't connect             | Wait 15 seconds after Kafka starts. Check port 9092 is not in use.  |
| Spark out of memory                      | Reduce `spark.driver.memory` to `1g`. Reduce `spark.sql.shuffle.partitions` to `4`. |
| Iceberg table not found                  | Ensure MinIO bucket exists. Check warehouse path in Spark config.   |
| Airflow DAG not showing up               | Check for Python syntax errors: `python3 infra/airflow/dags/your_dag.py`. |
| PyTorch MPS not available                | Ensure running natively (not in Docker). Update: `pip install --upgrade torch`. |
| React can't reach FastAPI                | Check CORS middleware allows `http://localhost:5173`.                |
| Docker eating too much RAM               | `docker stats --no-stream` → stop containers you don't need.       |
| MinIO "access denied"                    | Credentials: minioadmin/minioadmin. Check `AWS_ACCESS_KEY_ID` env var. |
| Airflow "executor crashed"               | Set executor to `SequentialExecutor`. Increase container memory to 1.5g. |

---

## Success Criteria

When all 7 phases are done, you should be able to:

1. Start the platform with `docker compose up -d`
2. Run `fake_producer.py` and see events flowing into Kafka
3. Watch Spark process events and write clean data to Iceberg
4. Open Airflow at :8080 and see green DAG runs
5. Run ML training and see loss decrease over epochs
6. Open the React portal at :5173 and browse:
   - Dataset catalog with schemas and quality scores
   - Interactive lineage graph
   - Pipeline health dashboard
   - ML experiment results with training curves
7. Query historical data using Iceberg time-travel

This is a fully functional, local, end-to-end data + ML platform built entirely on your MacBook Air.
