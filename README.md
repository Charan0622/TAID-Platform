# Telemetry AI Data Platform

<div align="center">

**A fully local, end-to-end data + ML platform for ingesting, processing, validating, and analyzing operational telemetry — built to run entirely on a MacBook.**

[![Kafka](https://img.shields.io/badge/Apache_Kafka-3.9.0-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache_Spark-3.5.3-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Iceberg](https://img.shields.io/badge/Apache_Iceberg-1.7.1-4E8EE9?style=for-the-badge&logo=apache&logoColor=white)](https://iceberg.apache.org/)
[![Airflow](https://img.shields.io/badge/Apache_Airflow-2.9.3-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![PyTorch](https://img.shields.io/badge/PyTorch-2.11-EE4C2C?style=for-the-badge&logo=pytorch&logoColor=white)](https://pytorch.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.135-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![React](https://img.shields.io/badge/React-19-61DAFB?style=for-the-badge&logo=react&logoColor=black)](https://react.dev/)

---

[Quick Start](#-quick-start) · [Architecture](#-architecture) · [Features](#-features) · [Tech Stack](#-tech-stack) · [Screenshots](#-portal-pages) · [Project Structure](#-project-structure)

</div>

---

## What Is This?

A **production-grade data platform** running locally that demonstrates the full lifecycle of operational telemetry:

```
Raw sensor data → Event streaming → Validation → Versioned storage → Anomaly detection → Web portal
```

It simulates 50 IoT devices generating metrics (CPU, memory, temperature, disk I/O, network latency), processes them through a real-time pipeline with quality gates, trains a neural network to detect anomalies, and presents everything through an interactive web dashboard.

> **Built for learning.** Every component is documented, every decision is explained, and every concept is taught from scratch in `EXPLANATION.md`.

---

## Data Pipeline Flow

```mermaid
flowchart TB
    subgraph INGEST["1 &nbsp; INGESTION"]
        direction LR
        P["Fake Producer<br/><sub>50 devices &middot; 5 metrics<br/>5% anomaly injection</sub>"]
        K["Kafka<br/><sub>topic: telemetry.raw</sub>"]
        P -->|"JSON events<br/>every 0.5s"| K
    end

    subgraph PROCESS["2 &nbsp; PROCESSING"]
        direction LR
        SP["Spark Streaming<br/><sub>Structured Streaming<br/>4 validation rules</sub>"]
        CE["Iceberg<br/>clean_events<br/><sub>validated data</sub>"]
        DL["Iceberg<br/>dead_letter<br/><sub>rejected + reasons</sub>"]
        SP -->|"valid"| CE
        SP -->|"invalid"| DL
    end

    subgraph AGGREGATE["3 &nbsp; AGGREGATION"]
        direction LR
        BE["Batch ETL<br/><sub>hourly stats:<br/>avg, min, max, stddev</sub>"]
        HA["Iceberg<br/>hourly_aggregates"]
        QC["Quality Checks<br/><sub>row count &middot; nulls<br/>ranges &middot; duplicates</sub>"]
        CE2["clean_events"] --> BE --> HA
        CE2 --> QC
    end

    subgraph ML["4 &nbsp; ML TRAINING"]
        direction LR
        DS["Dataset Prep<br/><sub>pivot &middot; normalize<br/>time-split</sub>"]
        AE["Autoencoder<br/><sub>5 &rarr; 64 &rarr; 32 &rarr; 16<br/>16 &rarr; 32 &rarr; 64 &rarr; 5</sub>"]
        EV["Evaluate<br/><sub>precision &middot; recall<br/>F1 score</sub>"]
        DS --> AE --> EV
    end

    subgraph SERVE["5 &nbsp; SERVING"]
        direction LR
        API["FastAPI<br/><sub>11 REST endpoints<br/>port 8000</sub>"]
        UI["React Portal<br/><sub>4 interactive pages<br/>port 5173</sub>"]
        API --> UI
    end

    subgraph ORCHESTRATE["6 &nbsp; ORCHESTRATION"]
        AF["Airflow<br/><sub>2 DAGs &middot; quality gates<br/>scheduling &middot; monitoring</sub>"]
    end

    K --> SP
    CE --> CE2
    CE --> DS
    CE --> API
    HA --> API
    EV --> API
    AF -.->|"schedules"| SP
    AF -.->|"schedules"| BE
    AF -.->|"schedules"| QC
    AF -.->|"schedules"| AE

    style INGEST fill:#1e3a5f,stroke:#3b82f6,color:#fff
    style PROCESS fill:#5c2d0e,stroke:#f97316,color:#fff
    style AGGREGATE fill:#164e63,stroke:#06b6d4,color:#fff
    style ML fill:#5c1a1a,stroke:#ef4444,color:#fff
    style SERVE fill:#3b1a6e,stroke:#a855f7,color:#fff
    style ORCHESTRATE fill:#1a3a1a,stroke:#22c55e,color:#fff
```

---

## Quick Start

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Docker Desktop | 29+ | Runs Kafka, MinIO, Airflow |
| Python | 3.11+ | Data processing, ML, backend |
| Node.js | 20+ | React frontend |
| Java | 17 | Required by Spark (JVM) |

### 1. Clone & Setup

```bash
git clone https://github.com/Charan0622/TAID-Platform.git
cd TAID-Platform

# Create Python virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# Install Python dependencies
pip install kafka-python avro-python3 pyspark==3.5.3 pyiceberg \
    fastapi uvicorn torch torchvision pandas numpy scikit-learn mlflow httpx

# Install React dependencies
cd portal && npm install && cd ..
```

### 2. Start Infrastructure

```bash
# Start Kafka + MinIO + Airflow
docker compose up -d

# Wait ~30 seconds for services to initialize
```

### 3. Generate & Process Data

```bash
# Terminal 1: Generate fake telemetry events
python ingestion/fake_producer.py

# Terminal 2: Process events through Spark → Iceberg
python processing/stream_processor.py

# Terminal 3: Run batch aggregations
python processing/batch_etl.py

# Run quality checks
python processing/quality_checks.py
```

### 4. Train ML Model

```bash
# Stop Airflow first to free RAM
docker stop airflow

# Train the anomaly detection autoencoder (uses MPS GPU)
python ml/train.py

# Evaluate the model
python ml/evaluate.py

# Restart Airflow
docker start airflow
```

### 5. Launch the Portal

```bash
# Terminal 1: Start the API backend
uvicorn backend.main:app --port 8000

# Terminal 2: Start the React frontend
cd portal && npm run dev
```

### 6. Open in Browser

| Service | URL | Credentials |
|---------|-----|-------------|
| **React Portal** | http://localhost:5173 | — |
| **API Docs** | http://localhost:8000/docs | — |
| **Airflow** | http://localhost:8080 | admin / admin |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |

---

## Features

### Data Catalog
> Browse Iceberg tables with schemas, row counts, quality scores, and snapshot history

- Expandable table rows showing column types and nullability
- Color-coded quality scores (green > 90%, yellow > 70%, red < 70%)
- Snapshot listing for time-travel queries

### Lineage Graph
> Interactive visualization of the complete data pipeline

- 11 nodes representing every component (sources, processors, tables, ML, API)
- 12 edges showing data flow with transformation descriptions
- Color-coded by type: blue (source), orange (processing), cyan (storage), red (ML), purple (API)
- Zoom, pan, and click for details

### Pipeline Health
> Real-time monitoring of all platform services

- Live Kafka connectivity check with topic listing
- Airflow DAG run history with success/failure indicators
- Table freshness monitoring (stale data alerts)
- Auto-refreshes every 30 seconds

### ML Dashboard
> Experiment tracking with training curves and evaluation metrics

- Experiment list with sortable columns
- Interactive training loss curves (train + validation) via Recharts
- Metric cards: precision, recall, F1 score, best validation loss
- Hyperparameter table for each experiment
- Links to dataset snapshots for reproducibility

---

## Architecture

### System Overview

```mermaid
graph LR
    subgraph Docker["Docker (8GB limit)"]
        K[Kafka<br/>:9092]
        M[MinIO<br/>:9000/:9001]
        A[Airflow<br/>:8080]
    end

    subgraph Native["Native macOS"]
        S[PySpark]
        PT[PyTorch<br/>MPS GPU]
        FA[FastAPI<br/>:8000]
        R[React<br/>:5173]
    end

    K --> S
    S --> M
    M --> PT
    M --> FA
    PT --> FA
    FA --> R
    A -.->|orchestrates| S

    style Docker fill:#1a1a2e,stroke:#4b5563,color:#fff
    style Native fill:#0d1117,stroke:#4b5563,color:#fff
```

### Validation Rules (Stream Processor)

```mermaid
flowchart LR
    E[Raw Event] --> V{Validate}
    V -->|"timestamp null<br/>or future"| DL[Dead Letter]
    V -->|"unknown<br/>device_id"| DL
    V -->|"value null<br/>or negative"| DL
    V -->|"unknown<br/>metric_name"| DL
    V -->|"All rules<br/>pass"| CE[Clean Events]

    style CE fill:#166534,stroke:#22c55e,color:#fff
    style DL fill:#7f1d1d,stroke:#ef4444,color:#fff
```

### ML Anomaly Detection

```mermaid
flowchart LR
    subgraph Training
        D[Clean Events<br/>Iceberg] --> FE[Feature<br/>Engineering]
        FE --> N[Normalize<br/>0 to 1]
        N --> AE[Autoencoder<br/>5→16→5]
    end

    subgraph Detection
        NEW[New Data] --> AE2[Trained<br/>Model]
        AE2 --> RE{Reconstruction<br/>Error}
        RE -->|"< threshold"| NOR[Normal]
        RE -->|"> threshold"| ANO[Anomaly!]
    end

    style NOR fill:#166534,stroke:#22c55e,color:#fff
    style ANO fill:#7f1d1d,stroke:#ef4444,color:#fff
```

### Airflow DAGs

```mermaid
flowchart LR
    subgraph telemetry_pipeline["telemetry_pipeline (every 15 min)"]
        T1[Check Kafka] --> T2[Stream<br/>Processor]
        T2 --> T3[Quality<br/>Checks]
        T3 --> T4{Quality<br/>Gate}
        T4 -->|pass| T5[Batch<br/>Aggregation]
        T5 --> T6[Update<br/>Metadata]
        T6 --> T7[Notify<br/>Success]
        T4 -->|fail| T8[Alert!]
    end

    style T4 fill:#92400e,stroke:#f59e0b,color:#fff
    style T7 fill:#166534,stroke:#22c55e,color:#fff
    style T8 fill:#7f1d1d,stroke:#ef4444,color:#fff
```

```mermaid
flowchart LR
    subgraph ml_pipeline["ml_training_pipeline (daily)"]
        M1[Snapshot<br/>Dataset] --> M2[Prepare<br/>Data]
        M2 --> M3[Train<br/>Model]
        M3 --> M4[Evaluate]
        M4 --> M5[Register<br/>Model]
        M5 --> M6[Generate<br/>Report]
    end
```

---

## Tech Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| **Ingestion** | Apache Kafka 3.9 | Durable event streaming with replay capability |
| **Processing** | Apache Spark 3.5 (PySpark) | Distributed processing with Structured Streaming |
| **Storage** | Apache Iceberg 1.7 + MinIO | Versioned tables with time-travel on S3-compatible storage |
| **Orchestration** | Apache Airflow 2.9 | DAG-based scheduling with quality gates |
| **ML** | PyTorch 2.11 (MPS) | Autoencoder anomaly detection on Apple Silicon GPU |
| **Backend** | FastAPI | High-performance REST API with auto-generated docs |
| **Frontend** | React 19 + Vite + Tailwind | Interactive SPA with React Flow graphs and Recharts |
| **Infrastructure** | Docker Compose | Container orchestration for Kafka, MinIO, Airflow |

---

## Project Structure

```
TAID/
├── docker-compose.yml              # Kafka + MinIO + Airflow containers
├── CLAUDE.md                       # Build blueprint and instructions
├── EXPLANATION.md                  # Complete knowledge base (all concepts explained)
│
├── ingestion/                      # Data producers
│   ├── fake_producer.py            # Generates telemetry → Kafka (50 devices, 5 metrics)
│   ├── test_consumer.py            # Verifies Kafka messages
│   └── schemas/telemetry.avsc      # Avro schema (event contract)
│
├── storage/                        # Iceberg configuration
│   └── catalog.py                  # SparkSession factory (Iceberg + MinIO)
│
├── processing/                     # Spark jobs
│   ├── stream_processor.py         # Kafka → validate → Iceberg (streaming)
│   ├── batch_etl.py                # Hourly aggregations (batch)
│   └── quality_checks.py           # 4 SQL-based data health checks
│
├── infra/airflow/dags/             # Airflow workflows
│   ├── telemetry_pipeline_dag.py   # Main pipeline (every 15 min)
│   └── ml_training_dag.py          # ML training (daily)
│
├── ml/                             # Machine learning
│   ├── dataset.py                  # Iceberg → pivot → normalize → DataLoaders
│   ├── model.py                    # Autoencoder (5→64→32→16→32→64→5)
│   ├── train.py                    # Training loop (MPS GPU + early stopping)
│   ├── evaluate.py                 # Reconstruction error → anomaly detection
│   └── experiments/                # Saved models + logs + evaluation reports
│
├── backend/                        # REST API
│   ├── main.py                     # FastAPI app with CORS
│   └── routers/
│       ├── datasets.py             # Table metadata endpoints
│       ├── lineage.py              # Pipeline graph endpoint
│       ├── ml_results.py           # Experiment results endpoints
│       └── health.py               # Service health endpoints
│
└── portal/                         # Web UI
    ├── vite.config.js              # Vite + React + Tailwind
    └── src/
        ├── App.jsx                 # Layout with sidebar navigation
        └── pages/
            ├── DataCatalog.jsx     # Browse datasets and schemas
            ├── LineageGraph.jsx    # Interactive pipeline diagram
            ├── PipelineHealth.jsx  # Service monitoring dashboard
            └── MLDashboard.jsx     # ML experiment results + curves
```

---

## Memory Management

This platform runs on a **16GB MacBook Air M4**. Docker is limited to 8GB.

| Scenario | What to Run | Est. RAM |
|----------|-------------|----------|
| Ingestion dev | Docker (Kafka + MinIO) + producer | ~3GB |
| Spark development | Docker (Kafka + MinIO) + PySpark | ~6GB |
| Airflow development | Docker (Kafka + MinIO + Airflow) | ~5GB |
| ML training | Stop Airflow, run PyTorch natively | ~4GB |
| Full stack demo | All Docker + FastAPI + React | ~7GB |

```bash
# Check what's using memory
docker stats --no-stream

# Start only what you need
docker compose up kafka minio -d          # Minimal
docker compose up kafka minio airflow -d  # With orchestration

# Nuclear option if things get slow
docker system prune -a --volumes
```

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | API health check |
| GET | `/api/datasets` | List all Iceberg tables |
| GET | `/api/datasets/{name}` | Table detail with schema |
| GET | `/api/datasets/{name}/snapshots` | Iceberg snapshot history |
| GET | `/api/datasets/{name}/sample` | Sample rows from table |
| GET | `/api/lineage` | Full pipeline graph (nodes + edges) |
| GET | `/api/ml/experiments` | List all ML experiments |
| GET | `/api/ml/experiments/{id}` | Experiment detail with losses |
| GET | `/api/ml/latest-model` | Latest trained model info |
| GET | `/api/health/pipelines` | Airflow DAG statuses |
| GET | `/api/health/kafka` | Live Kafka health check |
| GET | `/api/health/storage` | Table freshness indicators |

Full interactive docs at **http://localhost:8000/docs**

---

## Key Metrics

| Metric | Value |
|--------|-------|
| Clean events processed | 1,196 |
| Rejected events (dead letter) | 13 |
| Hourly aggregation rows | 69 |
| Model parameters | 5,973 |
| Training time (MPS GPU) | 4.4 seconds |
| Anomaly detection precision | 1.00 |
| Anomaly detection recall | 0.60 |
| Anomaly detection F1 | 0.75 |
| API endpoints | 11 |
| Portal pages | 4 |

---

## Troubleshooting

<details>
<summary><strong>Kafka won't start</strong></summary>

Check Docker has 8GB memory allocated. Run:
```bash
docker compose down -v
docker compose up kafka -d
docker compose logs kafka -f
```
</details>

<details>
<summary><strong>Spark out of memory</strong></summary>

Reduce driver memory and shuffle partitions in `storage/catalog.py`:
```python
.config("spark.driver.memory", "1g")
.config("spark.sql.shuffle.partitions", "4")
```
</details>

<details>
<summary><strong>PyTorch MPS not available</strong></summary>

Ensure you're running natively (not in Docker):
```bash
python -c "import torch; print(torch.backends.mps.is_available())"
pip install --upgrade torch
```
</details>

<details>
<summary><strong>CORS errors in browser</strong></summary>

Ensure FastAPI is running with CORS middleware allowing `http://localhost:5173`:
```bash
uvicorn backend.main:app --port 8000
```
</details>

<details>
<summary><strong>React can't reach API</strong></summary>

1. Verify FastAPI is running: `curl http://localhost:8000/health`
2. Check browser DevTools → Network tab for failed requests
3. Ensure CORS allows `http://localhost:5173`
</details>

---

## License

This project is built for educational purposes. All Apache projects (Kafka, Spark, Iceberg, Airflow) are licensed under the Apache License 2.0.

---

<div align="center">

**Built with Apache Kafka, Spark, Iceberg, Airflow, PyTorch, FastAPI, and React**

*A complete data + ML platform running on a single laptop*

</div>
