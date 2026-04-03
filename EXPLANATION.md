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

> ⏳ **Status:** Not started
> 📝 Claude will fill this section after completing Phase 1

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

## Phase 2 — Kafka + Data Ingestion

> ⏳ **Status:** Not started
> 📝 Claude will fill this section after completing Phase 2

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
