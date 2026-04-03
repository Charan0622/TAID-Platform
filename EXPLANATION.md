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
