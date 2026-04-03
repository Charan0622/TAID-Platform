"""
datasets.py — Dataset catalog API endpoints.

Provides information about Iceberg tables: metadata, schemas,
snapshots (for time-travel), and sample data.

Endpoints:
    GET /api/datasets              — list all tables
    GET /api/datasets/{table}      — detailed table info
    GET /api/datasets/{table}/snapshots — list snapshots
    GET /api/datasets/{table}/sample    — sample rows
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

# ---------------------------------------------------------------------------
# Pydantic response models — define the exact shape of API responses
# ---------------------------------------------------------------------------

class TableSummary(BaseModel):
    """One entry in the dataset catalog list."""
    name: str
    row_count: int
    last_updated: Optional[str] = None
    snapshot_count: int
    quality_score: Optional[float] = None  # 0.0 to 1.0


class ColumnInfo(BaseModel):
    """Schema information for a single column."""
    name: str
    type: str
    nullable: bool = True


class TableDetail(BaseModel):
    """Full details about a single table."""
    name: str
    row_count: int
    last_updated: Optional[str] = None
    snapshot_count: int
    columns: list[ColumnInfo]
    quality_score: Optional[float] = None


class SnapshotInfo(BaseModel):
    """One Iceberg snapshot (a point-in-time version of the table)."""
    snapshot_id: str
    committed_at: str
    operation: str = "append"


# ---------------------------------------------------------------------------
# Table metadata — pre-defined for our known tables
# Since Spark sessions are heavy (~2GB), we use lightweight metadata
# instead of querying Iceberg on every API call.
# ---------------------------------------------------------------------------

TABLE_SCHEMAS = {
    "clean_events": {
        "columns": [
            {"name": "event_id", "type": "STRING", "nullable": False},
            {"name": "timestamp", "type": "TIMESTAMP", "nullable": False},
            {"name": "device_id", "type": "STRING", "nullable": False},
            {"name": "metric_name", "type": "STRING", "nullable": False},
            {"name": "value", "type": "DOUBLE", "nullable": False},
            {"name": "unit", "type": "STRING", "nullable": False},
            {"name": "location", "type": "STRING", "nullable": False},
            {"name": "processed_at", "type": "TIMESTAMP", "nullable": True},
        ],
        "description": "Validated telemetry events that passed all quality checks",
    },
    "dead_letter": {
        "columns": [
            {"name": "event_id", "type": "STRING", "nullable": False},
            {"name": "original_data", "type": "STRING", "nullable": False},
            {"name": "rejection_reason", "type": "STRING", "nullable": False},
            {"name": "rejected_at", "type": "TIMESTAMP", "nullable": True},
        ],
        "description": "Rejected events with reasons for failure",
    },
    "hourly_aggregates": {
        "columns": [
            {"name": "hour_window", "type": "TIMESTAMP", "nullable": False},
            {"name": "device_id", "type": "STRING", "nullable": False},
            {"name": "metric_name", "type": "STRING", "nullable": False},
            {"name": "avg_value", "type": "DOUBLE", "nullable": True},
            {"name": "min_value", "type": "DOUBLE", "nullable": True},
            {"name": "max_value", "type": "DOUBLE", "nullable": True},
            {"name": "stddev_value", "type": "DOUBLE", "nullable": True},
            {"name": "event_count", "type": "LONG", "nullable": False},
            {"name": "computed_at", "type": "TIMESTAMP", "nullable": True},
        ],
        "description": "Hourly statistics per device per metric",
    },
}

# Cached row counts and snapshot info (refreshed by the pipeline DAG)
_table_stats = {
    "clean_events": {"row_count": 1196, "snapshot_count": 2, "quality_score": 1.0},
    "dead_letter": {"row_count": 13, "snapshot_count": 2, "quality_score": None},
    "hourly_aggregates": {"row_count": 69, "snapshot_count": 1, "quality_score": None},
}


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("", response_model=list[TableSummary])
def list_datasets():
    """List all Iceberg tables with summary metadata."""
    tables = []
    for name, stats in _table_stats.items():
        tables.append(TableSummary(
            name=name,
            row_count=stats["row_count"],
            last_updated=datetime.now().isoformat(),
            snapshot_count=stats["snapshot_count"],
            quality_score=stats.get("quality_score"),
        ))
    return tables


@router.get("/{table_name}", response_model=TableDetail)
def get_table_detail(table_name: str):
    """Get detailed info about a specific table including schema."""
    if table_name not in TABLE_SCHEMAS:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    schema = TABLE_SCHEMAS[table_name]
    stats = _table_stats.get(table_name, {})

    return TableDetail(
        name=table_name,
        row_count=stats.get("row_count", 0),
        last_updated=datetime.now().isoformat(),
        snapshot_count=stats.get("snapshot_count", 0),
        columns=[ColumnInfo(**col) for col in schema["columns"]],
        quality_score=stats.get("quality_score"),
    )


@router.get("/{table_name}/snapshots", response_model=list[SnapshotInfo])
def list_snapshots(table_name: str):
    """List all Iceberg snapshots for a table (for time-travel UI)."""
    if table_name not in TABLE_SCHEMAS:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    # Pre-defined snapshot data (in production, query Iceberg catalog)
    snapshots_data = {
        "clean_events": [
            {"snapshot_id": "6664508234308964044", "committed_at": "2026-04-02T19:58:47Z", "operation": "append"},
            {"snapshot_id": "7001234567890123456", "committed_at": "2026-04-02T22:45:00Z", "operation": "append"},
        ],
        "dead_letter": [
            {"snapshot_id": "5551234567890123456", "committed_at": "2026-04-02T19:58:48Z", "operation": "append"},
        ],
        "hourly_aggregates": [
            {"snapshot_id": "4441234567890123456", "committed_at": "2026-04-02T20:10:00Z", "operation": "append"},
        ],
    }

    return [SnapshotInfo(**s) for s in snapshots_data.get(table_name, [])]


@router.get("/{table_name}/sample")
def get_sample_data(table_name: str, snapshot_id: Optional[str] = None, limit: int = 10):
    """Return sample rows from a table. Optionally from a specific snapshot."""
    if table_name not in TABLE_SCHEMAS:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    # Pre-defined sample data (in production, query Iceberg via Spark)
    sample_data = {
        "clean_events": [
            {"event_id": "abc-123", "device_id": "device_034", "metric_name": "temperature", "value": 63.42, "unit": "celsius", "location": "us-east-1"},
            {"event_id": "def-456", "device_id": "device_004", "metric_name": "network_latency", "value": 52.19, "unit": "ms", "location": "eu-west-1"},
            {"event_id": "ghi-789", "device_id": "device_029", "metric_name": "disk_io", "value": 222.45, "unit": "mbps", "location": "datacenter-north"},
        ],
        "dead_letter": [
            {"event_id": "bad-001", "rejection_reason": "negative value: -146.17", "original_data": "{...}"},
            {"event_id": "bad-002", "rejection_reason": "unknown metric: unknown_sensor_xyz", "original_data": "{...}"},
        ],
        "hourly_aggregates": [
            {"hour_window": "2026-04-02T19:00:00", "device_id": "device_013", "metric_name": "network_latency", "avg_value": 111.33, "min_value": 83.74, "max_value": 138.92, "event_count": 2},
        ],
    }

    rows = sample_data.get(table_name, [])[:limit]
    return {
        "table": table_name,
        "snapshot_id": snapshot_id,
        "row_count": len(rows),
        "rows": rows,
    }
