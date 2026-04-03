"""
lineage.py — Data lineage graph endpoint.

Returns the full data flow graph as nodes + edges JSON, ready for
visualization with React Flow in the frontend.

Each node represents a component (Kafka topic, Spark job, Iceberg table, ML model).
Each edge represents a data flow between components.

This is MANUALLY DEFINED metadata — we know our pipeline architecture.
In production systems, tools like Apache Atlas or OpenLineage auto-discover lineage.
"""

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class LineageNode(BaseModel):
    """A component in the data pipeline."""
    id: str
    label: str
    type: str       # source, processing, storage, ml, api
    description: str


class LineageEdge(BaseModel):
    """A data flow connection between two nodes."""
    source: str     # Node ID
    target: str     # Node ID
    label: str      # What transformation happens along this edge


class LineageGraph(BaseModel):
    """The complete lineage graph."""
    nodes: list[LineageNode]
    edges: list[LineageEdge]


# ---------------------------------------------------------------------------
# The lineage graph — our complete data pipeline
# ---------------------------------------------------------------------------

LINEAGE_GRAPH = LineageGraph(
    nodes=[
        # Sources
        LineageNode(
            id="fake_producer",
            label="Fake Producer",
            type="source",
            description="Generates simulated telemetry from 50 devices with 5% anomaly injection",
        ),
        LineageNode(
            id="kafka_raw",
            label="Kafka: telemetry.raw",
            type="source",
            description="Raw event stream — all events land here before processing",
        ),

        # Processing
        LineageNode(
            id="stream_processor",
            label="Stream Processor",
            type="processing",
            description="Spark Structured Streaming: validates events against 4 rules",
        ),
        LineageNode(
            id="batch_etl",
            label="Batch ETL",
            type="processing",
            description="Spark batch job: computes hourly aggregations (avg, min, max, stddev)",
        ),
        LineageNode(
            id="quality_checks",
            label="Quality Checks",
            type="processing",
            description="SQL-based data health checks: row count, nulls, ranges, duplicates",
        ),

        # Storage
        LineageNode(
            id="clean_events",
            label="Iceberg: clean_events",
            type="storage",
            description="Validated telemetry events — the trusted source of truth",
        ),
        LineageNode(
            id="dead_letter",
            label="Iceberg: dead_letter",
            type="storage",
            description="Rejected events with rejection reasons for investigation",
        ),
        LineageNode(
            id="hourly_agg",
            label="Iceberg: hourly_aggregates",
            type="storage",
            description="Hourly statistics per device per metric",
        ),

        # ML
        LineageNode(
            id="ml_dataset",
            label="ML Dataset Prep",
            type="ml",
            description="Pivot, normalize, time-split data for autoencoder training",
        ),
        LineageNode(
            id="autoencoder",
            label="Autoencoder Model",
            type="ml",
            description="PyTorch anomaly detector trained on normal telemetry patterns",
        ),

        # API
        LineageNode(
            id="fastapi",
            label="FastAPI Backend",
            type="api",
            description="REST API serving data to the React portal",
        ),
    ],
    edges=[
        LineageEdge(source="fake_producer", target="kafka_raw", label="JSON events via kafka-python"),
        LineageEdge(source="kafka_raw", target="stream_processor", label="Spark reads via Structured Streaming"),
        LineageEdge(source="stream_processor", target="clean_events", label="Valid events (passed 4 rules)"),
        LineageEdge(source="stream_processor", target="dead_letter", label="Invalid events + rejection reasons"),
        LineageEdge(source="clean_events", target="quality_checks", label="SQL validation queries"),
        LineageEdge(source="clean_events", target="batch_etl", label="Read for aggregation"),
        LineageEdge(source="batch_etl", target="hourly_agg", label="Hourly stats written"),
        LineageEdge(source="clean_events", target="ml_dataset", label="Load via Iceberg snapshot"),
        LineageEdge(source="ml_dataset", target="autoencoder", label="Train/val/test DataLoaders"),
        LineageEdge(source="clean_events", target="fastapi", label="Table metadata + sample data"),
        LineageEdge(source="hourly_agg", target="fastapi", label="Aggregation data"),
        LineageEdge(source="autoencoder", target="fastapi", label="Experiment results + metrics"),
    ],
)


@router.get("", response_model=LineageGraph)
def get_lineage():
    """Return the full data lineage graph as nodes + edges."""
    return LINEAGE_GRAPH
