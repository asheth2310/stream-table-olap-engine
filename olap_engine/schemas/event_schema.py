"""Apache Arrow schema for the fact event RecordBatch format."""

import pyarrow as pa

FACT_EVENT_SCHEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("ingest_time", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("join_key", pa.string(), nullable=False),
    pa.field("payload_json", pa.string(), nullable=False),  # JSON-serialized payload
    pa.field("source_topic", pa.string(), nullable=False),
    pa.field("partition", pa.int32(), nullable=False),
    pa.field("offset", pa.int64(), nullable=False),
])
