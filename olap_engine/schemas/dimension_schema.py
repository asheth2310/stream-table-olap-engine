"""Apache Arrow schema for the dimension table format."""

import pyarrow as pa

DIMENSION_TABLE_SCHEMA = pa.schema([
    pa.field("dimension_key", pa.string(), nullable=False),
    pa.field("attributes_json", pa.string(), nullable=False),
    pa.field("version", pa.int32(), nullable=False),
    pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("is_active", pa.bool_(), nullable=False),
])
