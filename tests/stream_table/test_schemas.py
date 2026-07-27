"""Tests for event and dimension schemas and validation utilities."""

import json
import uuid
from datetime import datetime, timezone

import pyarrow as pa
import pytest

from olap_engine.schemas import (
    DIMENSION_TABLE_SCHEMA,
    FACT_EVENT_JSON_SCHEMA,
    FACT_EVENT_SCHEMA,
    check_schema_compatibility,
    validate_event_payload,
)


class TestFactEventSchema:
    """Tests for the Apache Arrow fact event schema."""

    def test_schema_has_expected_fields(self):
        field_names = [f.name for f in FACT_EVENT_SCHEMA]
        assert field_names == [
            "event_id",
            "event_time",
            "ingest_time",
            "join_key",
            "payload_json",
            "source_topic",
            "partition",
            "offset",
        ]

    def test_schema_field_types(self):
        assert FACT_EVENT_SCHEMA.field("event_id").type == pa.string()
        assert FACT_EVENT_SCHEMA.field("event_time").type == pa.timestamp("us", tz="UTC")
        assert FACT_EVENT_SCHEMA.field("ingest_time").type == pa.timestamp("us", tz="UTC")
        assert FACT_EVENT_SCHEMA.field("join_key").type == pa.string()
        assert FACT_EVENT_SCHEMA.field("payload_json").type == pa.string()
        assert FACT_EVENT_SCHEMA.field("source_topic").type == pa.string()
        assert FACT_EVENT_SCHEMA.field("partition").type == pa.int32()
        assert FACT_EVENT_SCHEMA.field("offset").type == pa.int64()

    def test_all_fields_are_non_nullable(self):
        for field in FACT_EVENT_SCHEMA:
            assert field.nullable is False, f"Field {field.name} should be non-nullable"

    def test_can_create_record_batch_from_schema(self):
        now = datetime.now(timezone.utc)
        batch = pa.RecordBatch.from_pydict(
            {
                "event_id": [str(uuid.uuid4())],
                "event_time": [now],
                "ingest_time": [now],
                "join_key": ["user_123"],
                "payload_json": [json.dumps({"action": "click"})],
                "source_topic": ["fact-events"],
                "partition": [0],
                "offset": [42],
            },
            schema=FACT_EVENT_SCHEMA,
        )
        assert batch.num_rows == 1
        assert batch.schema.equals(FACT_EVENT_SCHEMA)


class TestDimensionTableSchema:
    """Tests for the Apache Arrow dimension table schema."""

    def test_schema_has_expected_fields(self):
        field_names = [f.name for f in DIMENSION_TABLE_SCHEMA]
        assert field_names == [
            "dimension_key",
            "attributes_json",
            "version",
            "updated_at",
            "is_active",
        ]

    def test_schema_field_types(self):
        assert DIMENSION_TABLE_SCHEMA.field("dimension_key").type == pa.string()
        assert DIMENSION_TABLE_SCHEMA.field("attributes_json").type == pa.string()
        assert DIMENSION_TABLE_SCHEMA.field("version").type == pa.int32()
        assert DIMENSION_TABLE_SCHEMA.field("updated_at").type == pa.timestamp("us", tz="UTC")
        assert DIMENSION_TABLE_SCHEMA.field("is_active").type == pa.bool_()

    def test_all_fields_are_non_nullable(self):
        for field in DIMENSION_TABLE_SCHEMA:
            assert field.nullable is False, f"Field {field.name} should be non-nullable"

    def test_can_create_record_batch_from_schema(self):
        now = datetime.now(timezone.utc)
        batch = pa.RecordBatch.from_pydict(
            {
                "dimension_key": ["user_123"],
                "attributes_json": [json.dumps({"name": "Alice", "region": "US"})],
                "version": [1],
                "updated_at": [now],
                "is_active": [True],
            },
            schema=DIMENSION_TABLE_SCHEMA,
        )
        assert batch.num_rows == 1
        assert batch.schema.equals(DIMENSION_TABLE_SCHEMA)


class TestFactEventJsonSchema:
    """Tests for the JSON Schema definition used for event validation."""

    def test_schema_structure(self):
        assert FACT_EVENT_JSON_SCHEMA["type"] == "object"
        assert set(FACT_EVENT_JSON_SCHEMA["required"]) == {
            "event_id",
            "event_time",
            "join_key",
            "payload",
        }

    def test_schema_property_types(self):
        props = FACT_EVENT_JSON_SCHEMA["properties"]
        assert props["event_id"]["type"] == "string"
        assert props["event_id"]["format"] == "uuid"
        assert props["event_time"]["type"] == "string"
        assert props["event_time"]["format"] == "date-time"
        assert props["join_key"]["type"] == "string"
        assert props["join_key"]["minLength"] == 1
        assert props["payload"]["type"] == "object"


class TestValidateEventPayload:
    """Tests for the validate_event_payload function."""

    def test_valid_event(self):
        event = {
            "event_id": str(uuid.uuid4()),
            "event_time": "2024-01-01T00:00:00Z",
            "join_key": "user_123",
            "payload": {"action": "click", "page": "/home"},
        }
        assert validate_event_payload(event) is True

    def test_missing_required_field(self):
        event = {
            "event_id": str(uuid.uuid4()),
            "event_time": "2024-01-01T00:00:00Z",
            # missing join_key
            "payload": {"action": "click"},
        }
        assert validate_event_payload(event) is False

    def test_empty_join_key_is_invalid(self):
        event = {
            "event_id": str(uuid.uuid4()),
            "event_time": "2024-01-01T00:00:00Z",
            "join_key": "",
            "payload": {"action": "click"},
        }
        assert validate_event_payload(event) is False

    def test_payload_must_be_object(self):
        event = {
            "event_id": str(uuid.uuid4()),
            "event_time": "2024-01-01T00:00:00Z",
            "join_key": "user_123",
            "payload": "not_an_object",
        }
        assert validate_event_payload(event) is False

    def test_extra_fields_are_allowed(self):
        event = {
            "event_id": str(uuid.uuid4()),
            "event_time": "2024-01-01T00:00:00Z",
            "join_key": "user_123",
            "payload": {"action": "click"},
            "extra_field": "extra_value",
        }
        assert validate_event_payload(event) is True


class TestCheckSchemaCompatibility:
    """Tests for the check_schema_compatibility function."""

    def test_compatible_schemas(self):
        topic_schema = {
            "type": "object",
            "required": ["event_id", "event_time", "join_key", "payload"],
            "properties": {
                "event_id": {"type": "string"},
                "event_time": {"type": "string"},
                "join_key": {"type": "string"},
                "payload": {"type": "object"},
            },
        }
        assert check_schema_compatibility(topic_schema, FACT_EVENT_JSON_SCHEMA) is True

    def test_topic_with_extra_fields_is_compatible(self):
        topic_schema = {
            "type": "object",
            "required": ["event_id", "event_time", "join_key", "payload", "source"],
            "properties": {
                "event_id": {"type": "string"},
                "event_time": {"type": "string"},
                "join_key": {"type": "string"},
                "payload": {"type": "object"},
                "source": {"type": "string"},
            },
        }
        assert check_schema_compatibility(topic_schema, FACT_EVENT_JSON_SCHEMA) is True

    def test_missing_required_field_is_incompatible(self):
        topic_schema = {
            "type": "object",
            "required": ["event_id", "event_time"],
            "properties": {
                "event_id": {"type": "string"},
                "event_time": {"type": "string"},
            },
        }
        assert check_schema_compatibility(topic_schema, FACT_EVENT_JSON_SCHEMA) is False

    def test_missing_property_is_incompatible(self):
        topic_schema = {
            "type": "object",
            "required": ["event_id", "event_time", "join_key", "payload"],
            "properties": {
                "event_id": {"type": "string"},
                "event_time": {"type": "string"},
                # missing join_key and payload properties
            },
        }
        assert check_schema_compatibility(topic_schema, FACT_EVENT_JSON_SCHEMA) is False

    def test_empty_schemas_are_compatible(self):
        assert check_schema_compatibility({}, {}) is True
