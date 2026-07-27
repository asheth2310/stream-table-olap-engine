"""Schema validation utilities for event payloads and schema compatibility checks."""

import logging
from typing import Any

import jsonschema

from olap_engine.schemas.json_schema import FACT_EVENT_JSON_SCHEMA

logger = logging.getLogger(__name__)


def validate_event_payload(data: dict) -> bool:
    """Validate an event payload against the fact event JSON schema.

    Args:
        data: The event payload dict to validate.

    Returns:
        True if valid, False otherwise.
    """
    try:
        jsonschema.validate(instance=data, schema=FACT_EVENT_JSON_SCHEMA)
        return True
    except jsonschema.ValidationError as e:
        logger.debug("Event payload validation failed: %s", e.message)
        return False


def check_schema_compatibility(topic_schema: dict, expected_schema: dict) -> bool:
    """Check if a topic schema is compatible with the expected schema.

    Verifies that all required fields in the expected schema are present in the
    topic schema. Logs a warning if a mismatch is detected.

    Args:
        topic_schema: The schema reported by the topic (JSON Schema dict).
        expected_schema: The expected schema to compare against (JSON Schema dict).

    Returns:
        True if compatible, False if a mismatch is detected.
    """
    expected_required = set(expected_schema.get("required", []))
    topic_required = set(topic_schema.get("required", []))

    expected_properties = set(expected_schema.get("properties", {}).keys())
    topic_properties = set(topic_schema.get("properties", {}).keys())

    # Check required fields are present
    missing_required = expected_required - topic_required
    if missing_required:
        logger.warning(
            "Schema mismatch: topic schema is missing required fields: %s",
            sorted(missing_required),
        )
        return False

    # Check that expected properties exist in the topic schema
    missing_properties = expected_properties - topic_properties
    if missing_properties:
        logger.warning(
            "Schema mismatch: topic schema is missing properties: %s",
            sorted(missing_properties),
        )
        return False

    return True
