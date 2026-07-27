"""JSON Schema definitions for event payload validation at startup."""

FACT_EVENT_JSON_SCHEMA = {
    "type": "object",
    "required": ["event_id", "event_time", "join_key", "payload"],
    "properties": {
        "event_id": {"type": "string", "format": "uuid"},
        "event_time": {"type": "string", "format": "date-time"},
        "join_key": {"type": "string", "minLength": 1},
        "payload": {"type": "object"},
    },
}
