"""Pipeline configuration with TOML serialization and fail-fast validation."""

from __future__ import annotations

import os
from dataclasses import dataclass, fields, asdict
from pathlib import Path
from typing import Any

import tomli
import tomli_w


class ConfigurationError(Exception):
    """Raised when pipeline configuration is missing or invalid."""

    def __init__(self, key: str, message: str) -> None:
        self.key = key
        super().__init__(f"Configuration error for '{key}': {message}")


@dataclass(frozen=True)
class PipelineConfig:
    """Complete pipeline configuration (serializable to TOML)."""

    # Kafka settings
    kafka_bootstrap_servers: str = "localhost:9092"
    fact_topic: str = "fact-events"
    dimension_topic: str = "dimension-updates"
    dead_letter_topic: str = "dead-letter"
    consumer_group: str = "olap-engine"

    # Watermark settings
    allowed_lateness_sec: float = 10.0
    idle_timeout_sec: float = 30.0
    watermark_emit_interval_sec: float = 1.0

    # Join settings
    join_key_column: str = "user_id"
    dimension_refresh_interval_sec: float = 5.0

    # Window settings
    window_size_sec: int = 300
    slide_interval_sec: int = 1

    # DuckDB settings
    duckdb_path: str = "analytics.duckdb"
    retention_days: int = 7

    # API settings
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    max_concurrent_queries: int = 100
    query_timeout_sec: float = 30.0


def serialize_pipeline_config(config: PipelineConfig) -> str:
    """Serialize pipeline configuration to TOML format.

    Args:
        config: A valid PipelineConfig instance.

    Returns:
        TOML-formatted string representing the configuration.
    """
    data = asdict(config)
    return tomli_w.dumps(data)


def deserialize_pipeline_config(toml_str: str) -> PipelineConfig:
    """Deserialize TOML string to PipelineConfig.

    Args:
        toml_str: A syntactically valid TOML string.

    Returns:
        A PipelineConfig instance populated from the TOML data.

    Raises:
        ConfigurationError: If the TOML contains invalid values.
    """
    try:
        data = tomli.loads(toml_str)
    except tomli.TOMLDecodeError as e:
        raise ConfigurationError("toml", f"Invalid TOML syntax: {e}") from e

    return _build_config_from_dict(data)


def load_config(config_path: str | Path | None = None) -> PipelineConfig:
    """Load pipeline configuration from environment variables or TOML file.

    Resolution order:
    1. Environment variables with OLAP_ prefix (e.g., OLAP_KAFKA_BOOTSTRAP_SERVERS)
    2. TOML file at `config_path` (defaults to `pipeline.toml` in current directory)
    3. Dataclass defaults

    Args:
        config_path: Optional path to TOML config file. Defaults to 'pipeline.toml'.

    Returns:
        A validated PipelineConfig instance.

    Raises:
        ConfigurationError: If a value is missing or invalid.
    """
    # Start with values from TOML file if it exists
    file_values: dict[str, Any] = {}
    toml_path = Path(config_path) if config_path else Path("pipeline.toml")
    if toml_path.exists():
        try:
            raw = toml_path.read_text(encoding="utf-8")
            file_values = tomli.loads(raw)
        except tomli.TOMLDecodeError as e:
            raise ConfigurationError("toml", f"Invalid TOML file '{toml_path}': {e}") from e

    # Override with environment variables (OLAP_ prefix)
    env_values: dict[str, Any] = {}
    for field in fields(PipelineConfig):
        env_key = f"OLAP_{field.name.upper()}"
        env_val = os.environ.get(env_key)
        if env_val is not None:
            env_values[field.name] = _coerce_value(field.name, env_val, field.type)

    # Merge: env vars take priority over file values
    merged = {**file_values, **env_values}

    return _build_config_from_dict(merged)


def _coerce_value(key: str, raw: str, type_hint: str) -> Any:
    """Coerce a string environment variable to the expected type.

    Raises:
        ConfigurationError: If the value cannot be coerced.
    """
    try:
        if type_hint == "int":
            return int(raw)
        elif type_hint == "float":
            return float(raw)
        elif type_hint == "str":
            return raw
        else:
            return raw
    except (ValueError, TypeError) as e:
        raise ConfigurationError(
            key, f"Cannot convert '{raw}' to {type_hint}: {e}"
        ) from e


def _build_config_from_dict(data: dict[str, Any]) -> PipelineConfig:
    """Build a PipelineConfig from a dict, applying fail-fast validation.

    Raises:
        ConfigurationError: If any value is invalid.
    """
    # Filter to only known fields
    known_fields = {f.name for f in fields(PipelineConfig)}
    filtered = {k: v for k, v in data.items() if k in known_fields}

    # Type coercion for values that may come from TOML (int vs float)
    for field in fields(PipelineConfig):
        if field.name in filtered:
            val = filtered[field.name]
            if field.type == "int" and isinstance(val, float):
                if val != int(val):
                    raise ConfigurationError(
                        field.name,
                        f"Expected integer but got float with fractional part: {val}",
                    )
                filtered[field.name] = int(val)
            elif field.type == "float" and isinstance(val, int):
                filtered[field.name] = float(val)

    config = PipelineConfig(**filtered)
    _validate_config(config)
    return config


def _validate_config(config: PipelineConfig) -> None:
    """Validate configuration values, raising ConfigurationError on failure."""
    # String fields must be non-empty
    string_fields = [
        "kafka_bootstrap_servers",
        "fact_topic",
        "dimension_topic",
        "dead_letter_topic",
        "consumer_group",
        "join_key_column",
        "duckdb_path",
        "api_host",
    ]
    for name in string_fields:
        val = getattr(config, name)
        if not val or not val.strip():
            raise ConfigurationError(name, "Must be a non-empty string")

    # Positive float fields
    positive_floats = [
        "allowed_lateness_sec",
        "idle_timeout_sec",
        "watermark_emit_interval_sec",
        "dimension_refresh_interval_sec",
        "query_timeout_sec",
    ]
    for name in positive_floats:
        val = getattr(config, name)
        if val <= 0:
            raise ConfigurationError(name, f"Must be positive, got {val}")

    # Positive integer fields
    positive_ints = [
        "window_size_sec",
        "slide_interval_sec",
        "retention_days",
        "api_port",
        "max_concurrent_queries",
    ]
    for name in positive_ints:
        val = getattr(config, name)
        if val <= 0:
            raise ConfigurationError(name, f"Must be positive, got {val}")

    # Window size must be greater than or equal to slide interval
    if config.window_size_sec < config.slide_interval_sec:
        raise ConfigurationError(
            "window_size_sec",
            f"Must be >= slide_interval_sec ({config.slide_interval_sec}), got {config.window_size_sec}",
        )

    # Port range
    if config.api_port > 65535:
        raise ConfigurationError(
            "api_port", f"Must be <= 65535, got {config.api_port}"
        )
