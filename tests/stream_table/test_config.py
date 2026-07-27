"""Unit tests for the pipeline configuration module."""

import os
import tempfile
from pathlib import Path

import pytest

from olap_engine.config import (
    ConfigurationError,
    PipelineConfig,
    deserialize_pipeline_config,
    load_config,
    serialize_pipeline_config,
)


class TestPipelineConfigDefaults:
    """Tests that default configuration values are correct."""

    def test_default_kafka_settings(self):
        config = PipelineConfig()
        assert config.kafka_bootstrap_servers == "localhost:9092"
        assert config.fact_topic == "fact-events"
        assert config.dimension_topic == "dimension-updates"
        assert config.dead_letter_topic == "dead-letter"
        assert config.consumer_group == "olap-engine"

    def test_default_watermark_settings(self):
        config = PipelineConfig()
        assert config.allowed_lateness_sec == 10.0
        assert config.idle_timeout_sec == 30.0
        assert config.watermark_emit_interval_sec == 1.0

    def test_default_join_settings(self):
        config = PipelineConfig()
        assert config.join_key_column == "user_id"
        assert config.dimension_refresh_interval_sec == 5.0

    def test_default_window_settings(self):
        config = PipelineConfig()
        assert config.window_size_sec == 300
        assert config.slide_interval_sec == 1

    def test_default_duckdb_settings(self):
        config = PipelineConfig()
        assert config.duckdb_path == "analytics.duckdb"
        assert config.retention_days == 7

    def test_default_api_settings(self):
        config = PipelineConfig()
        assert config.api_host == "0.0.0.0"
        assert config.api_port == 8000
        assert config.max_concurrent_queries == 100
        assert config.query_timeout_sec == 30.0

    def test_config_is_frozen(self):
        config = PipelineConfig()
        with pytest.raises(Exception):
            config.api_port = 9000  # type: ignore


class TestSerializeDeserialize:
    """Tests for TOML serialization and deserialization."""

    def test_serialize_produces_valid_toml(self):
        config = PipelineConfig()
        toml_str = serialize_pipeline_config(config)
        assert isinstance(toml_str, str)
        assert "kafka_bootstrap_servers" in toml_str
        assert "localhost:9092" in toml_str

    def test_deserialize_roundtrip_with_defaults(self):
        config = PipelineConfig()
        toml_str = serialize_pipeline_config(config)
        restored = deserialize_pipeline_config(toml_str)
        assert restored == config

    def test_deserialize_roundtrip_with_custom_values(self):
        config = PipelineConfig(
            kafka_bootstrap_servers="broker1:9093,broker2:9093",
            fact_topic="my-events",
            window_size_sec=600,
            api_port=9000,
            allowed_lateness_sec=5.0,
        )
        toml_str = serialize_pipeline_config(config)
        restored = deserialize_pipeline_config(toml_str)
        assert restored == config

    def test_deserialize_invalid_toml_raises_error(self):
        with pytest.raises(ConfigurationError) as exc_info:
            deserialize_pipeline_config("not valid [[ toml {{")
        assert "toml" in exc_info.value.key

    def test_deserialize_ignores_unknown_keys(self):
        toml_str = serialize_pipeline_config(PipelineConfig())
        toml_str += '\nunknown_key = "hello"\n'
        config = deserialize_pipeline_config(toml_str)
        assert config == PipelineConfig()


class TestValidation:
    """Tests for fail-fast validation of configuration values."""

    def test_window_size_zero_raises(self):
        with pytest.raises(ConfigurationError) as exc_info:
            deserialize_pipeline_config('window_size_sec = 0\nslide_interval_sec = 1\n')
        assert "window_size_sec" in exc_info.value.key

    def test_window_size_negative_raises(self):
        with pytest.raises(ConfigurationError) as exc_info:
            deserialize_pipeline_config('window_size_sec = -10\n')
        assert "window_size_sec" in exc_info.value.key

    def test_slide_interval_zero_raises(self):
        with pytest.raises(ConfigurationError) as exc_info:
            deserialize_pipeline_config('slide_interval_sec = 0\n')
        assert "slide_interval_sec" in exc_info.value.key

    def test_allowed_lateness_negative_raises(self):
        with pytest.raises(ConfigurationError) as exc_info:
            deserialize_pipeline_config('allowed_lateness_sec = -1.0\n')
        assert "allowed_lateness_sec" in exc_info.value.key

    def test_api_port_zero_raises(self):
        with pytest.raises(ConfigurationError) as exc_info:
            deserialize_pipeline_config('api_port = 0\n')
        assert "api_port" in exc_info.value.key

    def test_api_port_too_high_raises(self):
        with pytest.raises(ConfigurationError) as exc_info:
            deserialize_pipeline_config('api_port = 70000\n')
        assert "api_port" in exc_info.value.key

    def test_empty_kafka_bootstrap_servers_raises(self):
        with pytest.raises(ConfigurationError) as exc_info:
            deserialize_pipeline_config('kafka_bootstrap_servers = ""\n')
        assert "kafka_bootstrap_servers" in exc_info.value.key

    def test_empty_fact_topic_raises(self):
        with pytest.raises(ConfigurationError) as exc_info:
            deserialize_pipeline_config('fact_topic = ""\n')
        assert "fact_topic" in exc_info.value.key

    def test_retention_days_zero_raises(self):
        with pytest.raises(ConfigurationError) as exc_info:
            deserialize_pipeline_config('retention_days = 0\n')
        assert "retention_days" in exc_info.value.key

    def test_window_size_less_than_slide_raises(self):
        with pytest.raises(ConfigurationError) as exc_info:
            deserialize_pipeline_config(
                'window_size_sec = 5\nslide_interval_sec = 10\n'
            )
        assert "window_size_sec" in exc_info.value.key

    def test_configuration_error_has_descriptive_message(self):
        with pytest.raises(ConfigurationError) as exc_info:
            deserialize_pipeline_config('api_port = -1\n')
        error = exc_info.value
        assert "api_port" in str(error)
        assert "positive" in str(error).lower() or "Must be" in str(error)


class TestLoadConfig:
    """Tests for loading configuration from env vars and TOML files."""

    def test_load_config_from_toml_file(self, tmp_path):
        config_file = tmp_path / "pipeline.toml"
        config = PipelineConfig(api_port=9999, fact_topic="custom-topic")
        config_file.write_text(serialize_pipeline_config(config), encoding="utf-8")

        loaded = load_config(config_path=config_file)
        assert loaded.api_port == 9999
        assert loaded.fact_topic == "custom-topic"

    def test_load_config_env_overrides_file(self, tmp_path, monkeypatch):
        config_file = tmp_path / "pipeline.toml"
        config = PipelineConfig(api_port=9999)
        config_file.write_text(serialize_pipeline_config(config), encoding="utf-8")

        monkeypatch.setenv("OLAP_API_PORT", "7777")
        loaded = load_config(config_path=config_file)
        assert loaded.api_port == 7777

    def test_load_config_env_var_string(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OLAP_KAFKA_BOOTSTRAP_SERVERS", "remote:9093")
        loaded = load_config(config_path=tmp_path / "nonexistent.toml")
        assert loaded.kafka_bootstrap_servers == "remote:9093"

    def test_load_config_env_var_float(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OLAP_ALLOWED_LATENESS_SEC", "20.5")
        loaded = load_config(config_path=tmp_path / "nonexistent.toml")
        assert loaded.allowed_lateness_sec == 20.5

    def test_load_config_env_var_invalid_int_raises(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OLAP_API_PORT", "not_a_number")
        with pytest.raises(ConfigurationError) as exc_info:
            load_config(config_path=tmp_path / "nonexistent.toml")
        assert "api_port" in exc_info.value.key

    def test_load_config_defaults_when_no_file_no_env(self, tmp_path):
        loaded = load_config(config_path=tmp_path / "nonexistent.toml")
        assert loaded == PipelineConfig()

    def test_load_config_invalid_toml_file_raises(self, tmp_path):
        config_file = tmp_path / "pipeline.toml"
        config_file.write_text("invalid [[ toml {{", encoding="utf-8")
        with pytest.raises(ConfigurationError):
            load_config(config_path=config_file)
