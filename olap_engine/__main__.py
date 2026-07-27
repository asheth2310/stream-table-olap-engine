"""Entry point for the Stream-Table OLAP Engine.

Loads configuration, creates the pipeline, and starts the FastAPI server
with the pipeline running as a background task.
"""

from __future__ import annotations

import asyncio
import logging
import sys

import uvicorn

from olap_engine.config.pipeline_config import PipelineConfig, load_config
from olap_engine.pipeline import Pipeline
from olap_engine.api.app import app, set_store, set_pipeline

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


async def _start_pipeline(config: PipelineConfig) -> Pipeline:
    """Initialize and start the pipeline."""
    pipeline = Pipeline(config)

    # Wire the store and pipeline into the API module
    set_store(pipeline.store)
    set_pipeline(pipeline)

    await pipeline.start()
    return pipeline


def main() -> None:
    """Load config, create pipeline, start FastAPI with uvicorn."""
    logger.info("Stream-Table OLAP Engine starting...")

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        logger.error("Configuration error: %s", e)
        sys.exit(1)

    logger.info(
        "Configuration loaded: host=%s port=%d duckdb=%s",
        config.api_host,
        config.api_port,
        config.duckdb_path,
    )

    # Create an event loop for pipeline startup
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Start pipeline before uvicorn
    pipeline = loop.run_until_complete(_start_pipeline(config))

    # Run uvicorn (it manages its own event loop)
    try:
        uvicorn_config = uvicorn.Config(
            app=app,
            host=config.api_host,
            port=config.api_port,
            log_level="info",
            loop="asyncio",
        )
        server = uvicorn.Server(uvicorn_config)
        loop.run_until_complete(server.serve())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        loop.run_until_complete(pipeline.stop())
        loop.close()


if __name__ == "__main__":
    main()
