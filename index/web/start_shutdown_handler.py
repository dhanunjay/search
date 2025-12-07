import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from core.indexer import IndexingAgent, IndexingWorker
from core.ingestion import IngestionService
from core.metadata import MetadataAgent, MetadataWorker
from core.model import custom_deserializer, custom_serializer
from db.db_manager import DBManager
from util import logger as log
from wal.kafka import KafkaReader, KafkaWriter

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "YOUR_GOOGLE_API_KEY")

# Indexing service configuration details
INDEX_JOBS_TOPIC = "index_jobs_topic"
IDX_KAFKA_CONSUMER_CONF = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "index_jobs_topic",
}

# Kafka producer
KAFKA_PRODUCER_CONF = {
    "bootstrap.servers": "localhost:9092",
}

# Metadata service configuration
META_JOBS_TOPIC = "db_jobs_topic"
META_KAFKA_CONSUMER_CONF = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "db_jobs_topic",
}
META_DATABASE_URL = (
    "postgresql+psycopg2://test:testpwd123@localhost:5432/hybridsearch"
)


@asynccontextmanager
async def lifespan_context_mgr(app: FastAPI):
    """
    Application startup and shutdown handler using async context manager.
    """
    # -------------------- STARTUP LOGIC (before 'yield') --------------------
    log.setup_logging()

    wal_writer = KafkaWriter(
        conf=KAFKA_PRODUCER_CONF, value_serializer=custom_serializer
    )
    wal_writer.__enter__()
    app.state.index_writer = wal_writer

    ingestion_service = IngestionService(
        kafka_writer=wal_writer, topic=META_JOBS_TOPIC
    )
    app.state.ingestion_service = ingestion_service

    # configure metadata worker and agent
    metadata_reader = KafkaReader(
        conf=META_KAFKA_CONSUMER_CONF,
        topic=META_JOBS_TOPIC,
        value_deserializer=custom_deserializer,
    )
    metadata_service = MetadataWorker(
        kafka_writer=wal_writer,
        topic=INDEX_JOBS_TOPIC,
        db_manager=DBManager(database_url=META_DATABASE_URL),
    )
    metadata_agent = MetadataAgent(
        kafka_reader=metadata_reader, metadata_worker=metadata_service
    )
    metadata_future_task = asyncio.to_thread(metadata_agent.run)
    metadata_reader_task = asyncio.create_task(metadata_future_task)

    # configure index worker and agent
    index_reader = KafkaReader(
        conf=IDX_KAFKA_CONSUMER_CONF,
        topic=INDEX_JOBS_TOPIC,
        value_deserializer=custom_deserializer,
    )
    app.state.index_reader = index_reader
    index_service = IndexingWorker(
        db_manager=DBManager(database_url=META_DATABASE_URL),
        api_key=GOOGLE_API_KEY,
    )
    indexing_agent = IndexingAgent(index_service, index_reader)
    future_task = asyncio.to_thread(indexing_agent.run)
    index_reader_task = asyncio.create_task(future_task)

    # Optional: Short sleep to verify startup
    await asyncio.sleep(0.1)

    # Yield control back to FastAPI to start accepting requests
    yield

    # -------------------- SHUTDOWN LOGIC (after 'yield') --------------------
    if metadata_agent:
        metadata_agent.stop()

    if metadata_future_task:
        metadata_reader_task.cancel()

    if indexing_agent:
        indexing_agent.stop()

    if index_reader_task:
        index_reader_task.cancel()

    if wal_writer:
        wal_writer.__exit__(None, None, None)
