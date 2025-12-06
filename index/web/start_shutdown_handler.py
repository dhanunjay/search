import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from core.indexer import IndexingAgent, IndexingWorker
from core.ingestion import IngestionService
from core.model import custom_deserializer, custom_serializer
from util import logger as log
from wal.kafka import KafkaReader, KafkaWriter

GOOGLE_API_KEY = "AIzaSyACiooyyR56PJ_Oztt3EDDQTjFQUuZgZjo"

INDEX_JOBS_TOPIC = "index_jobs_topic"
KAFKA_CONSUMER_CONF = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "index_jobs_topic",
}

KAFKA_PRODUCER_CONF = {
    "bootstrap.servers": "localhost:9092",
}


@asynccontextmanager
async def lifespan_context_mgr(app: FastAPI):
    """
    Application startup and shutdown handler using async context manager.
    """
    # -------------------- STARTUP LOGIC (before 'yield') --------------------
    log.setup_logging()

    index_writer = KafkaWriter(
        conf=KAFKA_PRODUCER_CONF, value_serializer=custom_serializer
    )
    index_writer.__enter__()
    app.state.index_writer = index_writer

    ingestion_service = IngestionService(
        kafka_writer=index_writer, topic=INDEX_JOBS_TOPIC
    )
    app.state.ingestion_service = ingestion_service

    index_reader = KafkaReader(
        conf=KAFKA_CONSUMER_CONF,
        topic=INDEX_JOBS_TOPIC,
        value_deserializer=custom_deserializer,
    )
    app.state.index_reader = index_reader

    index_service = IndexingWorker(api_key=GOOGLE_API_KEY)
    indexing_agent = IndexingAgent(index_service, index_reader)
    future_task = asyncio.to_thread(indexing_agent.start)
    index_reader_task = asyncio.create_task(future_task)
    # Optional: Short sleep to verify startup
    await asyncio.sleep(0.1)

    # Yield control back to FastAPI to start accepting requests
    yield

    # -------------------- SHUTDOWN LOGIC (after 'yield') --------------------
    if indexing_agent:
        indexing_agent.stop()

    if index_reader_task:
        index_reader_task.cancel()

    if index_writer:
        index_writer.__exit__(None, None, None)
