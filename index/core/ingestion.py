import logging
import os
from uuid import uuid4

from core.model import IndexDocumentJob, IndexDocumentRequest, IndexDocumentResponse
from wal.kafka import KafkaWriter

log = logging.getLogger(__name__)


class IngestionService:

    def __init__(self, kafka_writer: KafkaWriter, topic: str):
        self._kafka_writer = kafka_writer
        self._topic = topic

    def create_indexing_job(self, req: IndexDocumentRequest) -> IndexDocumentResponse:
        file_path = req.source_properties.get("file_path")
        if not file_path:
            raise ValueError("file_path is missing")
        if not os.path.exists(file_path):
            raise FileNotFoundError(file_path)
        if not os.path.isfile(file_path):
            raise IsADirectoryError(file_path)

        job_id = str(uuid4())
        job = IndexDocumentJob(
            job_id=job_id,
            content_type=req.content_type,
            source_type=req.source_type,
            source_properties=req.source_properties,
        )

        # Use the async context manager for clean lifecycle, even if used for one operation.
        self._kafka_writer.publish(topic=self._topic, key=job_id, value=job)

        log.info(f"Indexing Job Published: job={job} topic={self._topic}")

        return IndexDocumentResponse(
            job_id=job_id,
            indexing_status="JOB_SUBMITTED",
            metadata={"source_type": req.source_type, "file_path": file_path},
        )
