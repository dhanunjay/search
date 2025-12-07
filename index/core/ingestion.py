import logging
from uuid import uuid4

from core.model import (
    DocumentSource,
    IndexDocumentJob,
    IndexDocumentRequest,
    IndexDocumentResponse,
)
from wal.kafka import KafkaWriter

log = logging.getLogger(__name__)


class IngestionService:

    def __init__(self, kafka_writer: KafkaWriter, topic: str):
        self._kafka_writer = kafka_writer
        self._topic = topic

    def create_indexing_job(
        self, req: IndexDocumentRequest
    ) -> IndexDocumentResponse:

        doc_source = DocumentSource(
            uri=req.source_url, source_properties=req.source_properties
        )
        if not doc_source.is_validate_local_source():
            raise FileNotFoundError(req.source_url)

        file_path = doc_source.get_local_path()
        doc_source.update_properties("local_file_path", file_path)
        doc_source.update_properties("content_type", req.content_type)

        job_id = str(uuid4())
        job = IndexDocumentJob(
            job_id=job_id,
            content_type=req.content_type,
            source_url=req.source_url,
            source_properties=doc_source.source_properties,
        )

        # Use the async context manager for clean lifecycle, even if used for one operation.
        self._kafka_writer.publish(topic=self._topic, key=job_id, value=job)

        log.info(f"Indexing Job Published: job={job} topic={self._topic}")

        return IndexDocumentResponse(
            job_id=job_id,
            indexing_status="JOB_SUBMITTED",
            metadata={"source_url": req.source_url},
        )


class RemoteFileNotSupportedError(Exception):
    """
    Custom exception raised when an operation is restricted to local files,
    but a remote URI (e.g., http://, s3://) is provided.
    """

    def __init__(self, uri: str):
        self.uri = uri
        # The message passed to the base Exception class
        message = f"Operation is restricted to local files ('file://' scheme). Received: {uri}"
        super().__init__(message)
