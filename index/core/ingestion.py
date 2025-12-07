import logging
import os
from urllib.parse import urlparse
from uuid import uuid4

from core.model import IndexDocumentJob, IndexDocumentRequest, IndexDocumentResponse
from wal.kafka import KafkaWriter

log = logging.getLogger(__name__)


class IngestionService:

    def __init__(self, kafka_writer: KafkaWriter, topic: str):
        self._kafka_writer = kafka_writer
        self._topic = topic

    def create_indexing_job(self, req: IndexDocumentRequest) -> IndexDocumentResponse:

        file_path = get_local_path_from_uri(req.source_url)
        if not file_path:
            raise ValueError("file_path is missing")
        if not os.path.exists(file_path):
            raise FileNotFoundError(file_path)
        if not os.path.isfile(file_path):
            raise IsADirectoryError(file_path)

        src_props = getattr(req, "source_properties", {}) or {}
        src_props["local_file_path"] = file_path

        job_id = str(uuid4())
        job = IndexDocumentJob(
            job_id=job_id,
            content_type=req.content_type,
            source_url=req.source_url,
            source_properties=src_props,
        )

        # Use the async context manager for clean lifecycle, even if used for one operation.
        self._kafka_writer.publish(topic=self._topic, key=job_id, value=job)

        log.info(f"Indexing Job Published: job={job} topic={self._topic}")

        return IndexDocumentResponse(
            job_id=job_id,
            indexing_status="JOB_SUBMITTED",
            metadata={"source_url": req.source_url},
        )


def get_local_path_from_uri(uri: str) -> str:
    """Extracts the local file path from a file:// URI."""
    try:
        parsed_uri = urlparse(uri)

        # Check if the scheme is actually 'file'
        if parsed_uri.scheme != "file":
            raise RemoteFileNotSupportedError(uri=uri)

        # The path needs to be unquoted (e.g., if it contains spaces like %20)
        local_path = os.path.abspath(os.path.join(parsed_uri.netloc, parsed_uri.path))

        # For typical file:////home/xyz/abc.txt (4 slashes), netloc is empty and path is /home/xyz/abc.txt
        # The join and abspath should handle this correctly on Linux/macOS.
        return local_path

    except Exception as e:
        print(f"Error parsing URI: {e}")
        return ""


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
