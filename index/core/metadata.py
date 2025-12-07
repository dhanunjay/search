import hashlib
import logging
import time
from typing import List

from confluent_kafka import KafkaException
from langchain_community.document_loaders import PyPDFLoader
from sqlalchemy.exc import IntegrityError

from core.model import DocumentSource, IndexDocumentJob
from db.data_models import IndexStatusType
from db.db_manager import DBManager
from wal.kafka import KafkaMessageData, KafkaReader, KafkaWriter

log = logging.getLogger(__name__)


class MetadataWorker:
    """
    Implements the CRUD logic to save Document and DocumentMetadata using DBManager,
    including local file validation and hashing.
    """

    def __init__(
        self,
        kafka_writer: KafkaWriter,
        topic: str,
        db_manager: "DBManager",
    ):
        self._kafka_writer = kafka_writer
        self._topic = topic
        self._db_manager = db_manager

    def save_document(self, job: IndexDocumentJob) -> None:
        """
        Validates the local source file, computes its hash, and persists the
        Document and Metadata records.
        """

        doc_source = DocumentSource(
            uri=job.source_url, source_properties=job.source_properties
        )

        if not doc_source.is_validate_local_source():
            # File might be deleted, inaccessible, or a directory
            log.error(f"File is deleted or inaccessible for job {job.job_id}.")
            # Should we create an entry or log in dead topic for auditing?
            return

        file_path = doc_source.get_local_path()
        loader = PyPDFLoader(file_path, extract_images=False, mode="single")
        docs = loader.load()
        doc = docs[0]
        content_hash = hashlib.sha256(doc.page_content.encode()).hexdigest()

        try:
            self._db_manager.create_document(
                owner_id=1,  # hard code for now
                correlation_id=job.job_id,
                title=doc.metadata.get("title", "Unknown"),
                source_uri=job.source_url,
                content_hash=content_hash,
            )
        except IntegrityError as e:
            log.error(
                f"Integrity Violation for content_hash={content_hash} correlation_id={job.job_id}). Error: {e.orig}",
                exc_info=True,
            )
            self._mark_job_failed(job=job, content_hash=content_hash)
            return

        self._kafka_writer.publish(topic=self._topic, key=job.job_id, value=job)

    def _mark_job_failed(
        self, job: IndexDocumentJob, content_hash: str
    ) -> None:
        try:
            self._db_manager.create_document_metadata(
                correlation_id=job.job_id,
                content_hash=content_hash,
                new_status=IndexStatusType.FAILED,
            )
        except Exception as e:
            log.error(
                f"Failed to change the job status to failed for correlation_id={job.job_id}",
                exc_info=True,
            )


class MetadataAgent:
    """
    Interfaces with the Kafka reader to read a batch of jobs and persists them
    in the DB using MetadataWorker.
    """

    def __init__(
        self,
        kafka_reader: KafkaReader,
        metadata_worker: MetadataWorker,
        batch_size: int = 10,
        timeout: float = 1.0,
        poll_interval: float = 5.0,  # Time to wait between consuming loops if no messages found
    ):
        self._kafka_reader = kafka_reader
        self._metadata_worker = metadata_worker
        self._batch_size = batch_size
        self._timeout = timeout
        self._poll_interval = poll_interval
        self._running = True

    def _process_job_batch(self, batch: List[KafkaMessageData]) -> None:
        """
        Callback passed to KafkaReader.consume_one_batch.
        Processes deserialized messages and hands them to the worker.
        """
        for kafka_msg in batch:
            job_data = kafka_msg.value

            # 1. Ensure the deserialized value is a valid IndexDocumentJob
            if not isinstance(job_data, IndexDocumentJob):
                log.error(
                    f"Kafka value is not an IndexDocumentJob. Skipping message at offset {kafka_msg.offset}"
                )
                continue

            # 2. Persist the record via the worker
            try:
                self._metadata_worker.save_document(job_data)
            except Exception as e:
                # If persistence fails for one job, we log it, but the crucial
                # failure handling is done by re-raising, which causes
                # KafkaReader to NOT commit the offset for the entire batch.
                log.error(
                    f"Failed to save job {job_data.job_id} to DB. Re-raising to block commit.",
                    exc_info=True,
                )
                raise  # Re-raise to trigger the non-commit logic in KafkaReader

    def run(self) -> None:
        """
        Main run loop for the agent, calling Kafka consume in batches.
        """
        log.info("MetadataAgent: started listening for messages.")

        # Use the KafkaReader's context manager for safe consumer initialization/closing
        with self._kafka_reader as reader:
            while self._running:
                try:
                    # Call the batch API exposed by KafkaReader
                    messages_read = reader.consume_one_batch(
                        callback=self._process_job_batch,
                        batch_size=self._batch_size,
                        timeout=self._timeout,
                    )

                    if messages_read == 0 and self._running:
                        # If no messages were read, wait before polling again
                        time.sleep(self._poll_interval)

                except KafkaException as ke:
                    log.fatal(
                        f"FATAL Kafka error. Shutting down agent.",
                        exc_info=True,
                    )
                    break
                except Exception as e:
                    # Catch any unexpected error not related to callback failure (which is re-raised)
                    log.error(f"Unexpected error in run loop.", exc_info=True)
                    time.sleep(self._poll_interval)

    def stop(self) -> None:
        self._running = False
        log.info(f"MetadataAgent: agent shutdown requested")
