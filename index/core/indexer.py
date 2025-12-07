import logging
import os
import time
from typing import Any, List

from langchain_community.document_loaders import PyPDFLoader
from langchain_elasticsearch import DenseVectorStrategy, ElasticsearchStore
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter

from core.model import DocumentSource, IndexDocumentJob
from wal.kafka import KafkaMessageData, KafkaReader

log = logging.getLogger(__name__)


class IndexingWorker:
    """
    Service class responsible for indexing documents into Elasticsearch.

    This service:
    - Loads documents from a file path.
    - Splits documents into smaller chunks for efficient retrieval.
    - Embeds chunks using Google Generative AI embeddings.
    - Stores them in Elasticsearch with hybrid search enabled (dense vector + BM25).
    """

    def __init__(
        self,
        es_url: str = "http://localhost:9200",
        index_name: str = "hybrid-search",
        api_key: str = "YOUR_API_KEY",
    ):
        """
        Initializes the IndexService with Elasticsearch connection and embedding model.

        Args:
            es_url (str): URL of the Elasticsearch instance.
            index_name (str): Name of the index where documents will be stored.
            api_key (str): API key for Google Generative AI embeddings.
        """
        self.es_url = es_url
        self.index_name = index_name
        self.embeddings = GoogleGenerativeAIEmbeddings(
            model="models/gemini-embedding-001",
            google_api_key=api_key,
        )
        self.db = ElasticsearchStore(
            es_url=self.es_url,
            index_name=self.index_name,
            embedding=self.embeddings,
            strategy=DenseVectorStrategy(hybrid=True),
        )

    def index_document(self, req: IndexDocumentJob) -> None:
        """
        Indexes a document into Elasticsearch.

        Steps:
        1. Loads the document from the given file path.
        2. Splits the document into smaller chunks for efficient retrieval.
        3. Embeds the chunks using the configured embedding model.
        4. Adds the chunks to the Elasticsearch index.

        Args:
            req (IndexDocumentRequest): The request containing the file path to index.

        Returns:
            IndexDocumentResponse: Response containing job ID, status, and metadata.
        """

        doc_source = DocumentSource(uri=req.source_url)
        if not doc_source.is_validate_local_source():
            # File might have been deleted after we accepted the request or it might have been deleted and a folder with similar name was created
            log.error(
                "File is deleted or inaccessible. Drop the request and mark job failed"
            )
            return

        file_path = doc_source.get_local_path()
        loader = PyPDFLoader(file_path, extract_images=False, mode="single")
        docs = loader.load()

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=2048, chunk_overlap=64
        )
        chunks = text_splitter.split_documents(docs)

        # add correlation id for deduplication and correlating the chunks of the document
        for chunk in chunks:
            if not hasattr(chunk, "metadata"):
                chunk.metadata = {}
            chunk.metadata["correlation_id"] = req.job_id

        self.db.add_documents(chunks)
        log.info(
            f"Indexed document: file={file_path} chunks={len(chunks)} correlation_id={req.job_id}"
        )


class IndexingAgent:

    def __init__(self, index_worker: IndexingWorker, kafka_reader: KafkaReader):
        self._index_worker = index_worker
        self._kafka_reader = kafka_reader
        self._running = True
        self.CONSUME_TIMEOUT = 1.0
        self.IDLE_SLEEP = 1

    def __call__(self, batch: List[KafkaMessageData]) -> None:

        log.debug(f"IndexingAgent: Processing batch of {len(batch)} jobs")

        for msg in batch:
            try:
                # The file path is retrieved from the request structure
                log.debug(f"IndexingAgent: {msg.value}")
                self._index_worker.index_document(msg.value)

            except Exception as e:
                # BUG: if the indexing is failing repeatedly for a document we should store it in a dead topic for later examination (otherwise it is retried again and again)
                log.error(f"IndexingAgent: ", exc_info=True)
                raise

    def start(self) -> None:
        with self._kafka_reader as reader:
            log.info("IndexingAgent: agent started listening for messages")
            while self._running:
                try:
                    # PASS THE TIMEOUT: This is CRITICAL for shutdown
                    processed_count = reader.consume_one_batch(
                        callback=self.__call__, timeout=self.CONSUME_TIMEOUT
                    )

                    if processed_count == 0 and self._running:
                        # Sleep only if no messages were found AND we haven't been asked to stop
                        time.sleep(self.IDLE_SLEEP)

                except Exception as e:
                    # Log critical error in the consumer loop itself
                    log.error(f"IndexingAgent:", exc_info=True)
                    time.sleep(
                        self.IDLE_SLEEP * 5
                    )  # Sleep longer after an error

            log.info(f"IndexingAgent: agent shutdown")

    def stop(self) -> None:
        self._running = False
        log.info(f"IndexingAgent: agent shutdown requested")
