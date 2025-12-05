import os
from typing import Any, Dict, Literal
from uuid import uuid4
from fastapi import Path
from pydantic import BaseModel
from langchain_elasticsearch import DenseVectorStrategy, ElasticsearchStore
from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_google_genai import GoogleGenerativeAIEmbeddings

from typing import Literal, Dict, Any
from pydantic import BaseModel, field_validator, model_validator

class IndexDocumentRequest(BaseModel):
    """
    Request model for indexing a document into Elasticsearch.

    Attributes:
        source_type (Literal["local_file"]): The type of source. Currently only 'local_file' is supported.
        content_type (Literal["application/pdf"]): MIME type of the content. Currently only 'application/pdf' is supported.
        source_properties (Dict[str, Any]): Properties specific to the source type.
            - For local_file: {"file_path": "/path/to/file.pdf"}
    """
    source_type: Literal["local_file"]
    
    content_type: Literal["application/pdf"]
    
    source_properties: Dict[str, Any]

    model_config = {
        "frozen": True
    }
    
    @model_validator(mode="after")
    def validate_source_properties(self) -> "IndexDocumentRequest":
        if self.source_type == "local_file":
            if "file_path" not in self.source_properties:
                raise ValueError("source_properties must include 'file_path' for local_file")
            
            if not self.source_properties.get("file_path"):
                 raise ValueError("file_path cannot be empty for local_file")
        return self
    

class IndexDocumentResponse(BaseModel):
    """
    Response model for indexing operations.

    Attributes:
        job_id (str): Unique identifier for the indexing job.
        indexing_status (str): Status of the indexing operation (e.g., 'completed').
        metadata (dict[str, Any]): Additional metadata such as file path and number of chunks indexed.
    """
    job_id: str
    indexing_status: str
    metadata: dict[str, Any]

    class Config:
        frozen = True


class IndexService:
    """
    Service class responsible for ingesting and indexing documents into Elasticsearch.

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

    def index_document(self, req: IndexDocumentRequest) -> IndexDocumentResponse:
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

        file_path = req.source_properties["file_path"]
        if not os.path.exists(file_path):
            raise FileNotFoundError(file_path)
        if not os.path.isfile(file_path):
            raise IsADirectoryError(file_path)
        
        loader = PyPDFLoader(str(file_path), extract_images=False, mode="single")
        docs = loader.load()

        text_splitter = RecursiveCharacterTextSplitter(chunk_size=2048, chunk_overlap=64)
        chunks = text_splitter.split_documents(docs)

        # add correlation id for deduplication and correlating the chunks of the document
        correlation_id = str(uuid4())
        for chunk in chunks:
            if not hasattr(chunk, "metadata"):
                chunk.metadata = {}
            chunk.metadata["correlation_id"] = correlation_id

        self.db.add_documents(chunks)

        return IndexDocumentResponse(
            job_id=str(uuid4()),
            indexing_status="completed",
            metadata={"file_path": req.source_properties.file_path, "chunks_indexed": len(chunks), "correlation_id": correlation_id},
        )
