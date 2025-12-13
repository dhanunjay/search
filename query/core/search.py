from dataclasses import dataclass
from typing import List

from langchain_elasticsearch import DenseVectorStrategy, ElasticsearchStore
from langchain_google_genai import GoogleGenerativeAIEmbeddings

from core.retrieval import HybridSearcher


@dataclass
class SearchDocumentRequest:
    """
    Request model for performing a search query.

    Attributes:
        query (str): The text query string to search against the indexed documents.
        limt (int): The number of documents to return.
    """

    query: str
    limit: int

    class Config:
        frozen = True


@dataclass
class SearchResult:
    """
    Represents a search result

    Attributes:
        title (str): The title of the document.
        link (str): The link to the document source.
        snippet (str): The snippet for showing the context.
    """

    title: str
    link: str
    snippet: str


@dataclass
class SearchDocumentResponse:
    """
    Response model for search operations.

    Attributes:
        result (list[SearchResult]): A list of documents that matched the query.
    """

    result: list[SearchResult]

    class Config:
        frozen = True


class SearchService:
    """
    Service class responsible for querying documents stored in Elasticsearch
    using hybrid search (dense vector + BM25 keyword search).

    Attributes:
        es_url (str): URL of the Elasticsearch instance.
        index_name (str): Name of the index where documents are stored.
        embeddings (GoogleGenerativeAIEmbeddings): Embedding model used for semantic search.
        db (ElasticsearchStore): LangChain wrapper around Elasticsearch for vector storage and retrieval.
    """

    def __init__(
        self,
        es_url: str = "http://localhost:9200",
        index_name: str = "hybrid-search",
        api_key: str = None,
    ):
        """
        Initializes the SearchService with Elasticsearch connection and embedding model.

        Args:
            es_url (str): URL of the Elasticsearch instance.
            index_name (str): Name of the index to query.
            api_key (str): API key for Google Generative AI embeddings.
        """

        self._hybridSearcher = HybridSearcher(
            index_name=index_name, es_url=es_url
        )
        self._embeddings = GoogleGenerativeAIEmbeddings(
            model="text-embedding-004",
            google_api_key=api_key,
            task_type="RETRIEVAL_QUERY",
        )

    def search_documents(
        self, req: SearchDocumentRequest
    ) -> SearchDocumentResponse:
        """
        Executes a hybrid search against the indexed documents.

        Args:
            req (SearchDocumentRequest): The search request containing the query string.

        Returns:
            SearchDocumentResponse: A response containing the list of matching document contents.
        """
        query_vector = self._embeddings.embed_query(text=req.query, output_dimensionality=768, task_type="RETRIEVAL_QUERY")
        hits = self._hybridSearcher.hybrid_search_rrf(
            query=req.query,
            query_vector=query_vector,
            k=req.limit,
            num_candidates=req.limit,
        )

        result = []
        for hit in hits:
            metadata = hit.get("_source", {}).get("metadata", {})
            full_content = hit.get("_source", {}).get("text", "")
            link = metadata.get("source_uri", "")
            title = metadata.get("title", link)
            snippet = full_content[:200].replace("\n", " ") + "..."
            result.append(SearchResult(title=title, link=link, snippet=snippet))

        return SearchDocumentResponse(result=result)

