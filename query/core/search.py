from typing import List
from pydantic import BaseModel
from langchain_elasticsearch import DenseVectorStrategy, ElasticsearchStore
from langchain_google_genai import GoogleGenerativeAIEmbeddings


class SearchDocumentRequest(BaseModel):
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

class SearchResult(BaseModel):
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


class SearchDocumentResponse(BaseModel):
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
        api_key: str = "YOUR_API_KEY",
    ):
        """
        Initializes the SearchService with Elasticsearch connection and embedding model.

        Args:
            es_url (str): URL of the Elasticsearch instance.
            index_name (str): Name of the index to query.
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

    
    def search_documents(self, req: SearchDocumentRequest) -> SearchDocumentResponse:
        """
        Executes a hybrid search against the indexed documents.

        Args:
            req (SearchDocumentRequest): The search request containing the query string.

        Returns:
            SearchDocumentResponse: A response containing the list of matching document contents.
        """
        retriever = self.db.as_retriever(search_kwargs={"k": req.limit})
        docs = retriever.invoke(req.query)

        # Deduplicate based on correlation_id
        docs = self._deduplicate_docs(docs)

        result = []
        for doc in docs:
            title = doc.metadata.get("title", "Untitled")
            link = doc.metadata.get("source", "")
            snippet = doc.page_content[:200] + "..."
            result.append(SearchResult(title=title, link=link, snippet=snippet))

        return SearchDocumentResponse(result=result)

    
    def _deduplicate_docs(self, docs: List) -> List:
        """
        Private helper to eliminate duplicate documents based on correlation_id in metadata.

        Args:
            docs (List): List of document objects returned by retriever.

        Returns:
            List: Deduplicated list of documents.
        """
        seen = set()
        unique_docs = []
        for doc in docs:
            correlation_id = doc.metadata.get("correlation_id")
        if correlation_id not in seen:
            seen.add(correlation_id)
            unique_docs.append(doc)
            
        return unique_docs