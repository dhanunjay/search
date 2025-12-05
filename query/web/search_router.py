import asyncio
from fastapi import APIRouter, Query

from core.search import SearchDocumentRequest, SearchDocumentResponse, SearchService

router = APIRouter(
    prefix="/v1/query/documents:search", # pattern /{version}/{service}/{resource}[:method_non_standard_http]
    tags=["Search"]
)

# TODO: hard coding api_key is against the best practices of 12 factor apps. Kept it for faster inner loop.
search_service = SearchService(api_key="AIzaSyACiooyyR56PJ_Oztt3EDDQTjFQUuZgZjo")


@router.get("", response_model=SearchDocumentResponse)
async def search_document(
    q: str = Query(..., description="The search query string"),
    limit: int = Query(20, description="Maximum number of results to return")
):
    """
    Search documents in Elasticsearch using query parameters.
    Example: GET /v1/query/search?query=hybrid+search&limit=20
    """
    # validate input
    if limit < 1:
        limit = 20
    request_obj = SearchDocumentRequest(query=q, limit=limit)

    # Our workload is IO bound: network calls to elastic, embedding model.
    # Schedule the task on a background thread (as opposed to background process)
    resp = await asyncio.to_thread(search_service.search_documents, request_obj)
    return resp
