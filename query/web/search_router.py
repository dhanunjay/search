import asyncio
import logging
import os

from fastapi import APIRouter, HTTPException, Query

from core.search import (
    SearchDocumentRequest,
    SearchDocumentResponse,
    SearchService,
)

router = APIRouter(
    prefix="/v1/query/documents",  # pattern /{version}/{service}/{resource}[:method_non_standard_http]
    tags=["Search"],
)

SEARCH_RESULTS_MAX_LIMIT = 100
SEARCH_RESULTS_DEFAULT_LIMIT = 20

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "YOUR_GOOGLE_API_KEY")
search_service = SearchService(api_key=GOOGLE_API_KEY)


@router.get(":search", response_model=SearchDocumentResponse)
async def search_document(
    q: str = Query(..., description="The search query string"),
    limit: int = Query(
        SEARCH_RESULTS_DEFAULT_LIMIT,
        description="Maximum number of results to return",
    ),
):
    """
    Search documents in Elasticsearch using query parameters.
    Example: GET /v1/query/documents:search?q=hybrid+search&limit=20
    """
    # validate input
    if limit < 1:
        limit = 20
    limit = min(limit, SEARCH_RESULTS_MAX_LIMIT)

    # TODO: check q for injection errors if any; sanitize before using it
    request_obj = SearchDocumentRequest(query=q, limit=limit)

    try:
        # Our workload is IO bound: network calls to elastic, embedding model.
        # Schedule the task on a background thread (as opposed to background process)
        resp = await asyncio.to_thread(
            search_service.search_documents, request_obj
        )
        return resp

    except Exception as e:
        logging.error("Search query failed: ", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"We encountered an unexpected error... Please try again shortly.",
        )
