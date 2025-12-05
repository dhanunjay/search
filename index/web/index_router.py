import asyncio
from fastapi import APIRouter, HTTPException

from core.index import IndexDocumentRequest, IndexDocumentResponse, IndexService

router = APIRouter(
    prefix="/v1/index/documents", # pattern: /{version}/{service}/{resource}
    tags=["Index"]
)

# TODO: hard coding api_key is against the best practices of 12 factor app. Kept it for faster inner loop.
index_service = IndexService(api_key="AIzaSyACiooyyR56PJ_Oztt3EDDQTjFQUuZgZjo")


@router.post("", response_model=IndexDocumentResponse)
async def index_document(request_body: IndexDocumentRequest):
    """
    Index a document into Elasticsearch.
    """
    try:
        # I/O bound workload: PDF loading, network calls to Elasticsearch, embedding model.
        # Run in a background thread to avoid blocking the event loop.
        resp = await asyncio.to_thread(index_service.index_document, request_body)
        return resp

    except FileNotFoundError as e:
        # Map Python exception → HTTP 404
        raise HTTPException(
            status_code=404,
            detail=f"File not found: {e.filename or str(e)}"
        )
    except IsADirectoryError as e:
        raise HTTPException(
            status_code=404,
            detail=f"File path can't be directory: {e.filename or str(e)}"
        )
    except Exception as e:
        # Catch-all for unexpected errors → HTTP 500
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}"
        )
