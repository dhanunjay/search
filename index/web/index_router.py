import asyncio
import logging

from fastapi import APIRouter, Depends, HTTPException, Request

from core.ingestion import IngestionService, RemoteFileNotSupportedError
from core.model import IndexDocumentRequest, IndexDocumentResponse

router = APIRouter(
    prefix="/v1/index/documents",  # pattern: /{version}/{service}/{resource}
    tags=["Index"],
)

log = logging.getLogger(__name__)


def get_ingestion_service(req: Request) -> IngestionService:
    return req.app.state.ingestion_service


@router.post("", response_model=IndexDocumentResponse)
async def index_document(
    request_body: IndexDocumentRequest,
    ings: IngestionService = Depends(get_ingestion_service),
):
    """
    Index a document into Elasticsearch.
    """
    try:
        # I/O bound workload: PDF loading, network calls to Elasticsearch, embedding model.
        # Run in a background thread to avoid blocking the event loop.
        resp = await asyncio.to_thread(ings.create_indexing_job, request_body)
        return resp

    except FileNotFoundError as e:
        # Map Python exception → HTTP 404
        raise HTTPException(
            status_code=404, detail=f"File not found: {e.filename or str(e)}"
        )
    except IsADirectoryError as e:
        raise HTTPException(
            status_code=404,
            detail=f"File path can't be directory: {e.filename or str(e)}",
        )
    except RemoteFileNotSupportedError as e:
        raise HTTPException(status_code=501, detail=f"{str(e)}")
    except Exception as e:
        # Catch-all for unexpected errors → HTTP 500
        log.error(f"Index_document: ", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"We encountered an unexpected error... Please try again shortly.",
        )
