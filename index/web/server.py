from fastapi import FastAPI

from web.start_shutdown_handler import lifespan_context_mgr

from . import index_router

# Initialize the FastAPI application
app = FastAPI(
    title="Index Service",
    description="API for indexing PDF documents.",
    lifespan=lifespan_context_mgr,
)

# 🔗 Include the defined index router
app.include_router(index_router.router)


@app.get("/health", tags=["Health Check"])
async def root():
    """
    Basic health check endpoint.
    """
    return {"status": "ok", "service": "Index Service"}
