from fastapi import FastAPI

from . import search_router

# 💡 Initialize the FastAPI application
app = FastAPI(
    title="Index Service", description="API for searching PDF documents."
)

# 🔗 Include the defined search router
app.include_router(search_router.router)


@app.get("/health", tags=["Health Check"])
async def root():
    """
    Basic health check endpoint.
    """
    return {"status": "ok", "service": "Query Service"}
