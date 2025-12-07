from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from web.start_shutdown_handler import lifespan_context_mgr

from . import index_router

# Initialize the FastAPI application
app = FastAPI(
    title="Index Service",
    description="API for indexing PDF documents.",
    lifespan=lifespan_context_mgr,
)

origins = [
    "http://localhost:3000",  # Your Next.js frontend
    # "https://your-frontend-domain.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # List of allowed origins
    allow_credentials=True,  # Allow cookies/authorization headers to be sent
    allow_methods=["*"],  # Allow all methods (GET, POST, OPTIONS, etc.)
    allow_headers=[
        "*"
    ],  # Allow all headers (Content-Type, Authorization, etc.)
)

# 🔗 Include the defined index router
app.include_router(index_router.router)


@app.get("/health", tags=["Health Check"])
async def root():
    """
    Basic health check endpoint.
    """
    return {"status": "ok", "service": "Index Service"}
