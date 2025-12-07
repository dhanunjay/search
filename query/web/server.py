from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import search_router

# 💡 Initialize the FastAPI application
app = FastAPI(
    title="Index Service", description="API for searching PDF documents."
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

# 🔗 Include the defined search router
app.include_router(search_router.router)


@app.get("/health", tags=["Health Check"])
async def root():
    """
    Basic health check endpoint.
    """
    return {"status": "ok", "service": "Query Service"}
