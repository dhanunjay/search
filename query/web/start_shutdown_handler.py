from contextlib import asynccontextmanager

from fastapi import FastAPI

from util import logger as log


@asynccontextmanager
async def lifespan_context_mgr(app: FastAPI):
    """
    Application startup and shutdown handler using async context manager.
    """
    # -------------------- STARTUP LOGIC (before 'yield') --------------------
    log.setup_logging()

    # Yield control back to FastAPI to start accepting requests
    yield

    # -------------------- SHUTDOWN LOGIC (after 'yield') --------------------
