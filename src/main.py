import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated

import psycopg
from fastapi import Depends, FastAPI, Request, Response
from fastapi.responses import JSONResponse

from src import __version__
from src.api.deps import check_database
from src.api.routes import router
from src.api.schemas import HealthResponse
from src.config import get_settings
from src.log import configure_logging

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    logger.info("CardPulse API starting (version %s)", __version__)
    yield


app = FastAPI(title="CardPulse", version=__version__, lifespan=lifespan)
app.include_router(router)


@app.exception_handler(psycopg.OperationalError)
async def database_unavailable(_request: Request, exc: psycopg.OperationalError) -> JSONResponse:
    # Log only the exception type: driver messages can contain host and database names.
    logger.warning("Database unavailable (%s)", type(exc).__name__)
    return JSONResponse(status_code=503, content={"detail": "Database unavailable"})


@app.get("/health", response_model=HealthResponse)
def health(
    response: Response, database_ok: Annotated[bool, Depends(check_database)]
) -> HealthResponse:
    if not database_ok:
        response.status_code = 503
    return HealthResponse(
        status="ok" if database_ok else "degraded",
        service="cardpulse",
        version=__version__,
        database="ok" if database_ok else "unavailable",
        timestamp=datetime.now(UTC),
    )
