import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src import __version__
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


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "cardpulse", "version": __version__}
