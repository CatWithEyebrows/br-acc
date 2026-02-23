import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from icarus.config import settings
from icarus.dependencies import close_driver, init_driver
from icarus.middleware.cpf_masking import CPFMaskingMiddleware
from icarus.middleware.rate_limit import limiter
from icarus.routers import (
    auth,
    baseline,
    entity,
    graph,
    investigation,
    meta,
    patterns,
    search,
)
from icarus.services.neo4j_service import ensure_schema

_logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    if settings.jwt_secret_key == "change-me-in-production" or len(settings.jwt_secret_key) < 32:
        _logger.critical(
            "JWT secret is weak or default"
            " — set JWT_SECRET_KEY env var (>= 32 chars)"
        )
    driver = await init_driver()
    app.state.neo4j_driver = driver
    await ensure_schema(driver)
    yield
    await close_driver()


app = FastAPI(
    title="ICARUS API",
    description="Brazilian public data graph analysis tool",
    version="0.1.0",
    lifespan=lifespan,
    redirect_slashes=False,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]
app.add_middleware(SlowAPIMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins.split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(CPFMaskingMiddleware)

app.include_router(meta.router)
app.include_router(auth.router)
app.include_router(entity.router)
app.include_router(search.router)
app.include_router(graph.router)
app.include_router(patterns.router)
app.include_router(baseline.router)
app.include_router(investigation.router)
app.include_router(investigation.shared_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
