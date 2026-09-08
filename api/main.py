"""FastAPI application. Security middleware, auth, dataset, run and
stream routes, static frontend, locally served API docs.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from api import routes_auth, routes_datasets, routes_runs, routes_stream
from api.deps import pools
from api.schemas import HealthResponse
from api.security import (
    SESSION_COOKIE,
    SecurityHeadersMiddleware,
    limiter,
    require_configured,
    validate_session,
)
from src.config import ALLOWED_ORIGINS, PROJECT_ROOT

logger = logging.getLogger("f1stream")

WEB_DIR = PROJECT_ROOT / "web"
VENDOR_DIR = WEB_DIR / "vendor"


@asynccontextmanager
async def lifespan(app: FastAPI):
    require_configured()
    await pools.open()
    app.state.pools = pools
    logger.info("pools open, origins=%s", ALLOWED_ORIGINS)
    yield
    await pools.close()


app = FastAPI(
    title="F1-Stream",
    description="Out-of-order telemetry stream processor",
    version="0.1.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Content-Type", "X-API-Key"],
)

app.include_router(routes_auth.router)
app.include_router(routes_datasets.router)
app.include_router(routes_runs.router)
app.include_router(routes_stream.router)

# Mounted at /static, not /, so it cannot shadow the API routes above.
if WEB_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")
if VENDOR_DIR.is_dir():
    app.mount("/vendor", StaticFiles(directory=str(VENDOR_DIR)), name="vendor")


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception) -> JSONResponse:
    """Full detail to logs, generic body to the client."""
    logger.exception("unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error."},
    )


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html")


@app.get("/docs", include_in_schema=False)
async def docs(request: Request) -> HTMLResponse:
    """Swagger UI from local assets, bootstrap script carrying the
    per-request CSP nonce."""
    if not VENDOR_DIR.is_dir():
        return HTMLResponse(
            "<h1>Docs unavailable</h1><p>Vendor assets missing from "
            "web/vendor. See step 15b.</p>",
            status_code=503,
        )

    nonce = getattr(request.state, "csp_nonce", "")
    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>F1-Stream API</title>
  <link rel="stylesheet" href="/vendor/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="/vendor/swagger-ui-bundle.js"></script>
  <script nonce="{nonce}">
    window.ui = SwaggerUIBundle({{
      url: '/openapi.json',
      dom_id: '#swagger-ui',
      withCredentials: true
    }});
  </script>
</body>
</html>"""
    return HTMLResponse(html)


@app.get("/api/health", response_model=HealthResponse)
@limiter.limit("60/minute")
async def health(request: Request) -> HealthResponse:
    database = "unknown"
    try:
        async with request.app.state.pools.ro.acquire() as conn:
            await conn.fetchval("select 1")
        database = "ok"
    except Exception:
        logger.exception("health check: database unreachable")
        database = "unreachable"

    return HealthResponse(
        status="ok",
        database=database,
        authenticated=validate_session(request.cookies.get(SESSION_COOKIE, "")),
    )