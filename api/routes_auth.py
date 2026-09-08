"""Authentication: exchange the API key for a signed session cookie.

The key is sent exactly once. After that the browser holds an httpOnly
cookie that JavaScript cannot read, so an XSS bug or a hostile extension
cannot exfiltrate the credential.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Request, Response, status
from fastapi.responses import JSONResponse

from api.schemas import AuthRequest, MessageResponse
from api.security import (
    SESSION_COOKIE,
    SESSION_MAX_AGE,
    issue_session,
    limiter,
    require_session,
    verify_api_key,
)

logger = logging.getLogger("f1stream")

router = APIRouter(prefix="/api/auth", tags=["auth"])

# secure=False because this runs over plain HTTP on localhost.
# Behind TLS this MUST become True, or the cookie travels in clear text.
COOKIE_SECURE = False


@router.post("/session", response_model=MessageResponse)
@limiter.limit("5/minute")
async def create_session(
    request: Request, payload: AuthRequest
) -> JSONResponse:
    """Trade a valid API key for a session cookie.

    Rate-limited to 5/minute per IP: with a 32-byte key, brute force was
    already infeasible, but the limit also caps a misconfigured client
    hammering the endpoint.
    """
    if not verify_api_key(payload.api_key):
        # Deliberately generic. "Wrong key" vs "malformed key" is free
        # information for anyone probing the endpoint.
        logger.warning(
            "failed auth attempt from %s",
            request.client.host if request.client else "unknown",
        )
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"message": "Authentication failed."},
        )

    response = JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": "Authenticated."},
    )
    response.set_cookie(
        key=SESSION_COOKIE,
        value=issue_session(),
        max_age=SESSION_MAX_AGE,
        httponly=True,      # JavaScript cannot read it
        samesite="strict",  # never sent cross-site: no CSRF
        secure=COOKIE_SECURE,
        path="/",
    )
    return response


@router.delete("/session", response_model=MessageResponse)
async def destroy_session(response: Response) -> MessageResponse:
    """Log out. The token stays valid until expiry — it is stateless —
    but the browser no longer holds it."""
    response.delete_cookie(
        key=SESSION_COOKIE,
        httponly=True,
        samesite="strict",
        secure=COOKIE_SECURE,
        path="/",
    )
    return MessageResponse(message="Signed out.")


@router.get(
    "/whoami",
    response_model=MessageResponse,
    dependencies=[Depends(require_session)],
)
async def whoami() -> MessageResponse:
    """Protected probe: 200 with a valid cookie, 401 without.

    Exists so the require_session dependency can be tested before any
    real protected route depends on it.
    """
    return MessageResponse(message="Session valid.")