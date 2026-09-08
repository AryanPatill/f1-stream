"""Security layer: constant-time auth, signed sessions, rate limits,
headers. Built before any route exists, so no endpoint is ever briefly
unprotected.
"""
from __future__ import annotations

import hmac
import secrets

from fastapi import HTTPException, Request, status
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from slowapi import Limiter
from slowapi.util import get_remote_address
from starlette.middleware.base import BaseHTTPMiddleware

from src.config import APP_API_KEY, SESSION_SECRET

SESSION_COOKIE = "f1stream_session"
SESSION_MAX_AGE = 8 * 60 * 60  # 8 hours
_SALT = "f1stream-session-v1"

limiter = Limiter(key_func=get_remote_address)


def require_configured() -> None:
    """Refuse to start unconfigured. Loud failure beats silent insecurity."""
    missing = []
    if not APP_API_KEY:
        missing.append("APP_API_KEY")
    if not SESSION_SECRET:
        missing.append("SESSION_SECRET")
    if missing:
        raise RuntimeError(
            f"Cannot start: {', '.join(missing)} not set in .env. "
            "Generate with: python -c \"import secrets; "
            "print(secrets.token_urlsafe(32))\""
        )
    if len(SESSION_SECRET) < 32:
        raise RuntimeError("SESSION_SECRET must be at least 32 characters.")


def _serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(SESSION_SECRET, salt=_SALT)


def verify_api_key(candidate: str) -> bool:
    """Constant-time comparison.

    `==` short-circuits on the first differing byte, so response time
    leaks how much of the key was correct. Over enough requests that
    recovers the key one byte at a time. compare_digest does not.
    """
    if not candidate or not APP_API_KEY:
        return False
    return hmac.compare_digest(candidate.encode(), APP_API_KEY.encode())


def issue_session() -> str:
    """Signed, timestamped token. Tamper-evident, not encrypted —
    it carries no secret, only proof that we issued it."""
    return _serializer().dumps({"v": 1})


def validate_session(token: str) -> bool:
    if not token:
        return False
    try:
        _serializer().loads(token, max_age=SESSION_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return False
    return True


async def require_session(request: Request) -> None:
    """Dependency for every mutating route."""
    token = request.cookies.get(SESSION_COOKIE, "")
    if not validate_session(token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated.",
        )


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Applied outermost, so headers wrap error responses too.

    CSP allows only same-origin scripts and styles: no inline script,
    no CDN. That constrains the frontend at step 19 by design.

    Exception: a per-request nonce is generated so specific inline
    scripts we control (Swagger's bootstrap) can be allowed individually.
    A nonce authorizes one script we wrote; 'unsafe-inline' would
    authorize every inline script including one an attacker injects.
    """

    async def dispatch(self, request: Request, call_next):
        # Fresh per request. A reused nonce is no better than no nonce.
        nonce = secrets.token_urlsafe(16)
        request.state.csp_nonce = nonce

        response = await call_next(request)
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            f"script-src 'self' 'nonce-{nonce}'; "
            "style-src 'self'; "
            "img-src 'self' data:; "
            "connect-src 'self'; "
            "frame-ancestors 'none'; "
            "base-uri 'self'; "
            "form-action 'self'"
        )
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Permissions-Policy"] = (
            "geolocation=(), microphone=(), camera=()"
        )
        return response