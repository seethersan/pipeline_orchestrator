from __future__ import annotations
import time
from collections import deque, defaultdict
from typing import Deque, Dict
from fastapi import Header, HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from app.core.config import settings


# -------- API Key Dependency --------
async def require_api_key(
    x_api_key: str | None = Header(default=None), request: Request = None
):
    expected = settings.API_KEY
    if not expected:
        return
    provided = x_api_key or (request.query_params.get("api_key") if request else None)
    if not provided or provided != expected:
        raise HTTPException(
            status_code=401, detail="Unauthorized: invalid or missing API key"
        )


# -------- Rate Limiting Middleware --------
class RateLimitMiddleware(BaseHTTPMiddleware):
    """Per-IP, per-path, sliding-window limiter.
    - Reads limits and window dynamically from settings on each request.
    - Applies only to paths that start with any prefix in settings.RATE_LIMIT_PATHS (default ['/health']).
    - Keys buckets by (ip, matched_path_prefix) to avoid cross-path interference.
    """

    def __init__(self, app, max_per_minute: int):
        super().__init__(app)
        self._default = max_per_minute
        self.bucket: Dict[str, Deque[float]] = defaultdict(deque)

    async def dispatch(self, request: Request, call_next):
        try:
            max_per_minute = int(
                getattr(settings, "RATE_LIMIT_PER_MINUTE", self._default)
            )
        except Exception:
            max_per_minute = self._default

        window_seconds = int(getattr(settings, "RATE_LIMIT_WINDOW_SECONDS", 60))
        path_prefixes = getattr(settings, "RATE_LIMIT_PATHS", ["/health"])

        # If disabled or no matching path, bypass
        if not max_per_minute or max_per_minute <= 0:
            return await call_next(request)
        path = request.url.path or "/"
        matched = None
        for p in path_prefixes:
            if path.startswith(p):
                matched = p
                break
        if matched is None:
            return await call_next(request)

        # Identify client
        client_ip = (
            request.client.host
            if request.client
            else request.headers.get("x-forwarded-for", "local")
        )
        key = f"{client_ip}|{matched}"

        now = time.time()
        window_start = now - float(window_seconds)

        dq = self.bucket[key]
        while dq and dq[0] < window_start:
            dq.popleft()

        if len(dq) >= max_per_minute:
            from starlette.responses import JSONResponse

            return JSONResponse({"detail": "Too Many Requests"}, status_code=429)

        dq.append(now)
        return await call_next(request)
