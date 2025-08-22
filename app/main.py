from fastapi import FastAPI
from starlette.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.infra.db import Base, engine
from app.core.auth import RateLimitMiddleware

app = FastAPI(title=settings.APP_NAME)
from app.core.logging import setup_logging

setup_logging()

# Rate limiting middleware (per-IP, per-path)
app.add_middleware(RateLimitMiddleware, max_per_minute=settings.RATE_LIMIT_PER_MINUTE)

# CORS (for Vite dev server / Nginx UI)
if settings.CORS_ALLOW_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ALLOW_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)


from app.api.routes import router as api_router

app.include_router(api_router)

for mod in [
    "app.api.logs_stream",
    "app.api.artifacts_routes",
    "app.api.graph_routes",
    "app.api.ops_routes",
    "app.api.import_routes",
    "app.api.export_routes",
    "app.api.timeline_routes",
    "app.api.admin_routes",
    "app.api.list_routes",
    "app.api.storage_routes",
    "app.api.stream_routes",
]:
    try:
        m = __import__(mod, fromlist=["router"])
        app.include_router(getattr(m, "router"))
    except Exception:
        pass


@app.get("/health")
def health():
    return {"status": "ok", "app": settings.APP_NAME}
