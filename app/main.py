from fastapi import FastAPI
from app.core.config import settings
from app.infra.db import Base, engine

app = FastAPI(title=settings.APP_NAME)


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)


from app.api.routes import router as api_router

app.include_router(api_router)

for mod in [
    "app.api.logs_routes",
    "app.api.artifacts_routes",
    "app.api.graph_routes",
    "app.api.ops_routes",
    "app.api.import_routes",
    "app.api.export_routes",
    "app.api.timeline_routes",
    "app.api.admin_routes",
]:
    try:
        m = __import__(mod, fromlist=["router"])
        app.include_router(getattr(m, "router"))
    except Exception:
        pass


@app.get("/health")
def health():
    return {"status": "ok", "app": settings.APP_NAME}
