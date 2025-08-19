
from fastapi import FastAPI
from app.core.config import settings
from app.infra.db import Base, engine
from app.api.routes import router as api_router
from app.api.logs_routes import router as logs_router

app = FastAPI(title=settings.APP_NAME)

@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)

app.include_router(api_router)
app.include_router(logs_router)

@app.get("/health")
def health():
    return {"status": "ok", "app": settings.APP_NAME}

@app.get("/version")
def version():
    return {"version": "0.1.0"}
