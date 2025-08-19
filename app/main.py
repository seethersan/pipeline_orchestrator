from fastapi import FastAPI
from app.core.config import settings
from app.infra.db import Base, engine

app = FastAPI(title=settings.APP_NAME)

@app.on_event("startup")
def on_startup():
    # Auto-create tables on first run (dev convenience)
    Base.metadata.create_all(bind=engine)

@app.get("/health")
def health():
    return {"status": "ok", "app": settings.APP_NAME}

@app.get("/version")
def version():
    return {"version": "0.1.0"}