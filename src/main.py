"""
Punto de entrada principal de FastAPI — Platform
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.config import settings
from src.api.health import router as health_router
from src.api.events import router as events_router
from src.api.twins import router as twins_router
from src.api.pipeline import router_correlations, router_pipeline, router_admin

app = FastAPI(
    title=settings.app_name,
    description=(
        "Plataforma de mantenimiento predictivo basada en Event Sourcing. "
        "Un único event log inmutable es la fuente de verdad de todo el sistema."
    ),
    version=settings.app_version,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Registrar routers
app.include_router(health_router)
app.include_router(events_router)
app.include_router(twins_router)
app.include_router(router_correlations)
app.include_router(router_pipeline)
app.include_router(router_admin)


@app.get("/", tags=["Root"])
def root():
    return {
        "app": settings.app_name,
        "version": settings.app_version,
        "docs": "/docs",
        "health": "/health",
        "endpoints": [
            "GET  /health",
            "GET  /events",
            "GET  /events/{event_id}",
            "GET  /twins",
            "GET  /twins/{machine_id}",
            "GET  /twins/{machine_id}/at?timestamp=",
            "GET  /correlations",
            "POST /correlations/run",
            "POST /pipeline/run",
            "DELETE /admin/reset",
        ],
    }
