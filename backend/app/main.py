"""
Enterprise CDC Platform: Control Plane Application Entrypoint
Wires all modular components following SOLID principles:
- Single Responsibility: app initialization, middleware, router mounts.
- Open/Closed: extensible via new router modules.
- Dependency Inversion: all services injected through dependencies.py.
"""

import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.app.core.config import settings
from backend.app.api.v1.health import router as health_router
from backend.app.api.v1.sources import router as sources_router
from backend.app.api.v1.connectors import router as connectors_router
from backend.app.api.v1.snapshots import router as snapshots_router
from backend.app.api.v1.simulations import router as simulations_router
from backend.app.api.v1.metrics import router as metrics_router

logging.basicConfig(level=settings.log_level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("cdc-control-plane")

def create_app() -> FastAPI:
    application = FastAPI(
        title=settings.app_name,
        description="Modular, SOLID-compliant asynchronous control plane orchestrating Debezium 2.7, Kafka Connect, and heterogeneous database sources.",
        version=settings.version,
    )

    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Register API v1 Routers
    api_prefix = "/api/v1"
    application.include_router(health_router, prefix=api_prefix)
    application.include_router(sources_router, prefix=api_prefix)
    application.include_router(connectors_router, prefix=api_prefix)
    application.include_router(snapshots_router, prefix=api_prefix)
    application.include_router(simulations_router, prefix=api_prefix)
    application.include_router(metrics_router, prefix=api_prefix)

    # Root route
    @application.get("/")
    async def root():
        return {
            "name": settings.app_name,
            "version": settings.version,
            "docs": "/docs",
            "api_v1": api_prefix
        }

    return application

app = create_app()
