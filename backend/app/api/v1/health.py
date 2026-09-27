"""
Enterprise CDC Platform: Health & Diagnostics API Router
"""

from fastapi import APIRouter
from backend.app.core.config import settings

router = APIRouter(prefix="/health", tags=["Health & Diagnostics"])

@router.get("")
async def get_health():
    return {
        "status": "HEALTHY",
        "service": settings.app_name,
        "version": settings.version,
        "environment": settings.environment,
        "kafka_connect": settings.connect_rest_url,
        "kafka_brokers": settings.kafka_bootstrap_servers,
        "schema_registry": settings.schema_registry_url,
        "memory_cap_mb": settings.memory_cap_mb
    }
