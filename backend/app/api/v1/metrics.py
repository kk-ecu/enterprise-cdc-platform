"""
Enterprise CDC Platform: Metrics & Observability API Router
"""

from fastapi import APIRouter, Depends
from backend.app.domain.models import GoldenSignalsMetrics
from backend.app.services.interfaces import IMetricsProvider
from backend.app.api.dependencies import get_metrics_provider

router = APIRouter(prefix="/metrics", tags=["SLO & Golden Signals"])

@router.get("/golden-signals", response_model=GoldenSignalsMetrics)
async def get_golden_signals(provider: IMetricsProvider = Depends(get_metrics_provider)):
    return await provider.collect_golden_signals()
