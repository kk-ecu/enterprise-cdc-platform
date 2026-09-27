"""
Enterprise CDC Platform: Incremental Snapshots API Router
"""

from fastapi import APIRouter, Depends
from typing import Dict, Any
from backend.app.domain.models import IncrementalSnapshotRequest
from backend.app.services.interfaces import ISnapshotCoordinator
from backend.app.api.dependencies import get_snapshot_coordinator

router = APIRouter(prefix="/snapshots", tags=["DBlog Incremental Snapshots"])

@router.post("/incremental")
async def trigger_incremental_snapshot(
    req: IncrementalSnapshotRequest,
    coordinator: ISnapshotCoordinator = Depends(get_snapshot_coordinator)
):
    return await coordinator.trigger_incremental_snapshot(req)
