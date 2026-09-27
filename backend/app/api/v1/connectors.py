"""
Enterprise CDC Platform: Connectors API Router
Manages Debezium connectors lifecycle via IConnectorManager.
"""

from fastapi import APIRouter, Depends, HTTPException
from typing import List, Dict, Any
from backend.app.domain.models import ConnectorConfig
from backend.app.services.interfaces import IConnectorManager
from backend.app.api.dependencies import get_connector_manager

router = APIRouter(prefix="/connectors", tags=["Debezium Connectors"])

@router.get("", response_model=List[str])
async def list_connectors(manager: IConnectorManager = Depends(get_connector_manager)):
    return await manager.list_connectors()

@router.post("")
async def create_connector(
    config: ConnectorConfig,
    manager: IConnectorManager = Depends(get_connector_manager)
):
    return await manager.create_or_update_connector(config)

@router.get("/{name}/status")
async def get_connector_status(
    name: str,
    manager: IConnectorManager = Depends(get_connector_manager)
):
    return await manager.get_connector_status(name)

@router.put("/{name}/pause")
async def pause_connector(
    name: str,
    manager: IConnectorManager = Depends(get_connector_manager)
):
    return await manager.pause_connector(name)

@router.put("/{name}/resume")
async def resume_connector(
    name: str,
    manager: IConnectorManager = Depends(get_connector_manager)
):
    return await manager.resume_connector(name)

@router.post("/{name}/restart")
async def restart_connector(
    name: str,
    manager: IConnectorManager = Depends(get_connector_manager)
):
    return await manager.restart_connector(name)
