"""
Enterprise CDC Platform: Database Sources API Router
Handles source registration and prerequisite discovery.
All default sources are populated from external configuration/properties files (settings)
with zero hardcoded hostnames, ports, credentials, or databases.
"""

from fastapi import APIRouter, Depends, HTTPException
from typing import List, Dict, Any
from backend.app.core.config import settings
from backend.app.domain.models import SourceRegistration, DatabaseType
from backend.app.services.interfaces import IPrerequisiteChecker
from backend.app.api.dependencies import get_prerequisite_checker

router = APIRouter(prefix="/sources", tags=["Database Sources"])

def _build_initial_sources() -> List[SourceRegistration]:
    env = settings.environment
    return [
        SourceRegistration(
            id=settings.postgres.id,
            type=DatabaseType.POSTGRES,
            host=settings.postgres.host,
            port=settings.postgres.port,
            database=settings.postgres.database,
            username=settings.postgres.username,
            environment=env
        ),
        SourceRegistration(
            id=settings.mysql.id,
            type=DatabaseType.MYSQL,
            host=settings.mysql.host,
            port=settings.mysql.port,
            database=settings.mysql.database,
            username=settings.mysql.username,
            environment=env
        ),
        SourceRegistration(
            id=settings.mariadb.id,
            type=DatabaseType.MARIADB,
            host=settings.mariadb.host,
            port=settings.mariadb.port,
            database=settings.mariadb.database,
            username=settings.mariadb.username,
            environment=env
        ),
        SourceRegistration(
            id=settings.mongodb.id,
            type=DatabaseType.MONGODB,
            host=settings.mongodb.host,
            port=settings.mongodb.port,
            database=settings.mongodb.database,
            username=settings.mongodb.username,
            environment=env
        ),
        SourceRegistration(
            id=settings.oracle.id,
            type=DatabaseType.ORACLE,
            host=settings.oracle.host,
            port=settings.oracle.port,
            database=settings.oracle.database,
            username=settings.oracle.username,
            environment=env
        ),
        SourceRegistration(
            id=settings.sqlserver.id,
            type=DatabaseType.SQLSERVER,
            host=settings.sqlserver.host,
            port=settings.sqlserver.port,
            database=settings.sqlserver.database,
            username=settings.sqlserver.username,
            environment=env
        )
    ]

# In-memory registry dynamically initialized from properties
_REGISTERED_SOURCES: List[SourceRegistration] = _build_initial_sources()

@router.get("", response_model=List[SourceRegistration])
async def list_sources():
    return _REGISTERED_SOURCES

@router.post("", response_model=SourceRegistration)
async def register_source(source: SourceRegistration):
    for s in _REGISTERED_SOURCES:
        if s.id == source.id:
            raise HTTPException(status_code=400, detail=f"Source ID '{source.id}' already registered.")
    _REGISTERED_SOURCES.append(source)
    return source

@router.get("/{source_id}/prerequisites")
async def verify_source_prerequisites(
    source_id: str,
    checker: IPrerequisiteChecker = Depends(get_prerequisite_checker)
):
    source = next((s for s in _REGISTERED_SOURCES if s.id == source_id), None)
    if not source:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found.")
    return await checker.verify_prerequisites(source)
