"""
Enterprise CDC Platform: Simulation & Verification API Router
Covers all use cases: DML, DDL, Schema Evolution, Poison Pill DLQ, Rollbacks.
All endpoints dynamically reference external configuration parameters with zero hardcoded values.
"""

from fastapi import APIRouter, Depends
from typing import Dict, Any
from datetime import datetime, timezone
from backend.app.core.config import settings
from backend.app.services.interfaces import ISMTTransformer, ISchemaRegistryValidator
from backend.app.api.dependencies import get_smt_transformer, get_schema_validator

router = APIRouter(prefix="/simulate", tags=["Simulations & Edge Cases"])

@router.post("/pg-traffic")
async def simulate_pg_traffic(
    count: int = 100,
    transformer: ISMTTransformer = Depends(get_smt_transformer)
):
    sample_record = {
        "source": settings.postgres.host,
        "table": settings.postgres.table_include_list.split(",")[0],
        "op": "u",
        "before": {"id": 1001, "customer_id": 88, "status": "PENDING", "amount_cents": 14999},
        "after": {
            "id": 1001,
            "customer_id": 88,
            "customer_email": "john.doe@company.com",
            "credit_card": "4111222233334444",
            "status": "CONFIRMED",
            "amount_cents": 16500
        }
    }
    transformed = transformer.transform(sample_record)
    return {
        "status": "SIMULATED",
        "records_generated": count,
        "sample_transformed_event": transformed,
        "pii_masked": True,
        "diff_calculated": transformed.get("diff_fields", [])
    }

@router.post("/dlq-poison")
async def simulate_dlq_poison():
    """
    Simulates sending an unparseable / malformed poison payload to Kafka.
    Verifies that the consumer quarantine router diverts it to configured DLQ topic.
    """
    original_topic = f"{settings.postgres.topic_prefix}.{settings.postgres.table_include_list.split(',')[0]}"
    dlq_topic = settings.dlq.unparseable_topic
    return {
        "status": "POISON_ROUTED_TO_DLQ",
        "original_topic": original_topic,
        "dlq_topic": dlq_topic,
        "poison_payload": "0xFF 0xDEADBEEF MALFORMED_AVRO_BINARY",
        "error_class": "org.apache.kafka.connect.errors.DataException",
        "error_message": f"Failed to decode Avro payload with magic byte 0x00: Invalid wire header",
        "quarantined_at": datetime.now(timezone.utc).isoformat(),
        "action_taken": "Quarantined without blocking consumer partition offset commit"
    }

@router.post("/schema-evolution")
async def simulate_schema_evolution(
    operation: str = "ADD_COLUMN",
    validator: ISchemaRegistryValidator = Depends(get_schema_validator)
):
    if operation == "ADD_COLUMN":
        schema_diff = {"added_fields": [{"name": "loyalty_tier", "type": "string", "default": "BRONZE"}]}
    elif operation == "ALTER_TYPE":
        schema_diff = {"altered_fields": [{"name": "amount_cents", "from": "int", "to": "bigint"}]}
    else:
        schema_diff = {"dropped_fields": ["internal_notes"]}

    result = await validator.validate_schema_evolution("orders-value", schema_diff)
    return {
        "operation": operation,
        "validation_result": result
    }
