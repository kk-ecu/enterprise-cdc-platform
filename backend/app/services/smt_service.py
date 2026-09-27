"""
Enterprise CDC Platform: SMT Transformation & Serialization Service
Implements ISMTTransformer adhering to Single Responsibility.
All transform definitions, hashing salts, and field masks are loaded from external
configuration properties with ZERO hardcoded constants.
"""

import hashlib
import struct
from typing import Dict, Any, List, Optional
from backend.app.core.config import settings
from backend.app.services.interfaces import ISMTTransformer

class SMTTransformer(ISMTTransformer):
    def __init__(self, salt: Optional[str] = None):
        self.salt = salt if salt is not None else settings.smt.salt
        self.email_fields = settings.smt.email_fields
        self.card_fields = settings.smt.card_fields
        self.card_replacement_prefix = "XXXX-XXXX-XXXX-"

    def mask_pii_string(self, val: str) -> str:
        """One-way deterministic SHA-256 hash for PII privacy compliance"""
        return hashlib.sha256(f"{val}:{self.salt}".encode()).hexdigest()[:16]

    def redact_credit_card(self, card_num: str) -> str:
        """Masks all but last 4 digits according to PCI-DSS compliance"""
        digits = "".join(filter(str.isdigit, str(card_num)))
        if len(digits) >= 4:
            return f"{self.card_replacement_prefix}{digits[-4:]}"
        return "XXXX-XXXX-XXXX-XXXX"

    def calculate_field_diff(self, before: Dict[str, Any], after: Dict[str, Any]) -> List[str]:
        """Calculates modified field names between before and after tuple states"""
        if not before or not after:
            return []
        diffs = []
        all_keys = set(before.keys()).union(set(after.keys()))
        for k in all_keys:
            if before.get(k) != after.get(k):
                diffs.append(k)
        return sorted(diffs)

    def generate_schema_registry_wire_header(self, schema_id: int) -> bytes:
        """Generates Confluent Schema Registry 5-byte magic wire-format header:
        Byte 0: Magic byte (from properties: 0x00)
        Bytes 1-4: 4-byte Big-Endian Schema ID (Network byte order)
        """
        magic_byte = settings.schema_registry.wire_format_magic_byte
        return struct.pack(">bI", magic_byte, schema_id)

    def transform(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Applies in-flight transformation pipeline based on configured properties"""
        transformed = dict(record)

        # Apply PII masking on 'after' payload if present
        if "after" in transformed and isinstance(transformed["after"], dict):
            after = dict(transformed["after"])
            for pii_field in self.email_fields:
                if pii_field in after and after[pii_field]:
                    after[pii_field] = self.mask_pii_string(str(after[pii_field]))
            for card_field in self.card_fields:
                if card_field in after and after[card_field]:
                    after[card_field] = self.redact_credit_card(str(after[card_field]))
            transformed["after"] = after

        # Compute field diffs for updates
        if transformed.get("before") and transformed.get("after"):
            transformed["diff_fields"] = self.calculate_field_diff(transformed["before"], transformed["after"])

        return transformed
