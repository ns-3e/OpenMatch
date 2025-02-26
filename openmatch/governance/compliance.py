from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any
from enum import Enum
from dataclasses import dataclass
import re

class PrivacyRegulation(Enum):
    GDPR = "gdpr"
    CCPA = "ccpa"
    HIPAA = "hipaa"
    PIPEDA = "pipeda"
    LGPD = "lgpd"

class DataCategory(Enum):
    PII = "personally_identifiable_information"
    PHI = "protected_health_information"
    FINANCIAL = "financial_information"
    LOCATION = "location_data"
    BEHAVIORAL = "behavioral_data"
    BIOMETRIC = "biometric_data"

@dataclass
class RetentionPolicy:
    category: DataCategory
    retention_period: timedelta
    regulation: PrivacyRegulation
    requires_encryption: bool = False
    requires_masking: bool = False

class ComplianceManager:
    def __init__(self):
        self.retention_policies: Dict[DataCategory, RetentionPolicy] = {}
        self.pii_patterns: Dict[str, re.Pattern] = {}
        self._initialize_default_policies()
        self._initialize_pii_patterns()

    def _initialize_default_policies(self):
        """Initialize default retention policies based on common regulations."""
        self.retention_policies = {
            DataCategory.PII: RetentionPolicy(
                category=DataCategory.PII,
                retention_period=timedelta(days=365 * 2),  # 2 years
                regulation=PrivacyRegulation.GDPR,
                requires_encryption=True,
                requires_masking=True
            ),
            DataCategory.PHI: RetentionPolicy(
                category=DataCategory.PHI,
                retention_period=timedelta(days=365 * 6),  # 6 years (HIPAA)
                regulation=PrivacyRegulation.HIPAA,
                requires_encryption=True,
                requires_masking=True
            ),
            DataCategory.FINANCIAL: RetentionPolicy(
                category=DataCategory.FINANCIAL,
                retention_period=timedelta(days=365 * 7),  # 7 years
                regulation=PrivacyRegulation.GDPR,
                requires_encryption=True,
                requires_masking=True
            )
        }

    def _initialize_pii_patterns(self):
        """Initialize regex patterns for PII detection."""
        self.pii_patterns = {
            "email": re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"),
            "ssn": re.compile(r"\d{3}-\d{2}-\d{4}"),
            "credit_card": re.compile(r"\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}"),
            "phone": re.compile(r"\+?1?\d{9,15}"),
            "ip_address": re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b")
        }

    def detect_pii(self, data: Dict[str, Any]) -> Set[str]:
        """Detect PII fields in a data dictionary."""
        pii_fields = set()
        
        def check_value(value: Any, field_path: str):
            if isinstance(value, str):
                for pii_type, pattern in self.pii_patterns.items():
                    if pattern.search(value):
                        pii_fields.add(f"{field_path} ({pii_type})")
            elif isinstance(value, dict):
                for k, v in value.items():
                    check_value(v, f"{field_path}.{k}" if field_path else k)
            elif isinstance(value, list):
                for i, v in enumerate(value):
                    check_value(v, f"{field_path}[{i}]")

        check_value(data, "")
        return pii_fields

    def mask_pii(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Mask detected PII in the data."""
        def mask_value(value: Any) -> Any:
            if isinstance(value, str):
                masked = value
                for pattern in self.pii_patterns.values():
                    masked = pattern.sub("*" * 8, masked)
                return masked
            elif isinstance(value, dict):
                return {k: mask_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [mask_value(v) for v in value]
            return value

        return mask_value(data)

    def check_retention(self, data: Dict[str, Any], created_at: datetime) -> List[str]:
        """Check if data should be retained or deleted based on policies."""
        violations = []
        current_time = datetime.utcnow()
        
        for category, policy in self.retention_policies.items():
            if current_time - created_at > policy.retention_period:
                violations.append(
                    f"Data retention period exceeded for {category.value} "
                    f"under {policy.regulation.value}"
                )
        
        return violations

    def get_data_subject_rights(self, regulation: PrivacyRegulation) -> Dict[str, str]:
        """Get data subject rights for a specific regulation."""
        rights = {
            PrivacyRegulation.GDPR: {
                "right_to_access": "Subject can request access to their personal data",
                "right_to_rectification": "Subject can request correction of their data",
                "right_to_erasure": "Subject can request deletion of their data",
                "right_to_portability": "Subject can request their data in portable format",
                "right_to_object": "Subject can object to processing of their data"
            },
            PrivacyRegulation.CCPA: {
                "right_to_know": "Consumer can request categories of personal info collected",
                "right_to_delete": "Consumer can request deletion of personal information",
                "right_to_opt_out": "Consumer can opt-out of personal info sales",
                "right_to_non_discrimination": "Business cannot discriminate against consumer"
            }
        }
        return rights.get(regulation, {})
