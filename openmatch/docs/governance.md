# OpenMatch Governance Module Documentation

The OpenMatch Governance module provides comprehensive data governance capabilities, including Role-Based Access Control (RBAC), Audit Logging, and Data Compliance management. This document details the features and usage of each component.

## Table of Contents
1. [Role-Based Access Control (RBAC)](#role-based-access-control)
2. [Audit Logging](#audit-logging)
3. [Data Compliance](#data-compliance)

## Role-Based Access Control

The RBAC system manages user permissions and roles within OpenMatch.

### Permissions

Available permissions:
```python
from openmatch.governance import Permission

Permission.READ    # Read access to records
Permission.WRITE   # Write/create records
Permission.DELETE  # Delete records
Permission.ADMIN   # Administrative access
Permission.MATCH   # Execute matching operations
Permission.MERGE   # Merge records
Permission.EXPORT  # Export data
Permission.AUDIT   # Access audit logs
```

### Default Roles

The system comes with pre-configured roles:

1. **Admin**
   - Full system access
   - All permissions granted

2. **Data Steward**
   - Can manage and merge master data
   - Permissions: READ, WRITE, MATCH, MERGE

3. **Auditor**
   - Can view and audit records
   - Permissions: READ, AUDIT

4. **Viewer**
   - Read-only access
   - Permissions: READ

### Usage Examples

```python
from openmatch.governance import RBACManager

# Initialize RBAC
rbac = RBACManager()

# Create custom role
rbac.create_role(
    name="data_analyst",
    permissions={Permission.READ, Permission.EXPORT},
    description="Can read and export data"
)

# Assign role to user
rbac.assign_role(user_id="user123", role_name="data_analyst")

# Check permissions
has_access = rbac.has_permission(user_id="user123", permission=Permission.READ)

# Remove role
rbac.remove_role(user_id="user123", role_name="data_analyst")
```

### Using the Permission Decorator

```python
from openmatch.governance import require_permission

class DataService:
    def __init__(self):
        self.rbac_manager = RBACManager()
    
    @require_permission(Permission.WRITE)
    def create_record(self, user_id: str, data: dict):
        # Only executes if user has WRITE permission
        pass
```

## Audit Logging

The audit system tracks all significant operations within OpenMatch.

### Event Types

```python
from openmatch.governance import AuditEventType

AuditEventType.RECORD_CREATE  # Record creation
AuditEventType.RECORD_UPDATE  # Record updates
AuditEventType.RECORD_DELETE  # Record deletion
AuditEventType.RECORD_MERGE   # Record merging
AuditEventType.RECORD_MATCH   # Record matching
AuditEventType.ROLE_ASSIGN    # Role assignments
AuditEventType.ROLE_REMOVE    # Role removals
AuditEventType.EXPORT         # Data exports
AuditEventType.LOGIN          # User logins
AuditEventType.LOGOUT         # User logouts
```

### Usage Examples

```python
from openmatch.governance import AuditLogger, AuditEvent
from datetime import datetime

# Initialize logger
logger = AuditLogger(log_file="audit.log")

# Log an event
event = AuditEvent(
    event_type=AuditEventType.RECORD_CREATE,
    user_id="user123",
    timestamp=datetime.utcnow(),
    resource_id="record456",
    details={"action": "create_customer", "data": {"name": "John Doe"}},
    ip_address="192.168.1.1"
)
logger.log_event(event)
```

### Using the Audit Decorator

```python
from openmatch.governance import audit_action

class DataService:
    def __init__(self):
        self.audit_logger = AuditLogger()
    
    @audit_action(AuditEventType.RECORD_CREATE)
    def create_record(self, user_id: str, data: dict):
        # Operation will be automatically logged
        pass
```

## Data Compliance

The compliance system manages data privacy, retention, and regulatory requirements.

### Privacy Regulations

```python
from openmatch.governance import PrivacyRegulation

PrivacyRegulation.GDPR    # General Data Protection Regulation
PrivacyRegulation.CCPA    # California Consumer Privacy Act
PrivacyRegulation.HIPAA   # Health Insurance Portability and Accountability Act
PrivacyRegulation.PIPEDA  # Personal Information Protection and Electronic Documents Act
PrivacyRegulation.LGPD    # Lei Geral de Proteção de Dados
```

### Data Categories

```python
from openmatch.governance import DataCategory

DataCategory.PII         # Personally Identifiable Information
DataCategory.PHI         # Protected Health Information
DataCategory.FINANCIAL   # Financial Information
DataCategory.LOCATION    # Location Data
DataCategory.BEHAVIORAL  # Behavioral Data
DataCategory.BIOMETRIC   # Biometric Data
```

### Usage Examples

```python
from openmatch.governance import ComplianceManager
from datetime import datetime

# Initialize compliance manager
compliance = ComplianceManager()

# Detect PII in data
data = {
    "name": "John Doe",
    "email": "john@example.com",
    "ssn": "123-45-6789"
}
pii_fields = compliance.detect_pii(data)

# Mask PII
masked_data = compliance.mask_pii(data)

# Check retention requirements
violations = compliance.check_retention(
    data=data,
    created_at=datetime(2022, 1, 1)
)

# Get data subject rights
rights = compliance.get_data_subject_rights(PrivacyRegulation.GDPR)
```

### Default Retention Policies

The system includes pre-configured retention policies:

1. **PII Data**
   - Retention: 2 years
   - Regulation: GDPR
   - Requires encryption and masking

2. **PHI Data**
   - Retention: 6 years
   - Regulation: HIPAA
   - Requires encryption and masking

3. **Financial Data**
   - Retention: 7 years
   - Regulation: GDPR
   - Requires encryption and masking

### PII Detection Patterns

The system automatically detects:
- Email addresses
- Social Security Numbers
- Credit card numbers
- Phone numbers
- IP addresses

## Best Practices

1. **RBAC**
   - Follow principle of least privilege
   - Regularly audit role assignments
   - Use custom roles for specific needs
   - Implement role hierarchies for complex organizations

2. **Audit Logging**
   - Enable file-based logging in production
   - Implement log rotation
   - Regular log analysis
   - Backup audit logs securely

3. **Compliance**
   - Regular compliance audits
   - Update retention policies as regulations change
   - Implement data encryption at rest
   - Regular staff training on compliance requirements

## Security Considerations

1. **Access Control**
   - Regular permission reviews
   - Immediate role revocation for departed users
   - Audit failed access attempts
   - Implement session management

2. **Audit Trails**
   - Tamper-proof logging
   - Secure log storage
   - Regular log backups
   - Monitor suspicious patterns

3. **Data Protection**
   - Encrypt sensitive data
   - Implement data masking
   - Regular security assessments
   - Incident response planning 