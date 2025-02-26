from enum import Enum, auto
from typing import Dict, List, Set, Optional
from dataclasses import dataclass
from functools import wraps

class Permission(Enum):
    READ = auto()
    WRITE = auto()
    DELETE = auto()
    ADMIN = auto()
    MATCH = auto()
    MERGE = auto()
    EXPORT = auto()
    AUDIT = auto()

@dataclass
class Role:
    name: str
    permissions: Set[Permission]
    description: str

class RBACManager:
    def __init__(self):
        self.roles: Dict[str, Role] = {}
        self.user_roles: Dict[str, List[str]] = {}
        self._initialize_default_roles()

    def _initialize_default_roles(self):
        """Initialize default system roles."""
        self.roles = {
            "admin": Role(
                name="admin",
                permissions=set(Permission),
                description="Full system access"
            ),
            "data_steward": Role(
                name="data_steward",
                permissions={Permission.READ, Permission.WRITE, Permission.MATCH, Permission.MERGE},
                description="Can manage and merge master data"
            ),
            "auditor": Role(
                name="auditor",
                permissions={Permission.READ, Permission.AUDIT},
                description="Can view and audit records"
            ),
            "viewer": Role(
                name="viewer",
                permissions={Permission.READ},
                description="Read-only access"
            )
        }

    def create_role(self, name: str, permissions: Set[Permission], description: str) -> Role:
        """Create a new role with specified permissions."""
        if name in self.roles:
            raise ValueError(f"Role {name} already exists")
        
        role = Role(name=name, permissions=permissions, description=description)
        self.roles[name] = role
        return role

    def assign_role(self, user_id: str, role_name: str) -> None:
        """Assign a role to a user."""
        if role_name not in self.roles:
            raise ValueError(f"Role {role_name} does not exist")
        
        if user_id not in self.user_roles:
            self.user_roles[user_id] = []
        
        if role_name not in self.user_roles[user_id]:
            self.user_roles[user_id].append(role_name)

    def remove_role(self, user_id: str, role_name: str) -> None:
        """Remove a role from a user."""
        if user_id in self.user_roles and role_name in self.user_roles[user_id]:
            self.user_roles[user_id].remove(role_name)

    def has_permission(self, user_id: str, permission: Permission) -> bool:
        """Check if a user has a specific permission."""
        if user_id not in self.user_roles:
            return False

        user_permissions = set()
        for role_name in self.user_roles[user_id]:
            if role_name in self.roles:
                user_permissions.update(self.roles[role_name].permissions)

        return permission in user_permissions

def require_permission(permission: Permission):
    """Decorator to check if user has required permission."""
    def decorator(func):
        @wraps(func)
        def wrapper(self, user_id: str, *args, **kwargs):
            if not self.rbac_manager.has_permission(user_id, permission):
                raise PermissionError(f"User {user_id} does not have {permission.name} permission")
            return func(self, user_id, *args, **kwargs)
        return wrapper
    return decorator
