
import json
import datetime
import os
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any

@dataclass
class User:
    """User information."""
    id: str
    username: str
    display_name: str
    email: str
    role: str  # 'admin', 'analyst', 'viewer'
    created_at: str = field(default_factory=lambda: datetime.datetime.now().isoformat())
    last_login: Optional[str] = None
    settings: Dict = field(default_factory=dict)
    
    def to_dict(self):
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict):
        """Create a User instance from a dictionary."""
        return cls(**data)

class UserManager:
    """Manages users."""
    
    def __init__(self):
        self.users: Dict[str, User] = {}
    
    def add_user(self, user: User) -> bool:
        """Add a new user."""
        if user.id in self.users:
            return False
        
        self.users[user.id] = user
        return True
    
    def remove_user(self, user_id: str) -> bool:
        """Remove a user."""
        if user_id not in self.users:
            return False
        
        del self.users[user_id]
        return True
    
    def get_user(self, user_id: str) -> Optional[User]:
        """Get a user by ID."""
        return self.users.get(user_id)
    
    def get_user_by_username(self, username: str) -> Optional[User]:
        """Get a user by username."""
        for user in self.users.values():
            if user.username == username:
                return user
        return None
    
    def list_users(self) -> List[str]:
        """List all user IDs."""
        return list(self.users.keys())
    
    def update_last_login(self, user_id: str) -> bool:
        """Update a user's last login timestamp."""
        if user_id not in self.users:
            return False
        
        self.users[user_id].last_login = datetime.datetime.now().isoformat()
        return True
    
    def save_to_file(self, file_path: str) -> bool:
        """Save users to a file."""
        try:
            # Convert users to dicts
            users_data = {user_id: user.to_dict() for user_id, user in self.users.items()}
            
            with open(file_path, 'w') as f:
                json.dump(users_data, f)
            
            return True
        except Exception as e:
            print(f"Error saving users to file: {e}")
            return False
    
    @classmethod
    def load_from_file(cls, file_path: str) -> 'UserManager':
        """Load users from a file."""
        manager = cls()
        
        try:
            if not os.path.exists(file_path):
                return manager
            
            with open(file_path, 'r') as f:
                users_data = json.load(f)
            
            for user_id, data in users_data.items():
                user = User.from_dict(data)
                manager.add_user(user)
            
            return manager
        except Exception as e:
            print(f"Error loading users from file: {e}")
            return manager
