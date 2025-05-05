"""
Agentic BI Platform - Database Models
-------------------------------------
This module provides model classes for database connections, reports, and users.
"""

import sqlite3
import pandas as pd
import json
import datetime
import os
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any, Union

@dataclass
class Connection:
    """Database connection information."""
    name: str
    type: str
    path: str
    conn: Any = None
    schema: Dict = field(default_factory=dict)
    
    def to_dict(self):
        """Convert to dictionary, excluding connection object."""
        result = asdict(self)
        result.pop('conn')
        return result
    
    @classmethod
    def from_dict(cls, data: Dict):
        """Create a Connection instance from a dictionary."""
        # Recreate connection based on type
        conn_data = data.copy()
        
        if data['type'] == 'sqlite' and os.path.exists(data['path']):
            conn_data['conn'] = sqlite3.connect(data['path'])
        else:
            conn_data['conn'] = None
        
        return cls(**conn_data)

class Database:
    """Manages database connections."""
    
    def __init__(self):
        self.connections: Dict[str, Connection] = {}
    
    def add_connection(self, connection: Connection) -> bool:
        """Add a new database connection."""
        if connection.name in self.connections:
            return False
        
        self.connections[connection.name] = connection
        return True
    
    def remove_connection(self, name: str) -> bool:
        """Remove a database connection."""
        if name not in self.connections:
            return False
        
        # Close the connection if it exists
        conn = self.connections[name].conn
        if conn:
            try:
                conn.close()
            except:
                pass
        
        del self.connections[name]
        return True
    
    def get_connection(self, name: str) -> Optional[Connection]:
        """Get a connection by name."""
        return self.connections.get(name)
    
    def list_connections(self) -> List[str]:
        """List all connection names."""
        return list(self.connections.keys())
    
    def save_to_file(self, file_path: str) -> bool:
        """Save connections to a file."""
        try:
            # Convert connections to dicts
            connections_data = {name: conn.to_dict() for name, conn in self.connections.items()}
            
            with open(file_path, 'w') as f:
                json.dump(connections_data, f)
            
            return True
        except Exception as e:
            print(f"Error saving connections to file: {e}")
            return False
    
    @classmethod
    def load_from_file(cls, file_path: str) -> 'Database':
        """Load connections from a file."""
        db = cls()
        
        try:
            if not os.path.exists(file_path):
                return db
            
            with open(file_path, 'r') as f:
                connections_data = json.load(f)
            
            for name, data in connections_data.items():
                connection = Connection.from_dict(data)
                db.add_connection(connection)
            
            return db
        except Exception as e:
            print(f"Error loading connections from file: {e}")
            return db
    
    def close_all(self):
        """Close all database connections."""
        for name in list(self.connections.keys()):
            self.remove_connection(name)

@dataclass
class ReportConfig:
    """Configuration for a report."""
    visualization_type: str
    x_axis: Optional[str] = None
    y_axis: Optional[str] = None
    color: Optional[str] = None
    size: Optional[str] = None
    title: Optional[str] = None
    
    def to_dict(self):
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict):
        """Create a ReportConfig instance from a dictionary."""
        return cls(**data)

@dataclass
class Report:
    """A saved report."""
    id: str
    title: str
    description: str
    query: str
    sql_query: str
    data: List[Dict]
    config: ReportConfig
    database: str
    created_at: str = field(default_factory=lambda: datetime.datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.datetime.now().isoformat())
    created_by: Optional[str] = None
    goal: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    
    def to_dict(self):
        """Convert to dictionary."""
        result = asdict(self)
        result['config'] = self.config.to_dict()
        return result
    
    @classmethod
    def from_dict(cls, data: Dict):
        """Create a Report instance from a dictionary."""
        report_data = data.copy()
        
        # Convert config to ReportConfig
        if 'config' in report_data and isinstance(report_data['config'], dict):
            report_data['config'] = ReportConfig.from_dict(report_data['config'])
        
        return cls(**report_data)
    
    def update(self, **kwargs):
        """Update report attributes."""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
        
        self.updated_at = datetime.datetime.now().isoformat()

class ReportManager:
    """Manages reports."""
    
    def __init__(self):
        self.reports: Dict[str, Report] = {}
    
    def add_report(self, report: Report) -> bool:
        """Add a new report."""
        if report.id in self.reports:
            return False
        
        self.reports[report.id] = report
        return True
    
    def remove_report(self, report_id: str) -> bool:
        """Remove a report."""
        if report_id not in self.reports:
            return False
        
        del self.reports[report_id]
        return True
    
    def get_report(self, report_id: str) -> Optional[Report]:
        """Get a report by ID."""
        return self.reports.get(report_id)
    
    def list_reports(self) -> List[str]:
        """List all report IDs."""
        return list(self.reports.keys())
    
    def filter_reports(self, 
                       goal: Optional[str] = None, 
                       database: Optional[str] = None,
                       tags: Optional[List[str]] = None,
                       created_by: Optional[str] = None,
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> List[Report]:
        """Filter reports based on criteria."""
        filtered_reports = []
        
        for report in self.reports.values():
            # Check each filter criterion
            if goal and report.goal != goal:
                continue
            
            if database and report.database != database:
                continue
            
            if tags and not all(tag in report.tags for tag in tags):
                continue
            
            if created_by and report.created_by != created_by:
                continue
            
            if start_date and report.created_at < start_date:
                continue
            
            if end_date and report.created_at > end_date:
                continue
            
            filtered_reports.append(report)
        
        return filtered_reports
    
    def save_to_file(self, file_path: str) -> bool:
        """Save reports to a file."""
        try:
            # Convert reports to dicts
            reports_data = {report_id: report.to_dict() for report_id, report in self.reports.items()}
            
            with open(file_path, 'w') as f:
                json.dump(reports_data, f)
            
            return True
        except Exception as e:
            print(f"Error saving reports to file: {e}")
            return False
    
    @classmethod
    def load_from_file(cls, file_path: str) -> 'ReportManager':
        """Load reports from a file."""
        manager = cls()
        
        try:
            if not os.path.exists(file_path):
                return manager
            
            with open(file_path, 'r') as f:
                reports_data = json.load(f)
            
            for report_id, data in reports_data.items():
                report = Report.from_dict(data)
                manager.add_report(report)
            
            return manager
        except Exception as e:
            print(f"Error loading reports from file: {e}")
            return manager

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