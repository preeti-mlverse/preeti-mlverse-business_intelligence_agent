"""

# models/report.py
"""
import json
import datetime
import os
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any, Union

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

