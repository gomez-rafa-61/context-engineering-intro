"""
Core job status data models for the data pipeline monitoring framework.
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


class JobStatus(str, Enum):
    """Enumeration of possible job statuses across all platforms."""
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PENDING = "pending"
    UNKNOWN = "unknown"


class PlatformType(str, Enum):
    """Enumeration of supported data platform types."""
    AIRBYTE = "airbyte"
    DATABRICKS = "databricks"
    POWER_AUTOMATE = "power_automate"
    SNOWFLAKE_TASK = "snowflake_task"


class RiskLevel(str, Enum):
    """Risk levels for health assessments."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class NotificationPriority(str, Enum):
    """Priority levels for notifications."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class JobStatusRecord(BaseModel):
    """Core job status record for Snowflake storage and processing."""
    
    job_id: str = Field(..., description="Platform-specific job identifier")
    platform: PlatformType = Field(..., description="Data platform type")
    job_name: str = Field(..., description="Human-readable job name")
    status: JobStatus = Field(..., description="Current job status")
    last_run_time: Optional[datetime] = Field(None, description="Last execution time")
    duration_seconds: Optional[int] = Field(None, description="Job duration in seconds", ge=0)
    error_message: Optional[str] = Field(None, description="Error details if failed")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Platform-specific metadata")
    checked_at: datetime = Field(default_factory=datetime.utcnow, description="Status check timestamp")
    
    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "job_id": "airbyte_123456",
                "platform": "airbyte",
                "job_name": "Customer Data Sync",
                "status": "success",
                "last_run_time": "2024-01-15T10:30:00Z",
                "duration_seconds": 300,
                "error_message": None,
                "metadata": {"connection_id": "conn_789", "stream_count": 5},
                "checked_at": "2024-01-15T10:35:00Z"
            }
        }


class HealthAssessment(BaseModel):
    """AI-generated health assessment of job status."""
    
    overall_health: str = Field(..., description="Overall health assessment description")
    risk_level: RiskLevel = Field(..., description="Risk level assessment")
    recommendations: List[str] = Field(default_factory=list, description="Recommended actions")
    requires_notification: bool = Field(False, description="Whether to send notification")
    notification_priority: NotificationPriority = Field(
        NotificationPriority.NORMAL, 
        description="Priority for notification"
    )
    assessment_timestamp: datetime = Field(
        default_factory=datetime.utcnow, 
        description="When the assessment was made"
    )
    jobs_analyzed: int = Field(0, description="Number of jobs analyzed", ge=0)
    failed_jobs_count: int = Field(0, description="Number of failed jobs", ge=0)
    critical_issues: List[str] = Field(default_factory=list, description="Critical issues identified")
    
    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "overall_health": "Good with minor issues detected",
                "risk_level": "MEDIUM",
                "recommendations": [
                    "Monitor Airbyte connection ABC - has failed twice",
                    "Consider increasing timeout for Databricks job XYZ"
                ],
                "requires_notification": True,
                "notification_priority": "normal",
                "assessment_timestamp": "2024-01-15T10:35:00Z",
                "jobs_analyzed": 23,
                "failed_jobs_count": 2,
                "critical_issues": ["High failure rate in customer data pipeline"]
            }
        }


class PlatformHealthSummary(BaseModel):
    """Health summary for a specific platform."""
    
    platform: PlatformType = Field(..., description="Platform type")
    total_jobs: int = Field(0, description="Total number of jobs", ge=0)
    successful_jobs: int = Field(0, description="Number of successful jobs", ge=0)
    failed_jobs: int = Field(0, description="Number of failed jobs", ge=0)
    running_jobs: int = Field(0, description="Number of running jobs", ge=0)
    platform_status: str = Field(..., description="Overall platform status")
    last_check: datetime = Field(default_factory=datetime.utcnow, description="Last check timestamp")
    issues: List[str] = Field(default_factory=list, description="Identified issues")
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as a percentage."""
        if self.total_jobs == 0:
            return 0.0
        return (self.successful_jobs / self.total_jobs) * 100
    
    @property
    def failure_rate(self) -> float:
        """Calculate failure rate as a percentage."""
        if self.total_jobs == 0:
            return 0.0
        return (self.failed_jobs / self.total_jobs) * 100
    
    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "platform": "airbyte",
                "total_jobs": 10,
                "successful_jobs": 8,
                "failed_jobs": 1,
                "running_jobs": 1,
                "platform_status": "Healthy with minor issues",
                "last_check": "2024-01-15T10:35:00Z",
                "issues": ["Connection timeout in stream ABC"]
            }
        }


class MonitoringResult(BaseModel):
    """Complete monitoring result across all platforms."""
    
    monitoring_id: str = Field(..., description="Unique monitoring session ID")
    started_at: datetime = Field(default_factory=datetime.utcnow, description="Monitoring start time")
    completed_at: Optional[datetime] = Field(None, description="Monitoring completion time")
    platform_summaries: List[PlatformHealthSummary] = Field(
        default_factory=list, 
        description="Per-platform health summaries"
    )
    overall_assessment: Optional[HealthAssessment] = Field(None, description="Overall health assessment")
    job_records: List[JobStatusRecord] = Field(
        default_factory=list, 
        description="All job status records collected"
    )
    errors: List[str] = Field(default_factory=list, description="Errors encountered during monitoring")
    
    @property
    def duration_seconds(self) -> Optional[int]:
        """Calculate monitoring duration in seconds."""
        if self.completed_at and self.started_at:
            return int((self.completed_at - self.started_at).total_seconds())
        return None
    
    @property
    def total_jobs_monitored(self) -> int:
        """Total number of jobs monitored across all platforms."""
        return len(self.job_records)
    
    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "monitoring_id": "mon_20240115_103500",
                "started_at": "2024-01-15T10:35:00Z",
                "completed_at": "2024-01-15T10:37:30Z",
                "platform_summaries": [],
                "overall_assessment": None,
                "job_records": [],
                "errors": []
            }
        }