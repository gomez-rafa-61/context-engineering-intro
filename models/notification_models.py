"""
Email notification data models for the monitoring framework.
"""

from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional, Dict, Any
from datetime import datetime
from .job_status import HealthAssessment, PlatformHealthSummary, NotificationPriority


class EmailRecipient(BaseModel):
    """Model for email recipients."""
    
    email: EmailStr = Field(..., description="Recipient email address")
    name: Optional[str] = Field(None, description="Recipient display name")
    type: str = Field("to", description="Recipient type: to, cc, bcc")
    
    class Config:
        json_schema_extra = {
            "example": {
                "email": "devops@company.com",
                "name": "DevOps Team",
                "type": "to"
            }
        }


class EmailTemplate(BaseModel):
    """Model for email templates."""
    
    template_id: str = Field(..., description="Unique template identifier")
    subject_template: str = Field(..., description="Email subject template with placeholders")
    body_template: str = Field(..., description="Email body template with placeholders")
    priority: NotificationPriority = Field(NotificationPriority.NORMAL, description="Default priority")
    
    class Config:
        json_schema_extra = {
            "example": {
                "template_id": "critical_alert",
                "subject_template": "🚨 Critical Alert: {platform} Pipeline Issues Detected",
                "body_template": "Dear Team,\n\nWe have detected critical issues...",
                "priority": "urgent"
            }
        }


class EmailNotification(BaseModel):
    """Model for email notification content."""
    
    notification_id: str = Field(..., description="Unique notification identifier")
    recipients: List[EmailRecipient] = Field(..., description="Email recipients")
    subject: str = Field(..., description="Email subject")
    body: str = Field(..., description="Email body content")
    priority: NotificationPriority = Field(NotificationPriority.NORMAL, description="Notification priority")
    attachments: List[str] = Field(default_factory=list, description="File paths for attachments")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")
    
    class Config:
        json_schema_extra = {
            "example": {
                "notification_id": "notif_20240115_103500",
                "recipients": [{
                    "email": "devops@company.com",
                    "name": "DevOps Team",
                    "type": "to"
                }],
                "subject": "Data Pipeline Monitoring Alert",
                "body": "Pipeline issues detected...",
                "priority": "high",
                "attachments": [],
                "metadata": {"monitoring_id": "mon_123"},
                "created_at": "2024-01-15T10:35:00Z"
            }
        }


class NotificationResult(BaseModel):
    """Model for notification delivery result."""
    
    notification_id: str = Field(..., description="Notification identifier")
    success: bool = Field(..., description="Whether notification was sent successfully")
    message_id: Optional[str] = Field(None, description="Email provider message ID")
    error_message: Optional[str] = Field(None, description="Error details if failed")
    sent_at: Optional[datetime] = Field(None, description="Timestamp when sent")
    recipients_count: int = Field(0, description="Number of recipients", ge=0)
    
    class Config:
        json_schema_extra = {
            "example": {
                "notification_id": "notif_20240115_103500",
                "success": True,
                "message_id": "msg_abc123",
                "error_message": None,
                "sent_at": "2024-01-15T10:36:00Z",
                "recipients_count": 3
            }
        }


class AlertConfiguration(BaseModel):
    """Model for alert configuration settings."""
    
    config_id: str = Field(..., description="Configuration identifier")
    enabled: bool = Field(True, description="Whether alerts are enabled")
    platforms: List[str] = Field(default_factory=list, description="Platforms to monitor")
    failure_threshold: int = Field(1, description="Number of failures before alert", ge=1)
    cooldown_minutes: int = Field(30, description="Minutes between duplicate alerts", ge=1)
    recipients: List[EmailRecipient] = Field(default_factory=list, description="Default recipients")
    escalation_rules: Dict[str, Any] = Field(default_factory=dict, description="Escalation configuration")
    
    class Config:
        json_schema_extra = {
            "example": {
                "config_id": "default_alerts",
                "enabled": True,
                "platforms": ["airbyte", "databricks"],
                "failure_threshold": 2,
                "cooldown_minutes": 60,
                "recipients": [],
                "escalation_rules": {
                    "critical": {"delay_minutes": 15},
                    "high": {"delay_minutes": 60}
                }
            }
        }


class MonitoringNotificationContext(BaseModel):
    """Model for the context needed to generate monitoring notifications."""
    
    monitoring_id: str = Field(..., description="Monitoring session ID")
    health_assessment: HealthAssessment = Field(..., description="Overall health assessment")
    platform_summaries: List[PlatformHealthSummary] = Field(
        default_factory=list,
        description="Per-platform health summaries"
    )
    failed_jobs_count: int = Field(0, description="Total number of failed jobs", ge=0)
    total_jobs_count: int = Field(0, description="Total number of jobs monitored", ge=0)
    critical_platforms: List[str] = Field(default_factory=list, description="Platforms with critical issues")
    monitoring_duration: Optional[int] = Field(None, description="Monitoring duration in seconds")
    
    @property
    def failure_rate(self) -> float:
        """Calculate overall failure rate as a percentage."""
        if self.total_jobs_count == 0:
            return 0.0
        return (self.failed_jobs_count / self.total_jobs_count) * 100
    
    class Config:
        json_schema_extra = {
            "example": {
                "monitoring_id": "mon_20240115_103500",
                "health_assessment": {},
                "platform_summaries": [],
                "failed_jobs_count": 3,
                "total_jobs_count": 25,
                "critical_platforms": ["databricks"],
                "monitoring_duration": 150
            }
        }


# ===============================================================================
# Email Template Constants
# ===============================================================================

DEFAULT_EMAIL_TEMPLATES = {
    "critical_alert": EmailTemplate(
        template_id="critical_alert",
        subject_template="🚨 CRITICAL: Data Pipeline Failures Detected - {failed_count} jobs failed",
        body_template="""
<html>
<body>
<h2>🚨 Critical Data Pipeline Alert</h2>

<p><strong>Alert Time:</strong> {timestamp}</p>
<p><strong>Monitoring Session:</strong> {monitoring_id}</p>

<h3>Summary</h3>
<ul>
    <li><strong>Failed Jobs:</strong> {failed_count} out of {total_count}</li>
    <li><strong>Failure Rate:</strong> {failure_rate:.1f}%</li>
    <li><strong>Risk Level:</strong> {risk_level}</li>
</ul>

<h3>Critical Platforms</h3>
{platform_details}

<h3>Recommended Actions</h3>
{recommendations}

<p><em>This is an automated alert from the Data Pipeline Monitoring System.</em></p>
</body>
</html>
        """,
        priority=NotificationPriority.URGENT
    ),
    
    "warning_alert": EmailTemplate(
        template_id="warning_alert",
        subject_template="⚠️ WARNING: Data Pipeline Issues Detected - {failed_count} jobs need attention",
        body_template="""
<html>
<body>
<h2>⚠️ Data Pipeline Warning Alert</h2>

<p><strong>Alert Time:</strong> {timestamp}</p>
<p><strong>Monitoring Session:</strong> {monitoring_id}</p>

<h3>Summary</h3>
<ul>
    <li><strong>Failed Jobs:</strong> {failed_count} out of {total_count}</li>
    <li><strong>Failure Rate:</strong> {failure_rate:.1f}%</li>
    <li><strong>Risk Level:</strong> {risk_level}</li>
</ul>

<h3>Platform Status</h3>
{platform_details}

<h3>Recommended Actions</h3>
{recommendations}

<p><em>This is an automated alert from the Data Pipeline Monitoring System.</em></p>
</body>
</html>
        """,
        priority=NotificationPriority.HIGH
    ),
    
    "info_summary": EmailTemplate(
        template_id="info_summary",
        subject_template="📊 Data Pipeline Status Summary - All systems operational",
        body_template="""
<html>
<body>
<h2>📊 Data Pipeline Status Summary</h2>

<p><strong>Report Time:</strong> {timestamp}</p>
<p><strong>Monitoring Session:</strong> {monitoring_id}</p>

<h3>Summary</h3>
<ul>
    <li><strong>Total Jobs Monitored:</strong> {total_count}</li>
    <li><strong>Successful Jobs:</strong> {success_count}</li>
    <li><strong>Failed Jobs:</strong> {failed_count}</li>
    <li><strong>Success Rate:</strong> {success_rate:.1f}%</li>
</ul>

<h3>Platform Status</h3>
{platform_details}

<p><em>This is an automated report from the Data Pipeline Monitoring System.</em></p>
</body>
</html>
        """,
        priority=NotificationPriority.NORMAL
    )
}