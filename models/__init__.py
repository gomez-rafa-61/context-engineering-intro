"""Data models for the data pipeline monitoring framework."""

from .job_status import (
    JobStatus,
    PlatformType,
    RiskLevel,
    NotificationPriority,
    JobStatusRecord,
    HealthAssessment,
    PlatformHealthSummary,
    MonitoringResult,
)

from .notification_models import (
    EmailRecipient,
    EmailTemplate,
    EmailNotification,
    NotificationResult,
    AlertConfiguration,
    MonitoringNotificationContext,
    DEFAULT_EMAIL_TEMPLATES,
)

from .platform_models import (
    AirbyteJobResponse,
    AirbyteJobsListResponse,
    DatabricksJobRun,
    DatabricksJobRunsResponse,
    PowerAutomateFlowRun,
    PowerAutomateFlowRunsResponse,
    SnowflakeTaskHistory,
    APIErrorResponse,
    map_airbyte_status,
    map_databricks_status,
    map_powerautomate_status,
    map_snowflake_task_status,
)

__all__ = [
    # Core models
    "JobStatus",
    "PlatformType", 
    "RiskLevel",
    "NotificationPriority",
    "JobStatusRecord",
    "HealthAssessment",
    "PlatformHealthSummary",
    "MonitoringResult",
    
    # Notification models
    "EmailRecipient",
    "EmailTemplate",
    "EmailNotification",
    "NotificationResult",
    "AlertConfiguration",
    "MonitoringNotificationContext",
    "DEFAULT_EMAIL_TEMPLATES",
    
    # Platform models
    "AirbyteJobResponse",
    "AirbyteJobsListResponse",
    "DatabricksJobRun",
    "DatabricksJobRunsResponse",
    "PowerAutomateFlowRun",
    "PowerAutomateFlowRunsResponse",
    "SnowflakeTaskHistory",
    "APIErrorResponse",
    
    # Status mapping utilities
    "map_airbyte_status",
    "map_databricks_status",
    "map_powerautomate_status",
    "map_snowflake_task_status",
]