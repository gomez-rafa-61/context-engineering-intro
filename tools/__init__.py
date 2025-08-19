"""API tools for data platform integrations."""

from .airbyte_api import (
    AirbyteAPIClient,
    AirbyteAPIError,
    get_airbyte_job_status,
    get_airbyte_connection_health,
)

from .databricks_api import (
    DatabricksAPIClient,
    DatabricksAPIError,
    get_databricks_job_status,
    get_databricks_cluster_health,
)

from .powerautomate_api import (
    PowerAutomateAPIClient,
    PowerAutomateAPIError,
    get_powerautomate_job_status,
)

from .snowflake_task_api import (
    SnowflakeTaskAPIClient,
    SnowflakeTaskAPIError,
    get_snowflake_task_status,
)

from .snowflake_db_api import (
    SnowflakeDBAPIClient,
    SnowflakeDBAPIError,
    store_job_status_records,
    store_monitoring_result,
)

from .outlook_api import (
    OutlookAPIClient,
    OutlookAPIError,
    send_notification_email,
    create_notification_draft,
)

__all__ = [
    # Airbyte
    "AirbyteAPIClient",
    "AirbyteAPIError", 
    "get_airbyte_job_status",
    "get_airbyte_connection_health",
    
    # Databricks
    "DatabricksAPIClient",
    "DatabricksAPIError",
    "get_databricks_job_status", 
    "get_databricks_cluster_health",
    
    # Power Automate
    "PowerAutomateAPIClient",
    "PowerAutomateAPIError",
    "get_powerautomate_job_status",
    
    # Snowflake Task
    "SnowflakeTaskAPIClient",
    "SnowflakeTaskAPIError",
    "get_snowflake_task_status",
    
    # Snowflake Database
    "SnowflakeDBAPIClient",
    "SnowflakeDBAPIError",
    "store_job_status_records",
    "store_monitoring_result",
    
    # Outlook
    "OutlookAPIClient",
    "OutlookAPIError",
    "send_notification_email",
    "create_notification_draft",
]