"""
Platform-specific data models for API responses.
These models represent the raw API responses from each platform before conversion to JobStatusRecord.
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any


# ===============================================================================
# Airbyte Models
# ===============================================================================

class AirbyteJobResponse(BaseModel):
    """Model for Airbyte job API response."""
    
    job_id: str = Field(..., alias="jobId")
    config_id: str = Field(..., alias="configId")
    config_name: Optional[str] = Field(None, alias="configName")
    job_type: str = Field(..., alias="jobType")
    status: str = Field(...)
    created_at: Optional[str] = Field(None, alias="createdAt")
    updated_at: Optional[str] = Field(None, alias="updatedAt")
    started_at: Optional[str] = Field(None, alias="startedAt")
    ended_at: Optional[str] = Field(None, alias="endedAt")
    
    class Config:
        allow_population_by_field_name = True


class AirbyteJobsListResponse(BaseModel):
    """Model for Airbyte jobs list API response."""
    
    data: List[AirbyteJobResponse] = Field(default_factory=list)
    has_more: bool = Field(False, alias="hasMore")
    next: Optional[str] = Field(None)
    
    class Config:
        allow_population_by_field_name = True


class AirbyteConnectionResponse(BaseModel):
    """Model for Airbyte connection API response."""
    
    connection_id: str = Field(..., alias="connectionId")
    name: str = Field(...)
    source_id: str = Field(..., alias="sourceId")
    destination_id: str = Field(..., alias="destinationId")
    status: str = Field(...)
    
    class Config:
        allow_population_by_field_name = True


# ===============================================================================
# Databricks Models
# ===============================================================================

class DatabricksJobRun(BaseModel):
    """Model for Databricks job run response."""
    
    run_id: int = Field(..., alias="run_id")
    job_id: int = Field(..., alias="job_id")
    run_name: Optional[str] = Field(None, alias="run_name")
    state: Dict[str, Any] = Field(...)
    start_time: Optional[int] = Field(None, alias="start_time")
    end_time: Optional[int] = Field(None, alias="end_time")
    setup_duration: Optional[int] = Field(None, alias="setup_duration")
    execution_duration: Optional[int] = Field(None, alias="execution_duration")
    cleanup_duration: Optional[int] = Field(None, alias="cleanup_duration")
    
    class Config:
        allow_population_by_field_name = True


class DatabricksJobDetails(BaseModel):
    """Model for Databricks job details response."""
    
    job_id: int = Field(..., alias="job_id")
    settings: Dict[str, Any] = Field(...)
    created_time: Optional[int] = Field(None, alias="created_time")
    creator_user_name: Optional[str] = Field(None, alias="creator_user_name")
    
    class Config:
        allow_population_by_field_name = True


class DatabricksJobRunsResponse(BaseModel):
    """Model for Databricks job runs list response."""
    
    runs: List[DatabricksJobRun] = Field(default_factory=list)
    has_more: bool = Field(False, alias="has_more")
    next_page_token: Optional[str] = Field(None, alias="next_page_token")
    
    class Config:
        allow_population_by_field_name = True


# ===============================================================================
# Power Automate Models
# ===============================================================================

class PowerAutomateFlowRun(BaseModel):
    """Model for Power Automate flow run response."""
    
    name: str = Field(...)
    id: str = Field(...)
    type: str = Field(...)
    properties: Dict[str, Any] = Field(...)
    
    @property
    def run_id(self) -> str:
        """Extract run ID from the full name."""
        return self.name.split('/')[-1]
    
    @property
    def status(self) -> str:
        """Extract status from properties."""
        return self.properties.get("status", "Unknown")
    
    @property
    def start_time(self) -> Optional[str]:
        """Extract start time from properties."""
        return self.properties.get("startTime")
    
    @property
    def end_time(self) -> Optional[str]:
        """Extract end time from properties."""
        return self.properties.get("endTime")


class PowerAutomateFlowRunsResponse(BaseModel):
    """Model for Power Automate flow runs list response."""
    
    value: List[PowerAutomateFlowRun] = Field(default_factory=list)
    next_link: Optional[str] = Field(None, alias="@odata.nextLink")
    
    class Config:
        allow_population_by_field_name = True


class PowerAutomateFlow(BaseModel):
    """Model for Power Automate flow response."""
    
    name: str = Field(...)
    id: str = Field(...)
    type: str = Field(...)
    properties: Dict[str, Any] = Field(...)
    
    @property
    def display_name(self) -> str:
        """Extract display name from properties."""
        return self.properties.get("displayName", self.name)
    
    @property
    def state(self) -> str:
        """Extract state from properties."""
        return self.properties.get("state", "Unknown")


# ===============================================================================
# Snowflake Task Models
# ===============================================================================

class SnowflakeTaskHistory(BaseModel):
    """Model for Snowflake task history response."""
    
    name: str = Field(...)
    database_name: str = Field(..., alias="DATABASE_NAME")
    schema_name: str = Field(..., alias="SCHEMA_NAME")
    state: str = Field(...)
    scheduled_time: Optional[str] = Field(None, alias="SCHEDULED_TIME")
    started_time: Optional[str] = Field(None, alias="STARTED_TIME")
    completed_time: Optional[str] = Field(None, alias="COMPLETED_TIME")
    root_task_id: Optional[str] = Field(None, alias="ROOT_TASK_ID")
    graph_run_id: Optional[str] = Field(None, alias="GRAPH_RUN_ID")
    run_id: Optional[int] = Field(None, alias="RUN_ID")
    error_code: Optional[str] = Field(None, alias="ERROR_CODE")
    error_message: Optional[str] = Field(None, alias="ERROR_MESSAGE")
    
    class Config:
        allow_population_by_field_name = True


class SnowflakeTaskInfo(BaseModel):
    """Model for Snowflake task information."""
    
    name: str = Field(...)
    database_name: str = Field(..., alias="DATABASE_NAME")
    schema_name: str = Field(..., alias="SCHEMA_NAME")
    owner: Optional[str] = Field(None)
    comment: Optional[str] = Field(None)
    warehouse: Optional[str] = Field(None)
    schedule: Optional[str] = Field(None)
    state: str = Field(...)
    definition: Optional[str] = Field(None)
    condition: Optional[str] = Field(None)
    
    class Config:
        allow_population_by_field_name = True


# ===============================================================================
# Generic API Response Models
# ===============================================================================

class APIErrorResponse(BaseModel):
    """Generic model for API error responses."""
    
    error: str = Field(...)
    message: str = Field(...)
    code: Optional[int] = Field(None)
    details: Optional[Dict[str, Any]] = Field(None)


class PaginatedResponse(BaseModel):
    """Generic model for paginated API responses."""
    
    data: List[Dict[str, Any]] = Field(default_factory=list)
    page: int = Field(1)
    per_page: int = Field(10)
    total: int = Field(0)
    total_pages: int = Field(0)
    has_next: bool = Field(False)
    has_prev: bool = Field(False)


# ===============================================================================
# Platform Status Mapping Utilities
# ===============================================================================

def map_airbyte_status(airbyte_status: str) -> str:
    """Map Airbyte status to standardized JobStatus."""
    status_mapping = {
        "succeeded": "success",
        "failed": "failed",
        "cancelled": "cancelled",
        "running": "running",
        "pending": "pending",
        "incomplete": "running",
    }
    return status_mapping.get(airbyte_status.lower(), "unknown")


def map_databricks_status(databricks_state: Dict[str, Any]) -> str:
    """Map Databricks job run state to standardized JobStatus."""
    life_cycle_state = databricks_state.get("life_cycle_state", "").lower()
    result_state = databricks_state.get("result_state", "").lower()
    
    if life_cycle_state == "terminated":
        if result_state == "success":
            return "success"
        elif result_state == "failed":
            return "failed"
        elif result_state == "canceled":
            return "cancelled"
        else:
            return "unknown"
    elif life_cycle_state in ["pending", "running"]:
        return "running"
    else:
        return "unknown"


def map_powerautomate_status(pa_status: str) -> str:
    """Map Power Automate status to standardized JobStatus."""
    status_mapping = {
        "succeeded": "success",
        "failed": "failed",
        "cancelled": "cancelled",
        "running": "running",
        "waiting": "pending",
        "suspended": "pending",
    }
    return status_mapping.get(pa_status.lower(), "unknown")


def map_snowflake_task_status(snowflake_state: str) -> str:
    """Map Snowflake task state to standardized JobStatus."""
    status_mapping = {
        "succeeded": "success",
        "failed": "failed",
        "cancelled": "cancelled",
        "running": "running",
        "scheduled": "pending",
        "skipped": "cancelled",
    }
    return status_mapping.get(snowflake_state.lower(), "unknown")