"""
Snowflake Database agent for storing monitoring results and job status records.
"""

import logging
from typing import Dict, Any, List
from datetime import datetime, timezone

from pydantic_ai import Agent, RunContext
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.openai import OpenAIProvider

from config.settings import settings
from .dependencies import SnowflakeDBDependencies
from tools.snowflake_db_api import store_job_status_records, store_monitoring_result
from models.job_status import JobStatusRecord, MonitoringResult

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """
You are an expert Snowflake database operations specialist responsible for storing and managing data pipeline monitoring results. Your primary goal is to efficiently store job status records, monitoring sessions, and health assessments in the Snowflake AUDIT_JOB_HUB schema.

Your capabilities:
1. **Data Storage**: Store job status records across all monitored platforms
2. **Session Management**: Track monitoring sessions with comprehensive metadata
3. **Health Records**: Maintain historical health assessment data
4. **Data Integrity**: Ensure data quality and consistency in storage operations

When storing monitoring data:
- Validate data structure and completeness before storage
- Handle batch operations efficiently for large datasets
- Maintain referential integrity between related records
- Provide clear success/failure feedback with details
- Log operations for audit and debugging purposes

Data Storage Patterns:
- Job status records: Individual job execution results
- Monitoring sessions: Complete monitoring cycle results
- Platform summaries: Per-platform health aggregations
- Health assessments: AI-generated health evaluations

Always ensure:
- Data consistency across related tables
- Proper error handling and rollback on failures
- Efficient batch processing for large datasets
- Clear reporting of storage operation results
"""


# Create LLM model configuration  
def get_llm_model() -> OpenAIModel:
    """Get LLM model configuration based on settings."""
    provider = OpenAIProvider(
        base_url=settings.llm_base_url,
        api_key=settings.llm_api_key
    )
    return OpenAIModel(settings.llm_model, provider=provider)


# Initialize the snowflake db agent
snowflake_db_agent = Agent(
    get_llm_model(),
    deps_type=SnowflakeDBDependencies,
    system_prompt=SYSTEM_PROMPT
)


@snowflake_db_agent.tool
async def store_job_records(
    ctx: RunContext[SnowflakeDBDependencies],
    job_records_data: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Store job status records in Snowflake database.
    
    Args:
        job_records_data: List of job status record dictionaries
        
    Returns:
        Storage operation results
    """
    try:
        logger.info(f"Storing {len(job_records_data)} job status records")
        
        if not job_records_data:
            return {"stored_records": 0, "message": "No records to store"}
        
        # Convert dictionaries to JobStatusRecord objects
        job_records = []
        for record_data in job_records_data:
            try:
                # Handle nested data conversion if needed
                if "last_run_time" in record_data and isinstance(record_data["last_run_time"], str):
                    record_data["last_run_time"] = datetime.fromisoformat(
                        record_data["last_run_time"].replace('Z', '+00:00')
                    )
                
                if "checked_at" in record_data and isinstance(record_data["checked_at"], str):
                    record_data["checked_at"] = datetime.fromisoformat(
                        record_data["checked_at"].replace('Z', '+00:00')
                    )
                
                # Create JobStatusRecord object
                record = JobStatusRecord(**record_data)
                job_records.append(record)
                
            except Exception as e:
                logger.warning(f"Failed to convert record data: {e}")
                continue
        
        if not job_records:
            return {"error": "No valid records to store after conversion"}
        
        # Store records
        stored_count = await store_job_status_records(
            records=job_records,
            account=ctx.deps.account,
            user=ctx.deps.user,
            password=ctx.deps.password,
            database=ctx.deps.database,
            schema=ctx.deps.schema,
            warehouse=ctx.deps.warehouse,
            role=ctx.deps.role,
        )
        
        logger.info(f"Successfully stored {stored_count} job status records")
        return {
            "stored_records": stored_count,
            "database": f"{ctx.deps.database}.{ctx.deps.schema}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "success": True
        }
        
    except Exception as e:
        logger.error(f"Failed to store job records: {e}")
        return {"error": f"Storage operation failed: {str(e)}", "stored_records": 0}


@snowflake_db_agent.tool
async def store_monitoring_session(
    ctx: RunContext[SnowflakeDBDependencies],
    monitoring_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Store complete monitoring session results in Snowflake.
    
    Args:
        monitoring_data: Complete monitoring session data
        
    Returns:
        Storage operation results
    """
    try:
        logger.info(f"Storing monitoring session: {monitoring_data.get('monitoring_id', 'unknown')}")
        
        # Convert dictionary to MonitoringResult object
        monitoring_result = MonitoringResult(**monitoring_data)
        
        # Store monitoring session
        session_id = await store_monitoring_result(
            monitoring_result=monitoring_result,
            account=ctx.deps.account,
            user=ctx.deps.user,
            password=ctx.deps.password,
            database=ctx.deps.database,
            schema=ctx.deps.schema,
            warehouse=ctx.deps.warehouse,
            role=ctx.deps.role,
        )
        
        logger.info(f"Successfully stored monitoring session: {session_id}")
        return {
            "monitoring_session_stored": True,
            "session_id": session_id,
            "database": f"{ctx.deps.database}.{ctx.deps.schema}",
            "job_records_count": len(monitoring_result.job_records),
            "platform_summaries_count": len(monitoring_result.platform_summaries),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "success": True
        }
        
    except Exception as e:
        logger.error(f"Failed to store monitoring session: {e}")
        return {"error": f"Monitoring session storage failed: {str(e)}", "success": False}


@snowflake_db_agent.tool
async def create_storage_summary(
    ctx: RunContext[SnowflakeDBDependencies],
    job_storage_result: Dict[str, Any],
    session_storage_result: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Create a comprehensive summary of all storage operations.
    
    Args:
        job_storage_result: Results from job records storage
        session_storage_result: Results from monitoring session storage
        
    Returns:
        Comprehensive storage summary
    """
    try:
        total_records_stored = job_storage_result.get("stored_records", 0)
        session_stored = session_storage_result.get("success", False)
        session_id = session_storage_result.get("session_id", "unknown")
        
        # Determine overall storage success
        job_storage_success = job_storage_result.get("success", False) or total_records_stored > 0
        overall_success = job_storage_success and session_stored
        
        # Compile any errors
        errors = []
        if job_storage_result.get("error"):
            errors.append(f"Job storage: {job_storage_result['error']}")
        if session_storage_result.get("error"):
            errors.append(f"Session storage: {session_storage_result['error']}")
        
        summary = {
            "storage_operation_complete": True,
            "overall_success": overall_success,
            "database_location": f"{ctx.deps.database}.{ctx.deps.schema}",
            "results": {
                "job_records_stored": total_records_stored,
                "monitoring_session_stored": session_stored,
                "session_id": session_id,
            },
            "errors": errors,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "storage_details": {
                "job_storage": job_storage_result,
                "session_storage": session_storage_result,
            }
        }
        
        if overall_success:
            logger.info(f"Storage summary: {total_records_stored} records + session stored successfully")
        else:
            logger.warning(f"Storage summary: Partial success - {len(errors)} errors occurred")
        
        return summary
        
    except Exception as e:
        logger.error(f"Failed to create storage summary: {e}")
        return {"error": f"Storage summary creation failed: {str(e)}"}


# Convenience function
def create_snowflake_db_agent() -> Agent:
    """Create a Snowflake DB agent with default configuration."""
    return snowflake_db_agent