"""
Airbyte monitoring agent that monitors job status and assesses platform health.
"""

import logging
from typing import Dict, Any, List
from datetime import datetime, timezone

from pydantic_ai import Agent, RunContext
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.openai import OpenAIProvider

from config.settings import settings
from .dependencies import AirbyteDependencies
from tools.airbyte_api import get_airbyte_job_status, get_airbyte_connection_health

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """
You are an expert Airbyte monitoring specialist responsible for assessing the health and status of Airbyte data pipelines. Your primary goal is to monitor job status, evaluate connection health, and provide actionable insights for data engineering teams.

Your capabilities:
1. **Job Status Monitoring**: Retrieve and analyze Airbyte job runs and their statuses
2. **Connection Health Assessment**: Evaluate the health of Airbyte connections and streams
3. **Health Analysis**: Provide intelligent analysis of job failures, patterns, and recommendations

When monitoring Airbyte jobs:
- Focus on recent job runs and their success/failure patterns
- Identify connections with recurring issues
- Analyze job duration trends and performance degradation
- Look for patterns in error messages and failure causes
- Provide specific, actionable recommendations for issues

When assessing platform health:
- Consider success rates, failure patterns, and job frequency
- Evaluate the impact of failed jobs on data pipelines
- Provide risk assessment based on job criticality and failure frequency
- Suggest preventive measures and optimization opportunities

Always provide:
- Clear summary of current platform status
- Specific issues identified with their severity levels
- Actionable recommendations for issue resolution
- Context about job execution patterns and trends
"""


# Create LLM model configuration
def get_llm_model() -> OpenAIModel:
    """Get LLM model configuration based on settings."""
    provider = OpenAIProvider(
        base_url=settings.llm_base_url,
        api_key=settings.llm_api_key
    )
    return OpenAIModel(settings.llm_model, provider=provider)


# Initialize the airbyte agent
airbyte_agent = Agent(
    get_llm_model(),
    deps_type=AirbyteDependencies,
    system_prompt=SYSTEM_PROMPT
)


@airbyte_agent.tool
async def get_airbyte_jobs(
    ctx: RunContext[AirbyteDependencies],
    job_type: str = "sync",
    limit: int = 50
) -> List[Dict[str, Any]]:
    """
    Retrieve Airbyte job status records for monitoring and analysis.
    
    Args:
        job_type: Type of jobs to retrieve (sync, reset, etc.)
        limit: Maximum number of jobs to retrieve (1-100)
    
    Returns:
        List of job status records with details
    """
    try:
        logger.info(f"Retrieving Airbyte jobs: type={job_type}, limit={limit}")
        
        # Get job status records
        job_records = await get_airbyte_job_status(
            api_key=ctx.deps.api_key,
            workspace_id=ctx.deps.workspace_id,
            job_type=job_type,
            limit=min(max(limit, 1), 100)
        )
        
        # Convert to dictionaries for agent processing
        jobs_data = []
        for record in job_records:
            job_data = {
                "job_id": record.job_id,
                "job_name": record.job_name,
                "status": record.status.value,
                "last_run_time": record.last_run_time.isoformat() if record.last_run_time else None,
                "duration_seconds": record.duration_seconds,
                "error_message": record.error_message,
                "platform": record.platform.value,
                "metadata": record.metadata,
                "checked_at": record.checked_at.isoformat()
            }
            jobs_data.append(job_data)
        
        logger.info(f"Successfully retrieved {len(jobs_data)} Airbyte job records")
        return jobs_data
        
    except Exception as e:
        logger.error(f"Failed to get Airbyte jobs: {e}")
        return [{"error": f"Failed to retrieve Airbyte jobs: {str(e)}"}]


@airbyte_agent.tool
async def get_connection_health(
    ctx: RunContext[AirbyteDependencies]
) -> List[Dict[str, Any]]:
    """
    Retrieve Airbyte connection health information for assessment.
    
    Returns:
        List of connection health data
    """
    try:
        logger.info("Retrieving Airbyte connection health")
        
        # Get connection health data
        health_data = await get_airbyte_connection_health(
            api_key=ctx.deps.api_key,
            workspace_id=ctx.deps.workspace_id
        )
        
        logger.info(f"Successfully retrieved health data for {len(health_data)} connections")
        return health_data
        
    except Exception as e:
        logger.error(f"Failed to get connection health: {e}")
        return [{"error": f"Failed to retrieve connection health: {str(e)}"}]


@airbyte_agent.tool
async def analyze_job_patterns(
    ctx: RunContext[AirbyteDependencies],
    jobs_data: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Analyze job execution patterns and provide insights.
    
    Args:
        jobs_data: List of job status records to analyze
    
    Returns:
        Dictionary with pattern analysis results
    """
    try:
        if not jobs_data or any("error" in job for job in jobs_data):
            return {"error": "No valid job data available for analysis"}
        
        # Calculate basic statistics
        total_jobs = len(jobs_data)
        successful_jobs = len([j for j in jobs_data if j.get("status") == "success"])
        failed_jobs = len([j for j in jobs_data if j.get("status") == "failed"])
        running_jobs = len([j for j in jobs_data if j.get("status") == "running"])
        
        success_rate = (successful_jobs / total_jobs * 100) if total_jobs > 0 else 0
        failure_rate = (failed_jobs / total_jobs * 100) if total_jobs > 0 else 0
        
        # Identify problematic jobs
        failed_job_names = [j.get("job_name", "Unknown") for j in jobs_data if j.get("status") == "failed"]
        error_patterns = {}
        
        for job in jobs_data:
            if job.get("status") == "failed" and job.get("error_message"):
                error_msg = job.get("error_message", "").lower()
                # Simple error categorization
                if "timeout" in error_msg:
                    error_patterns["timeout"] = error_patterns.get("timeout", 0) + 1
                elif "connection" in error_msg:
                    error_patterns["connection"] = error_patterns.get("connection", 0) + 1
                elif "authentication" in error_msg or "auth" in error_msg:
                    error_patterns["authentication"] = error_patterns.get("authentication", 0) + 1
                else:
                    error_patterns["other"] = error_patterns.get("other", 0) + 1
        
        # Duration analysis
        durations = [j.get("duration_seconds") for j in jobs_data if j.get("duration_seconds")]
        avg_duration = sum(durations) / len(durations) if durations else 0
        
        analysis = {
            "total_jobs": total_jobs,
            "successful_jobs": successful_jobs,
            "failed_jobs": failed_jobs,
            "running_jobs": running_jobs,
            "success_rate": round(success_rate, 2),
            "failure_rate": round(failure_rate, 2),
            "avg_duration_seconds": round(avg_duration, 2),
            "failed_job_names": list(set(failed_job_names)),
            "error_patterns": error_patterns,
            "analysis_timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"Completed job pattern analysis: {success_rate:.1f}% success rate")
        return analysis
        
    except Exception as e:
        logger.error(f"Failed to analyze job patterns: {e}")
        return {"error": f"Pattern analysis failed: {str(e)}"}


@airbyte_agent.tool 
async def create_platform_health_summary(
    ctx: RunContext[AirbyteDependencies],
    jobs_analysis: Dict[str, Any],
    connections_health: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Create a comprehensive platform health summary based on job and connection analysis.
    
    Args:
        jobs_analysis: Results from job pattern analysis
        connections_health: Connection health information
        
    Returns:
        Platform health summary dictionary
    """
    try:
        if jobs_analysis.get("error") or any("error" in conn for conn in connections_health):
            return {"error": "Insufficient data for health summary"}
        
        # Determine overall platform status
        success_rate = jobs_analysis.get("success_rate", 0)
        failed_jobs = jobs_analysis.get("failed_jobs", 0)
        error_patterns = jobs_analysis.get("error_patterns", {})
        
        # Assess platform health
        if success_rate >= 95 and failed_jobs == 0:
            platform_status = "Excellent - All systems operational"
            risk_level = "LOW"
        elif success_rate >= 85 and failed_jobs <= 2:
            platform_status = "Good - Minor issues detected"
            risk_level = "LOW"
        elif success_rate >= 70 and failed_jobs <= 5:
            platform_status = "Fair - Some issues need attention"
            risk_level = "MEDIUM"
        elif success_rate >= 50:
            platform_status = "Poor - Multiple issues detected"
            risk_level = "HIGH"
        else:
            platform_status = "Critical - Major failures detected"
            risk_level = "CRITICAL"
        
        # Generate recommendations
        recommendations = []
        if failed_jobs > 0:
            recommendations.append(f"Investigate {failed_jobs} failed jobs")
        
        if error_patterns.get("timeout", 0) > 0:
            recommendations.append("Review job timeout configurations")
        
        if error_patterns.get("connection", 0) > 0:
            recommendations.append("Check connection configurations and network connectivity")
        
        if error_patterns.get("authentication", 0) > 0:
            recommendations.append("Verify API credentials and authentication settings")
        
        if success_rate < 90:
            recommendations.append("Consider implementing job retry mechanisms")
        
        # Connection health assessment
        total_connections = len(connections_health)
        healthy_connections = len([c for c in connections_health if c.get("is_healthy", False)])
        
        # Create issues list
        issues = []
        if failed_jobs > 3:
            issues.append(f"High number of failed jobs: {failed_jobs}")
        
        if success_rate < 85:
            issues.append(f"Low success rate: {success_rate:.1f}%")
        
        if total_connections > healthy_connections:
            unhealthy_count = total_connections - healthy_connections
            issues.append(f"{unhealthy_count} unhealthy connections detected")
        
        summary = {
            "platform": "airbyte",
            "total_jobs": jobs_analysis.get("total_jobs", 0),
            "successful_jobs": jobs_analysis.get("successful_jobs", 0),
            "failed_jobs": failed_jobs,
            "running_jobs": jobs_analysis.get("running_jobs", 0),
            "platform_status": platform_status,
            "risk_level": risk_level,
            "success_rate": success_rate,
            "failure_rate": jobs_analysis.get("failure_rate", 0),
            "total_connections": total_connections,
            "healthy_connections": healthy_connections,
            "last_check": datetime.now(timezone.utc).isoformat(),
            "issues": issues,
            "recommendations": recommendations,
            "requires_attention": risk_level in ["HIGH", "CRITICAL"] or len(issues) > 2
        }
        
        logger.info(f"Created Airbyte platform health summary: {platform_status} ({risk_level})")
        return summary
        
    except Exception as e:
        logger.error(f"Failed to create platform health summary: {e}")
        return {"error": f"Health summary creation failed: {str(e)}"}


# Convenience function to create airbyte agent with dependencies
def create_airbyte_agent() -> Agent:
    """Create an Airbyte agent with default configuration."""
    return airbyte_agent