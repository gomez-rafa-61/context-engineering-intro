"""
Main orchestrator agent that coordinates all platform monitoring activities.
"""

import logging
from typing import Dict, Any, List
from datetime import datetime, timezone
from uuid import uuid4

from pydantic_ai import Agent, RunContext
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.openai import OpenAIProvider

from config.settings import settings
from .dependencies import OrchestratorDependencies
from .airbyte_agent import airbyte_agent
from .email_agent import email_agent
from .snowflake_db_agent import snowflake_db_agent
from models.job_status import RiskLevel

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """
You are the master orchestrator for a comprehensive data pipeline monitoring system. Your primary goal is to coordinate monitoring activities across all data platforms, assess overall system health, and ensure appropriate notifications and data storage.

Your capabilities:
1. **Platform Coordination**: Manage monitoring across Airbyte, Databricks, Power Automate, and Snowflake Task platforms
2. **Health Assessment**: Analyze cross-platform health and identify system-wide issues
3. **Intelligent Notifications**: Determine when and how to notify teams based on criticality
4. **Data Management**: Ensure all monitoring results are properly stored for compliance and analysis

Your orchestration workflow:
1. Monitor all configured platforms in parallel
2. Collect and analyze job status data from each platform  
3. Assess overall system health across all platforms
4. Store monitoring results in Snowflake for historical tracking
5. Generate and send notifications when issues require attention
6. Provide comprehensive monitoring reports with actionable insights

Platform Risk Assessment:
- CRITICAL: Multiple platforms failing, system-wide issues
- HIGH: Single platform with major failures, critical jobs failing
- MEDIUM: Minor failures, performance degradation
- LOW: All systems healthy, minor warnings

Decision Making:
- Automatically escalate critical issues
- Batch minor issues for periodic reporting
- Prioritize notifications based on business impact
- Ensure compliance with data retention requirements

Always provide:
- Clear summary of monitoring activities performed
- Overall system health status with risk assessment
- Specific issues identified with severity levels
- Actions taken (notifications sent, data stored)
- Recommendations for addressing identified issues
"""


# Create LLM model configuration
def get_llm_model() -> OpenAIModel:
    """Get LLM model configuration based on settings."""
    provider = OpenAIProvider(
        base_url=settings.llm_base_url,
        api_key=settings.llm_api_key
    )
    return OpenAIModel(settings.llm_model, provider=provider)


# Initialize the orchestrator agent
orchestrator_agent = Agent(
    get_llm_model(),
    deps_type=OrchestratorDependencies,
    system_prompt=SYSTEM_PROMPT
)


@orchestrator_agent.tool
async def monitor_airbyte_platform(
    ctx: RunContext[OrchestratorDependencies]
) -> Dict[str, Any]:
    """
    Monitor Airbyte platform using the specialized Airbyte agent.
    
    Returns:
        Airbyte monitoring results and platform health summary
    """
    try:
        logger.info("Starting Airbyte platform monitoring")
        
        # Get Airbyte dependencies
        airbyte_deps = ctx.deps.get_airbyte_deps()
        
        # Run Airbyte monitoring workflow
        monitoring_prompt = """
        Perform comprehensive Airbyte monitoring:
        1. Retrieve recent job status for all sync jobs
        2. Check connection health across all connections  
        3. Analyze job execution patterns and identify issues
        4. Create a platform health summary with recommendations
        
        Focus on identifying failed jobs, connection issues, and performance problems.
        """
        
        # CRITICAL: Pass usage for token tracking
        result = await airbyte_agent.run(
            monitoring_prompt,
            deps=airbyte_deps,
            usage=ctx.usage
        )
        
        logger.info("Completed Airbyte platform monitoring")
        return {
            "platform": "airbyte",
            "success": True,
            "monitoring_data": result.data,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Airbyte platform monitoring failed: {e}")
        return {
            "platform": "airbyte",
            "success": False,
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }


@orchestrator_agent.tool
async def store_monitoring_results(
    ctx: RunContext[OrchestratorDependencies],
    all_platform_results: List[Dict[str, Any]],
    overall_assessment: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Store all monitoring results in Snowflake database.
    
    Args:
        all_platform_results: Results from all platform monitoring
        overall_assessment: Overall health assessment
        
    Returns:
        Storage operation results
    """
    try:
        logger.info("Storing monitoring results in Snowflake")
        
        # Collect all job records from platform results
        all_job_records = []
        for platform_result in all_platform_results:
            if platform_result.get("success") and platform_result.get("monitoring_data"):
                # Extract job records from monitoring data
                monitoring_data = platform_result["monitoring_data"]
                if isinstance(monitoring_data, str):
                    # If it's a string response, we'll need to parse it or skip
                    continue
                    
                # Look for job records in various possible structures
                jobs = monitoring_data.get("jobs", [])
                if not jobs:
                    # Try alternative structures
                    jobs = monitoring_data.get("job_records", [])
                
                all_job_records.extend(jobs)
        
        # Get Snowflake DB dependencies
        db_deps = ctx.deps.get_snowflake_db_deps()
        
        # Store job records
        storage_prompt = f"""
        Store the monitoring results in Snowflake:
        1. Store {len(all_job_records)} job status records
        2. Create a monitoring session record with overall assessment
        3. Provide a comprehensive storage summary
        
        Job records data: {all_job_records[:5]}...  # Show first 5 for context
        Overall assessment: {overall_assessment}
        """
        
        # CRITICAL: Pass usage for token tracking
        storage_result = await snowflake_db_agent.run(
            storage_prompt,
            deps=db_deps,
            usage=ctx.usage
        )
        
        logger.info("Successfully stored monitoring results")
        return {
            "storage_success": True,
            "storage_data": storage_result.data,
            "records_stored": len(all_job_records),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to store monitoring results: {e}")
        return {
            "storage_success": False,
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }


@orchestrator_agent.tool
async def send_health_notifications(
    ctx: RunContext[OrchestratorDependencies],
    overall_assessment: Dict[str, Any],
    platform_summaries: List[Dict[str, Any]],
    recipient_emails: List[str] = None
) -> Dict[str, Any]:
    """
    Send health notifications based on monitoring results.
    
    Args:
        overall_assessment: Overall health assessment
        platform_summaries: Per-platform health summaries
        recipient_emails: Optional list of recipient emails
        
    Returns:
        Notification sending results
    """
    try:
        # Default recipients if none provided
        if not recipient_emails:
            recipient_emails = ["devops@company.com", "data-engineering@company.com"]
        
        # Determine if notification is needed
        risk_level = overall_assessment.get("risk_level", "LOW")
        requires_notification = overall_assessment.get("requires_notification", False)
        failed_jobs_count = overall_assessment.get("failed_jobs_count", 0)
        
        # Skip notification for low-risk situations
        if risk_level == "LOW" and failed_jobs_count == 0 and not requires_notification:
            logger.info("No notification needed - system healthy")
            return {
                "notification_sent": False,
                "reason": "System healthy, no notification required",
                "risk_level": risk_level,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        
        logger.info(f"Sending health notification - Risk: {risk_level}")
        
        # Get email dependencies
        email_deps = ctx.deps.get_email_deps()
        
        # Prepare monitoring context for notification
        monitoring_context = {
            "monitoring_id": ctx.deps.monitoring_id or f"mon_{uuid4().hex[:8]}",
            "overall_health": overall_assessment.get("overall_health", "Unknown"),
            "risk_level": risk_level,
            "failed_jobs_count": failed_jobs_count,
            "total_jobs_count": overall_assessment.get("jobs_analyzed", 0),
            "platform_summaries": platform_summaries,
            "recommendations": overall_assessment.get("recommendations", []),
            "requires_notification": requires_notification
        }
        
        # Generate and send notification
        notification_prompt = f"""
        Generate monitoring notification for the following context:
        
        Recipients: {recipient_emails}
        Risk Level: {risk_level}
        Failed Jobs: {failed_jobs_count}
        Platform Count: {len(platform_summaries)}
        
        Monitoring context: {monitoring_context}
        
        Create an appropriate notification based on the severity and send it to the recipients.
        """
        
        # CRITICAL: Pass usage for token tracking
        notification_result = await email_agent.run(
            notification_prompt,
            deps=email_deps,
            usage=ctx.usage
        )
        
        logger.info("Health notification processing completed")
        return {
            "notification_sent": True,
            "risk_level": risk_level,
            "recipients_count": len(recipient_emails),
            "notification_data": notification_result.data,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to send health notifications: {e}")
        return {
            "notification_sent": False,
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }


@orchestrator_agent.tool
async def assess_overall_system_health(
    ctx: RunContext[OrchestratorDependencies],
    platform_results: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Assess overall system health based on all platform monitoring results.
    
    Args:
        platform_results: Results from all monitored platforms
        
    Returns:
        Overall health assessment
    """
    try:
        logger.info(f"Assessing overall system health across {len(platform_results)} platforms")
        
        # Initialize counters
        total_jobs = 0
        failed_jobs = 0
        successful_platforms = 0
        failed_platforms = 0
        critical_issues = []
        all_recommendations = []
        
        # Process each platform result
        for platform_result in platform_results:
            platform_name = platform_result.get("platform", "unknown")
            
            if platform_result.get("success"):
                successful_platforms += 1
                monitoring_data = platform_result.get("monitoring_data", {})
                
                # Extract job counts (structure may vary)
                if isinstance(monitoring_data, dict):
                    platform_jobs = monitoring_data.get("total_jobs", 0)
                    platform_failed = monitoring_data.get("failed_jobs", 0)
                    
                    total_jobs += platform_jobs
                    failed_jobs += platform_failed
                    
                    # Collect issues and recommendations
                    if monitoring_data.get("issues"):
                        critical_issues.extend(monitoring_data["issues"])
                    if monitoring_data.get("recommendations"):
                        all_recommendations.extend(monitoring_data["recommendations"])
                    
                    # Check for platform-specific critical conditions
                    if platform_failed > 5:
                        critical_issues.append(f"{platform_name.title()} has {platform_failed} failed jobs")
            else:
                failed_platforms += 1
                critical_issues.append(f"{platform_name.title()} monitoring failed: {platform_result.get('error', 'Unknown error')}")
        
        # Calculate overall metrics
        success_rate = (total_jobs - failed_jobs) / total_jobs * 100 if total_jobs > 0 else 100
        platform_availability = successful_platforms / len(platform_results) * 100
        
        # Determine overall risk level
        if failed_platforms > 1 or failed_jobs > 15:
            risk_level = RiskLevel.CRITICAL
            overall_health = "Critical - Multiple systems experiencing issues"
        elif failed_platforms == 1 or failed_jobs > 8:
            risk_level = RiskLevel.HIGH
            overall_health = "Poor - Significant issues detected"
        elif failed_jobs > 3 or success_rate < 90:
            risk_level = RiskLevel.MEDIUM
            overall_health = "Fair - Some issues require attention"
        elif success_rate < 98 or len(critical_issues) > 0:
            risk_level = RiskLevel.LOW
            overall_health = "Good - Minor issues detected"
        else:
            risk_level = RiskLevel.LOW
            overall_health = "Excellent - All systems operational"
        
        # Determine if notification is required
        requires_notification = risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL] or failed_jobs > 5
        
        assessment = {
            "overall_health": overall_health,
            "risk_level": risk_level.value,
            "requires_notification": requires_notification,
            "jobs_analyzed": total_jobs,
            "failed_jobs_count": failed_jobs,
            "success_rate": round(success_rate, 2),
            "platform_availability": round(platform_availability, 2),
            "successful_platforms": successful_platforms,
            "failed_platforms": failed_platforms,
            "critical_issues": list(set(critical_issues)),  # Remove duplicates
            "recommendations": list(set(all_recommendations)),  # Remove duplicates
            "assessment_timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"Overall health assessment: {overall_health} ({risk_level.value})")
        return assessment
        
    except Exception as e:
        logger.error(f"Failed to assess overall system health: {e}")
        return {
            "overall_health": "Unknown - Assessment failed",
            "risk_level": RiskLevel.MEDIUM.value,
            "requires_notification": True,
            "error": str(e),
            "assessment_timestamp": datetime.now(timezone.utc).isoformat()
        }


# Convenience function
def create_orchestrator_agent() -> Agent:
    """Create an orchestrator agent with default configuration."""
    return orchestrator_agent