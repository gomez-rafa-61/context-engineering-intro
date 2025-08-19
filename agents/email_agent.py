"""
Email notification agent for generating and sending monitoring alerts.
"""

import logging
from typing import Dict, Any, List
from datetime import datetime, timezone
from uuid import uuid4

from pydantic_ai import Agent, RunContext
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.openai import OpenAIProvider

from config.settings import settings
from .dependencies import EmailDependencies
from tools.outlook_api import send_notification_email, create_notification_draft
from models.notification_models import (
    EmailNotification,
    EmailRecipient,
    NotificationResult,
    DEFAULT_EMAIL_TEMPLATES,
    NotificationPriority,
)

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """
You are an expert email notification specialist responsible for creating intelligent, contextual alerts for data pipeline monitoring systems. Your primary goal is to generate clear, actionable email notifications based on monitoring results and health assessments.

Your capabilities:
1. **Alert Generation**: Create targeted email notifications based on monitoring data
2. **Content Optimization**: Tailor email content based on severity and context
3. **Template Management**: Use appropriate email templates for different alert types

When creating notifications:
- Assess the severity and urgency based on monitoring data
- Choose appropriate recipients based on the type and severity of issues
- Generate clear, actionable subject lines and content
- Include relevant details without overwhelming recipients
- Provide specific recommendations for issue resolution
- Use proper formatting for readability

Alert Prioritization:
- CRITICAL: System-wide failures, multiple platform issues
- HIGH: Platform-specific failures, significant degradation
- NORMAL: Minor issues, regular status updates
- LOW: Informational updates, successful recoveries

Always ensure notifications are:
- Clear and concise
- Actionable with specific next steps
- Appropriately prioritized
- Formatted for easy reading
"""


# Create LLM model configuration
def get_llm_model() -> OpenAIModel:
    """Get LLM model configuration based on settings."""
    provider = OpenAIProvider(
        base_url=settings.llm_base_url,
        api_key=settings.llm_api_key
    )
    return OpenAIModel(settings.llm_model, provider=provider)


# Initialize the email agent
email_agent = Agent(
    get_llm_model(),
    deps_type=EmailDependencies,
    system_prompt=SYSTEM_PROMPT
)


@email_agent.tool
async def generate_monitoring_notification(
    ctx: RunContext[EmailDependencies],
    monitoring_context: Dict[str, Any],
    recipient_emails: List[str],
    send_notification: bool = False
) -> Dict[str, Any]:
    """
    Generate and optionally send monitoring notification based on context.
    
    Args:
        monitoring_context: Monitoring results and health assessment data
        recipient_emails: List of email addresses to notify
        send_notification: Whether to send immediately or create draft
        
    Returns:
        Notification generation and sending results
    """
    try:
        logger.info(f"Generating monitoring notification for {len(recipient_emails)} recipients")
        
        # Extract key information
        failed_jobs = monitoring_context.get("failed_jobs_count", 0)
        total_jobs = monitoring_context.get("total_jobs_count", 0)
        risk_level = monitoring_context.get("risk_level", "LOW")
        platform_summaries = monitoring_context.get("platform_summaries", [])
        monitoring_id = monitoring_context.get("monitoring_id", f"mon_{uuid4().hex[:8]}")
        
        # Determine notification priority and template
        if risk_level == "CRITICAL" or failed_jobs > 10:
            template_key = "critical_alert"
            priority = NotificationPriority.URGENT
        elif risk_level == "HIGH" or failed_jobs > 5:
            template_key = "warning_alert" 
            priority = NotificationPriority.HIGH
        else:
            template_key = "info_summary"
            priority = NotificationPriority.NORMAL
        
        # Get template
        template = DEFAULT_EMAIL_TEMPLATES.get(template_key)
        if not template:
            return {"error": f"Template {template_key} not found"}
        
        # Generate email content
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        success_count = total_jobs - failed_jobs
        success_rate = (success_count / total_jobs * 100) if total_jobs > 0 else 0
        failure_rate = (failed_jobs / total_jobs * 100) if total_jobs > 0 else 0
        
        # Generate platform details section
        platform_details = []
        for summary in platform_summaries:
            if isinstance(summary, dict):
                platform_name = summary.get("platform", "Unknown").title()
                platform_status = summary.get("platform_status", "Unknown")
                platform_failed = summary.get("failed_jobs", 0)
                platform_total = summary.get("total_jobs", 0)
                
                platform_details.append(
                    f"<li><strong>{platform_name}</strong>: {platform_status} "
                    f"({platform_failed}/{platform_total} failed)</li>"
                )
        
        platform_details_html = "<ul>" + "".join(platform_details) + "</ul>" if platform_details else "No platform details available"
        
        # Generate recommendations
        recommendations = monitoring_context.get("recommendations", [])
        recommendations_html = "<ul>" + "".join(f"<li>{rec}</li>" for rec in recommendations) + "</ul>" if recommendations else "<p>No specific recommendations at this time.</p>"
        
        # Format subject and body
        subject = template.subject_template.format(
            failed_count=failed_jobs,
            total_count=total_jobs,
        )
        
        body = template.body_template.format(
            timestamp=timestamp,
            monitoring_id=monitoring_id,
            failed_count=failed_jobs,
            total_count=total_jobs,
            success_count=success_count,
            failure_rate=failure_rate,
            success_rate=success_rate,
            risk_level=risk_level,
            platform_details=platform_details_html,
            recommendations=recommendations_html,
        )
        
        # Create recipients
        recipients = [
            EmailRecipient(email=email, type="to") for email in recipient_emails
        ]
        
        # Create notification
        notification = EmailNotification(
            notification_id=f"notif_{monitoring_id}",
            recipients=recipients,
            subject=subject,
            body=body,
            priority=priority,
            metadata={"monitoring_id": monitoring_id, "risk_level": risk_level}
        )
        
        # Send or create draft
        if send_notification and ctx.deps.from_email:
            result = await send_notification_email(
                notification=notification,
                client_id=ctx.deps.client_id,
                client_secret=ctx.deps.client_secret,
                tenant_id=ctx.deps.tenant_id,
                from_email=ctx.deps.from_email,
            )
        elif ctx.deps.from_email:
            result = await create_notification_draft(
                notification=notification,
                client_id=ctx.deps.client_id,
                client_secret=ctx.deps.client_secret,
                tenant_id=ctx.deps.tenant_id,
                from_email=ctx.deps.from_email,
            )
        else:
            # Return notification content without sending
            result = NotificationResult(
                notification_id=notification.notification_id,
                success=True,
                message_id="preview_only",
                sent_at=datetime.now(timezone.utc),
                recipients_count=len(recipients),
            )
        
        response = {
            "notification_generated": True,
            "template_used": template_key,
            "priority": priority.value,
            "subject": subject,
            "recipients_count": len(recipients),
            "send_result": {
                "success": result.success,
                "message_id": result.message_id,
                "error": result.error_message,
            } if send_notification else {"action": "draft_created" if ctx.deps.from_email else "preview_only"},
            "notification_id": notification.notification_id,
        }
        
        logger.info(f"Successfully generated notification: {template_key} priority, {len(recipients)} recipients")
        return response
        
    except Exception as e:
        logger.error(f"Failed to generate monitoring notification: {e}")
        return {"error": f"Notification generation failed: {str(e)}"}


@email_agent.tool
async def create_custom_alert(
    ctx: RunContext[EmailDependencies],
    subject: str,
    message: str,
    recipients: List[str],
    priority: str = "normal",
    send_immediately: bool = False
) -> Dict[str, Any]:
    """
    Create a custom alert notification.
    
    Args:
        subject: Email subject line
        message: Email message content
        recipients: List of recipient email addresses
        priority: Priority level (low, normal, high, urgent)
        send_immediately: Whether to send or create draft
        
    Returns:
        Custom alert creation results
    """
    try:
        logger.info(f"Creating custom alert: {subject}")
        
        # Validate priority
        try:
            notification_priority = NotificationPriority(priority.lower())
        except ValueError:
            notification_priority = NotificationPriority.NORMAL
        
        # Create recipients
        email_recipients = [
            EmailRecipient(email=email, type="to") for email in recipients
        ]
        
        # Create notification
        notification = EmailNotification(
            notification_id=f"custom_{uuid4().hex[:8]}",
            recipients=email_recipients,
            subject=subject,
            body=message,
            priority=notification_priority,
            metadata={"type": "custom_alert"}
        )
        
        # Send or create draft
        if send_immediately and ctx.deps.from_email:
            result = await send_notification_email(
                notification=notification,
                client_id=ctx.deps.client_id,
                client_secret=ctx.deps.client_secret,
                tenant_id=ctx.deps.tenant_id,
                from_email=ctx.deps.from_email,
            )
            action = "sent"
        elif ctx.deps.from_email:
            result = await create_notification_draft(
                notification=notification,
                client_id=ctx.deps.client_id,
                client_secret=ctx.deps.client_secret,
                tenant_id=ctx.deps.tenant_id,
                from_email=ctx.deps.from_email,
            )
            action = "draft_created"
        else:
            result = NotificationResult(
                notification_id=notification.notification_id,
                success=True,
                message_id="preview_only",
                sent_at=datetime.now(timezone.utc),
                recipients_count=len(recipients),
            )
            action = "preview_only"
        
        return {
            "custom_alert_created": True,
            "action": action,
            "notification_id": notification.notification_id,
            "success": result.success,
            "recipients_count": len(recipients),
            "error": result.error_message if not result.success else None
        }
        
    except Exception as e:
        logger.error(f"Failed to create custom alert: {e}")
        return {"error": f"Custom alert creation failed: {str(e)}"}


# Convenience function
def create_email_agent() -> Agent:
    """Create an email agent with default configuration."""
    return email_agent