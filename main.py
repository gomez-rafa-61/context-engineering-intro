#!/usr/bin/env python3
"""
Main entry point for automated data pipeline monitoring.
Used by GitHub Actions and automated scheduling systems.
"""

import asyncio
import sys
import os
import logging
from datetime import datetime, timezone
from uuid import uuid4
import json

# Add current directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agents.orchestrator_agent import orchestrator_agent
from agents.dependencies import OrchestratorDependencies
from config.settings import settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('monitoring.log')
    ]
)

logger = logging.getLogger(__name__)


async def run_full_monitoring_cycle(
    notification_emails: list = None,
    monitoring_id: str = None,
    from_email: str = None
) -> dict:
    """
    Run a complete monitoring cycle across all platforms.
    
    Args:
        notification_emails: List of email addresses for notifications
        monitoring_id: Optional monitoring session ID
        from_email: Email address to send notifications from
        
    Returns:
        Dictionary with monitoring results and summary
    """
    if not monitoring_id:
        monitoring_id = f"mon_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
    
    if not notification_emails:
        notification_emails = [
            "devops@company.com", 
            "data-engineering@company.com"
        ]
    
    if not from_email:
        from_email = "pipeline-monitor@company.com"
    
    logger.info(f"Starting monitoring cycle: {monitoring_id}")
    
    try:
        # Set up orchestrator dependencies
        orchestrator_deps = OrchestratorDependencies.from_settings(
            session_id=f"auto_{uuid4().hex[:8]}",
            monitoring_id=monitoring_id,
            from_email=from_email
        )
        
        # Run comprehensive monitoring workflow
        monitoring_prompt = f"""
        Execute a complete data pipeline monitoring cycle:

        1. Monitor Airbyte platform - check all sync jobs and connections
        2. Assess overall system health across all monitored platforms
        3. Store monitoring results in Snowflake database
        4. Send health notifications if issues are detected

        Monitoring ID: {monitoring_id}
        Notification Recipients: {notification_emails}
        From Email: {from_email}

        Provide a comprehensive summary of:
        - Platforms monitored and their health status
        - Job success/failure counts and patterns  
        - Issues identified and their severity levels
        - Actions taken (data stored, notifications sent)
        - Recommendations for addressing any issues

        Focus on identifying actionable issues and ensuring all results are properly stored for compliance.
        """
        
        # Execute monitoring with the orchestrator agent
        result = await orchestrator_agent.run(
            monitoring_prompt,
            deps=orchestrator_deps
        )
        
        # Extract results
        monitoring_data = result.data if hasattr(result, 'data') else str(result)
        
        logger.info(f"Monitoring cycle completed: {monitoring_id}")
        
        return {
            "success": True,
            "monitoring_id": monitoring_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "monitoring_data": monitoring_data,
            "notification_recipients": notification_emails,
            "from_email": from_email
        }
        
    except Exception as e:
        logger.error(f"Monitoring cycle failed: {e}")
        return {
            "success": False,
            "monitoring_id": monitoring_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e),
            "notification_recipients": notification_emails,
            "from_email": from_email
        }


async def run_health_check() -> dict:
    """
    Run a quick health check across all platforms.
    
    Returns:
        Health check results
    """
    logger.info("Starting health check")
    
    try:
        orchestrator_deps = OrchestratorDependencies.from_settings(
            session_id=f"health_{uuid4().hex[:8]}",
            monitoring_id=f"health_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        
        health_prompt = """
        Perform a quick health check:
        1. Check Airbyte platform status
        2. Provide overall system health summary
        3. Identify any critical issues requiring immediate attention
        
        Focus on high-level status and critical issues only.
        """
        
        result = await orchestrator_agent.run(health_prompt, deps=orchestrator_deps)
        
        logger.info("Health check completed")
        
        return {
            "success": True,
            "type": "health_check",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "health_data": result.data if hasattr(result, 'data') else str(result)
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "success": False,
            "type": "health_check", 
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e)
        }


def print_summary(results: dict):
    """Print a summary of monitoring results."""
    print("\n" + "="*60)
    print("DATA PIPELINE MONITORING SUMMARY")
    print("="*60)
    print(f"Monitoring ID: {results.get('monitoring_id', 'N/A')}")
    print(f"Timestamp: {results.get('timestamp', 'N/A')}")
    print(f"Success: {'✅' if results.get('success') else '❌'}")
    
    if results.get('success'):
        print(f"Recipients: {len(results.get('notification_recipients', []))}")
        print(f"From Email: {results.get('from_email', 'N/A')}")
        
        # Try to extract key metrics from monitoring data
        monitoring_data = results.get('monitoring_data', {})
        if isinstance(monitoring_data, str):
            print(f"Summary: {monitoring_data[:200]}...")
        else:
            print(f"Data: {type(monitoring_data).__name__} object")
    else:
        print(f"Error: {results.get('error', 'Unknown error')}")
    
    print("="*60 + "\n")


async def main():
    """Main function for automated execution."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Data Pipeline Monitoring System")
    parser.add_argument(
        "--mode", 
        choices=["full", "health"],
        default="full",
        help="Monitoring mode (default: full)"
    )
    parser.add_argument(
        "--emails",
        nargs="*",
        help="Email addresses for notifications"
    )
    parser.add_argument(
        "--from-email",
        help="From email address for notifications"
    )
    parser.add_argument(
        "--monitoring-id",
        help="Custom monitoring session ID"
    )
    parser.add_argument(
        "--output-file",
        help="Save results to JSON file"
    )
    
    args = parser.parse_args()
    
    # Log startup information
    logger.info(f"Starting monitoring system - Mode: {args.mode}")
    logger.info(f"Environment: {settings.app_env}")
    logger.info(f"LLM Model: {settings.llm_model}")
    
    try:
        if args.mode == "health":
            results = await run_health_check()
        else:
            results = await run_full_monitoring_cycle(
                notification_emails=args.emails,
                monitoring_id=args.monitoring_id,
                from_email=args.from_email
            )
        
        # Print summary
        print_summary(results)
        
        # Save to file if requested
        if args.output_file:
            with open(args.output_file, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            print(f"Results saved to: {args.output_file}")
        
        # Exit with appropriate code
        sys.exit(0 if results.get('success') else 1)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())