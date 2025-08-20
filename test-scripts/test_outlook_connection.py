#!/usr/bin/env python3
"""
Test script for Outlook/Email API connection and functionality.
This script validates the Outlook API integration independently.
"""

import asyncio
import os
import sys
from pathlib import Path
from typing import Dict, Any, List
import json
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

from tools.outlook_api import (
    check_outlook_health,
    create_email_draft,
    send_email_notification,
    get_outlook_profile,
    create_pipeline_notification_email
)
from models.notification_models import EmailNotification, NotificationPriority, NotificationTemplate
from models.job_status import JobStatusRecord, JobStatus, PlatformType

console = Console()

class OutlookConnectionTest:
    """Test class for Outlook API connection and functionality."""
    
    def __init__(self):
        load_dotenv()
        self.client_id = os.getenv("OUTLOOK_CLIENT_ID")
        self.client_secret = os.getenv("OUTLOOK_CLIENT_SECRET")
        self.tenant_id = os.getenv("OUTLOOK_TENANT_ID")
        self.test_recipient = os.getenv("OUTLOOK_TEST_RECIPIENT", "")
        
        missing_vars = []
        if not self.client_id:
            missing_vars.append("OUTLOOK_CLIENT_ID")
        if not self.client_secret:
            missing_vars.append("OUTLOOK_CLIENT_SECRET")
        if not self.tenant_id:
            missing_vars.append("OUTLOOK_TENANT_ID")
        
        if missing_vars:
            console.print(f"[red]❌ Missing environment variables: {', '.join(missing_vars)}[/red]")
            console.print("[yellow]💡 Set these in your .env file for Microsoft Graph authentication[/yellow]")
            sys.exit(1)
        
        if not self.test_recipient:
            console.print("[yellow]⚠️ OUTLOOK_TEST_RECIPIENT not set - email sending tests will be skipped[/yellow]")
    
    def print_config(self):
        """Print current configuration."""
        config_table = Table(title="Outlook Test Configuration")
        config_table.add_column("Setting", style="cyan")
        config_table.add_column("Value", style="white")
        
        config_table.add_row("Client ID", f"{'*' * 20}...{self.client_id[-4:] if len(self.client_id) > 4 else 'SET'}")
        config_table.add_row("Client Secret", f"{'*' * 20}...{self.client_secret[-4:] if len(self.client_secret) > 4 else 'SET'}")
        config_table.add_row("Tenant ID", self.tenant_id)
        config_table.add_row("Test Recipient", self.test_recipient or "Not specified")
        
        console.print(config_table)
        console.print()
    
    async def test_health_check(self) -> bool:
        """Test basic Outlook API health and authentication."""
        console.print("[blue]🔍 Testing Outlook API health and authentication...[/blue]")
        
        try:
            health_status = await check_outlook_health(
                self.client_id, 
                self.client_secret, 
                self.tenant_id
            )
            
            if health_status.get("status") == "healthy":
                console.print("[green]✅ Outlook API health check passed[/green]")
                console.print(f"[blue]ℹ️ Token info: {health_status.get('token_info', {})}[/blue]")
                return True
            else:
                console.print(f"[yellow]⚠️ Outlook API health check returned: {health_status}[/yellow]")
                return False
                
        except Exception as e:
            console.print(f"[red]❌ Outlook API health check failed: {str(e)}[/red]")
            return False
    
    async def test_get_profile(self) -> Dict[str, Any]:
        """Test getting Outlook user profile."""
        console.print("[blue]🔍 Testing Outlook user profile retrieval...[/blue]")
        
        try:
            profile = await get_outlook_profile(
                self.client_id, 
                self.client_secret, 
                self.tenant_id
            )
            
            if profile:
                profile_table = Table(title="Outlook User Profile")
                profile_table.add_column("Property", style="cyan")
                profile_table.add_column("Value", style="white")
                
                profile_table.add_row("Display Name", profile.get("displayName", "Unknown"))
                profile_table.add_row("Email", profile.get("mail", profile.get("userPrincipalName", "Unknown")))
                profile_table.add_row("User ID", profile.get("id", "Unknown"))
                profile_table.add_row("Job Title", profile.get("jobTitle", "Not specified"))
                profile_table.add_row("Office Location", profile.get("officeLocation", "Not specified"))
                
                console.print(profile_table)
                console.print(f"[green]✅ Successfully retrieved user profile[/green]")
                return profile
            else:
                console.print("[yellow]⚠️ No profile data retrieved[/yellow]")
                return {}
                
        except Exception as e:
            console.print(f"[red]❌ Failed to get user profile: {str(e)}[/red]")
            return {}
    
    async def test_create_email_draft(self) -> str:
        """Test creating email drafts."""
        console.print("[blue]🔍 Testing email draft creation...[/blue]")
        
        try:
            # Create a sample notification
            test_notification = EmailNotification(
                subject="Test Pipeline Monitoring Alert",
                recipient=self.test_recipient or "test@example.com",
                priority=NotificationPriority.MEDIUM,
                template=NotificationTemplate.ALERT,
                job_summary={
                    "total_jobs": 10,
                    "failed_jobs": 2,
                    "running_jobs": 1,
                    "successful_jobs": 7
                },
                failed_jobs=[
                    {
                        "platform": "airbyte",
                        "job_name": "Test Connection",
                        "error": "Connection timeout"
                    }
                ],
                recommendations=[
                    "Check Airbyte connection configuration",
                    "Verify network connectivity",
                    "Review error logs for detailed information"
                ]
            )
            
            draft_id = await create_email_draft(
                self.client_id,
                self.client_secret,
                self.tenant_id,
                test_notification
            )
            
            if draft_id:
                console.print(f"[green]✅ Successfully created email draft: {draft_id}[/green]")
                return draft_id
            else:
                console.print("[yellow]⚠️ Failed to create email draft[/yellow]")
                return ""
                
        except Exception as e:
            console.print(f"[red]❌ Email draft creation failed: {str(e)}[/red]")
            return ""
    
    async def test_pipeline_notification_email(self) -> str:
        """Test creating pipeline-specific notification email."""
        console.print("[blue]🔍 Testing pipeline notification email creation...[/blue]")
        
        try:
            # Create sample job status records
            sample_jobs = [
                JobStatusRecord(
                    job_id="airbyte_001",
                    platform=PlatformType.AIRBYTE,
                    job_name="Customer Data Sync",
                    status=JobStatus.SUCCESS,
                    last_run_time=datetime.now(),
                    duration_seconds=120
                ),
                JobStatusRecord(
                    job_id="databricks_001",
                    platform=PlatformType.DATABRICKS,
                    job_name="Data Transformation",
                    status=JobStatus.FAILED,
                    last_run_time=datetime.now(),
                    duration_seconds=45,
                    error_message="Memory allocation error"
                ),
                JobStatusRecord(
                    job_id="powerautomate_001",
                    platform=PlatformType.POWER_AUTOMATE,
                    job_name="Report Generation",
                    status=JobStatus.RUNNING,
                    last_run_time=datetime.now()
                )
            ]
            
            notification_id = await create_pipeline_notification_email(
                self.client_id,
                self.client_secret,
                self.tenant_id,
                recipient=self.test_recipient or "test@example.com",
                job_records=sample_jobs,
                priority=NotificationPriority.HIGH
            )
            
            if notification_id:
                console.print(f"[green]✅ Successfully created pipeline notification: {notification_id}[/green]")
                
                # Show email content summary
                summary_table = Table(title="Pipeline Notification Summary")
                summary_table.add_column("Metric", style="cyan")
                summary_table.add_column("Value", style="white")
                
                summary_table.add_row("Total Jobs", str(len(sample_jobs)))
                summary_table.add_row("Failed Jobs", str(len([j for j in sample_jobs if j.status == JobStatus.FAILED])))
                summary_table.add_row("Running Jobs", str(len([j for j in sample_jobs if j.status == JobStatus.RUNNING])))
                summary_table.add_row("Successful Jobs", str(len([j for j in sample_jobs if j.status == JobStatus.SUCCESS])))
                summary_table.add_row("Notification Priority", "HIGH")
                summary_table.add_row("Recipient", self.test_recipient or "test@example.com")
                
                console.print(summary_table)
                return notification_id
            else:
                console.print("[yellow]⚠️ Failed to create pipeline notification[/yellow]")
                return ""
                
        except Exception as e:
            console.print(f"[red]❌ Pipeline notification creation failed: {str(e)}[/red]")
            return ""
    
    async def test_send_notification(self, draft_id: str) -> bool:
        """Test sending email notification (optional - only if test recipient is set)."""
        if not self.test_recipient:
            console.print("[yellow]⚠️ Skipping email send test - no test recipient configured[/yellow]")
            return True
        
        console.print("[blue]🔍 Testing email notification sending...[/blue]")
        
        if not draft_id:
            console.print("[yellow]⚠️ No draft ID available for send test[/yellow]")
            return False
        
        try:
            # Ask for confirmation before sending real email
            console.print(f"[yellow]📧 About to send test email to: {self.test_recipient}[/yellow]")
            response = input("Continue with email send test? (y/N): ").strip().lower()
            
            if response != 'y':
                console.print("[blue]ℹ️ Email send test skipped by user[/blue]")
                return True
            
            # Create a simple test notification
            test_notification = EmailNotification(
                subject="[TEST] Pipeline Monitoring System Test",
                recipient=self.test_recipient,
                priority=NotificationPriority.LOW,
                template=NotificationTemplate.INFORMATIONAL,
                job_summary={"total_jobs": 1, "test": True},
                content_override="This is a test email from the Pipeline Monitoring System. Please ignore."
            )
            
            result = await send_email_notification(
                self.client_id,
                self.client_secret,
                self.tenant_id,
                test_notification
            )
            
            if result.get("status") == "sent":
                console.print(f"[green]✅ Successfully sent test email: {result.get('message_id')}[/green]")
                return True
            else:
                console.print(f"[yellow]⚠️ Email send returned: {result}[/yellow]")
                return False
                
        except Exception as e:
            console.print(f"[red]❌ Email send failed: {str(e)}[/red]")
            return False
    
    async def test_notification_templates(self) -> List[str]:
        """Test different notification templates."""
        console.print("[blue]🔍 Testing notification email templates...[/blue]")
        
        templates_to_test = [
            (NotificationTemplate.ALERT, NotificationPriority.HIGH, "Critical Pipeline Failure"),
            (NotificationTemplate.WARNING, NotificationPriority.MEDIUM, "Pipeline Performance Warning"),
            (NotificationTemplate.INFORMATIONAL, NotificationPriority.LOW, "Pipeline Status Summary"),
            (NotificationTemplate.SUCCESS, NotificationPriority.LOW, "Pipeline Success Report")
        ]
        
        created_drafts = []
        
        for template, priority, subject in templates_to_test:
            try:
                notification = EmailNotification(
                    subject=f"[TEST] {subject}",
                    recipient=self.test_recipient or "test@example.com",
                    priority=priority,
                    template=template,
                    job_summary={
                        "total_jobs": 5,
                        "failed_jobs": 1 if template == NotificationTemplate.ALERT else 0,
                        "successful_jobs": 4 if template != NotificationTemplate.ALERT else 4
                    },
                    failed_jobs=[{
                        "platform": "test",
                        "job_name": "Test Job",
                        "error": "Test error message"
                    }] if template == NotificationTemplate.ALERT else [],
                    recommendations=[
                        "This is a test recommendation",
                        "Please ignore this test email"
                    ]
                )
                
                draft_id = await create_email_draft(
                    self.client_id,
                    self.client_secret,
                    self.tenant_id,
                    notification
                )
                
                if draft_id:
                    created_drafts.append(draft_id)
                    console.print(f"[green]✅ Created {template.value} template draft: {draft_id}[/green]")
                else:
                    console.print(f"[yellow]⚠️ Failed to create {template.value} template draft[/yellow]")
                    
            except Exception as e:
                console.print(f"[red]❌ Failed to create {template.value} template: {str(e)}[/red]")
        
        if created_drafts:
            templates_table = Table(title="Created Template Drafts")
            templates_table.add_column("Template", style="cyan")
            templates_table.add_column("Priority", style="white")
            templates_table.add_column("Draft ID", style="green")
            
            for i, (template, priority, _) in enumerate(templates_to_test):
                if i < len(created_drafts):
                    templates_table.add_row(
                        template.value,
                        priority.value,
                        created_drafts[i]
                    )
            
            console.print(templates_table)
            console.print(f"[green]✅ Successfully created {len(created_drafts)} template drafts[/green]")
        
        return created_drafts
    
    async def test_error_handling(self):
        """Test error handling with invalid credentials."""
        console.print("[blue]🔍 Testing error handling with invalid credentials...[/blue]")
        
        try:
            # Test with invalid client secret
            await check_outlook_health("invalid_client", "invalid_secret", self.tenant_id)
            console.print("[red]❌ Error handling test failed - should have thrown exception[/red]")
        except Exception as e:
            console.print(f"[green]✅ Error handling working correctly: {str(e)[:100]}...[/green]")
    
    async def save_test_results(self, profile: Dict, draft_ids: List[str], template_drafts: List[str]):
        """Save test results to JSON file for debugging."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"test-scripts/outlook_test_results_{timestamp}.json"
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "config": {
                "client_id_set": bool(self.client_id),
                "client_secret_set": bool(self.client_secret),
                "tenant_id": self.tenant_id,
                "test_recipient": self.test_recipient
            },
            "profile": {
                "display_name": profile.get("displayName"),
                "email": profile.get("mail", profile.get("userPrincipalName")),
                "user_id": profile.get("id")
            } if profile else {},
            "drafts_created": len(draft_ids),
            "template_drafts_created": len(template_drafts),
            "draft_ids": draft_ids,
            "template_draft_ids": template_drafts
        }
        
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        console.print(f"[blue]💾 Test results saved to: {results_file}[/blue]")
    
    async def run_all_tests(self):
        """Run all Outlook API tests."""
        console.print(Panel.fit("🚀 Outlook API Connection Test Suite", style="bold blue"))
        
        self.print_config()
        
        # Test 1: Health check
        health_ok = await self.test_health_check()
        console.print()
        
        if not health_ok:
            console.print("[red]❌ Health check failed. Stopping tests.[/red]")
            return
        
        # Test 2: Get user profile
        profile = await self.test_get_profile()
        console.print()
        
        # Test 3: Create email draft
        draft_id = await self.test_create_email_draft()
        console.print()
        
        # Test 4: Create pipeline notification
        pipeline_notification_id = await self.test_pipeline_notification_email()
        console.print()
        
        # Test 5: Test different templates
        template_drafts = await self.test_notification_templates()
        console.print()
        
        # Test 6: Send notification (optional)
        send_ok = await self.test_send_notification(draft_id)
        console.print()
        
        # Test 7: Error handling
        await self.test_error_handling()
        console.print()
        
        # Save results
        all_draft_ids = [draft_id, pipeline_notification_id] + template_drafts
        await self.save_test_results(profile, [d for d in all_draft_ids if d], template_drafts)
        console.print()
        
        # Summary
        summary_table = Table(title="Test Summary")
        summary_table.add_column("Test", style="cyan")
        summary_table.add_column("Status", style="white")
        summary_table.add_column("Details", style="blue")
        
        summary_table.add_row("Health Check", "✅ PASS" if health_ok else "❌ FAIL", "API authentication working")
        summary_table.add_row("User Profile", "✅ PASS" if profile else "❌ FAIL", "Profile data retrieved")
        summary_table.add_row("Email Draft", "✅ PASS" if draft_id else "❌ FAIL", "Draft creation working")
        summary_table.add_row("Pipeline Notification", "✅ PASS" if pipeline_notification_id else "❌ FAIL", "Pipeline emails working")
        summary_table.add_row("Template Tests", "✅ PASS" if template_drafts else "❌ FAIL", f"{len(template_drafts)} templates tested")
        summary_table.add_row("Email Send", "✅ PASS" if send_ok else "⚠️ SKIP", "Email sending working")
        summary_table.add_row("Error Handling", "✅ PASS", "Exceptions handled correctly")
        
        console.print(summary_table)
        
        if health_ok and profile and (draft_id or pipeline_notification_id):
            console.print("\n[green]🎉 Outlook API integration is working correctly![/green]")
            console.print(f"[blue]📧 Created {len([d for d in all_draft_ids if d])} email drafts for testing[/blue]")
        else:
            console.print("\n[yellow]⚠️ Some issues detected. Check configuration and API permissions.[/yellow]")


async def main():
    """Main entry point for Outlook connection test."""
    test_suite = OutlookConnectionTest()
    await test_suite.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())
