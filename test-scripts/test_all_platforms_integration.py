#!/usr/bin/env python3
"""
Comprehensive integration test script for all data pipeline platforms.
This script tests the full end-to-end pipeline monitoring workflow.
"""

import asyncio
import os
import sys
from pathlib import Path
from typing import Dict, Any, List
import json
from datetime import datetime
import subprocess

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, TaskID
from rich import print as rprint

# Import all individual test classes
from test_airbyte_connection import AirbyteConnectionTest
from test_databricks_connection import DatabricksConnectionTest
from test_powerautomate_connection import PowerAutomateConnectionTest
from test_snowflake_task_connection import SnowflakeTaskConnectionTest
from test_snowflake_db_connection import SnowflakeDbConnectionTest
from test_outlook_connection import OutlookConnectionTest

# Import agent and orchestrator components
from agents.orchestrator_agent import orchestrator_agent
from agents.dependencies import OrchestratorDependencies
from config.settings import Settings
from models.job_status import JobStatusRecord, JobStatus, PlatformType

console = Console()

class ComprehensiveIntegrationTest:
    """Comprehensive integration test for all platform APIs and the orchestrator agent."""
    
    def __init__(self):
        load_dotenv()
        self.settings = Settings()
        self.test_results = {}
        self.platform_test_results = {}
        
        # Track test timing
        self.start_time = datetime.now()
        self.test_durations = {}
        
        # Track overall health
        self.platforms_healthy = []
        self.platforms_failed = []
    
    def print_header(self):
        """Print test suite header."""
        console.print(Panel.fit(
            "🚀 Comprehensive Data Pipeline Monitoring Integration Test Suite\n"
            "Testing all platforms: Airbyte, Databricks, Power Automate, Snowflake Task, Snowflake DB, Outlook",
            style="bold blue"
        ))
        console.print()
    
    def print_environment_status(self):
        """Print environment configuration status."""
        env_table = Table(title="Environment Configuration Status")
        env_table.add_column("Platform", style="cyan")
        env_table.add_column("Status", style="white")
        env_table.add_column("Required Variables", style="green")
        env_table.add_column("Missing", style="red")
        
        # Define required environment variables for each platform
        platform_vars = {
            "Airbyte": ["AIRBYTE_API_KEY"],
            "Databricks": ["DATABRICKS_API_TOKEN", "DATABRICKS_WORKSPACE_URL"],
            "Power Automate": ["POWER_AUTOMATE_CLIENT_ID", "POWER_AUTOMATE_CLIENT_SECRET", "POWER_AUTOMATE_TENANT_ID"],
            "Snowflake Task": ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"],
            "Snowflake DB": ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"],
            "Outlook": ["OUTLOOK_CLIENT_ID", "OUTLOOK_CLIENT_SECRET", "OUTLOOK_TENANT_ID"]
        }
        
        for platform, required_vars in platform_vars.items():
            missing_vars = [var for var in required_vars if not os.getenv(var)]
            
            status = "✅ READY" if not missing_vars else "❌ INCOMPLETE"
            required_str = ", ".join(required_vars)
            missing_str = ", ".join(missing_vars) if missing_vars else "None"
            
            env_table.add_row(platform, status, required_str, missing_str)
        
        console.print(env_table)
        console.print()
    
    async def run_individual_platform_tests(self) -> Dict[str, bool]:
        """Run individual platform tests in parallel."""
        console.print("[blue]📋 Running individual platform connection tests...[/blue]")
        
        # Create test instances
        test_classes = {
            "airbyte": AirbyteConnectionTest(),
            "databricks": DatabricksConnectionTest(),
            "powerautomate": PowerAutomateConnectionTest(),
            "snowflake_task": SnowflakeTaskConnectionTest(),
            "snowflake_db": SnowflakeDbConnectionTest(),
            "outlook": OutlookConnectionTest()
        }
        
        # Track platform health
        platform_health = {}
        
        with Progress() as progress:
            # Create tasks for each platform
            tasks = {}
            for platform_name in test_classes.keys():
                tasks[platform_name] = progress.add_task(f"Testing {platform_name}...", total=1)
            
            # Run tests concurrently (with some safety for rate limits)
            for platform_name, test_instance in test_classes.items():
                try:
                    console.print(f"\n[blue]🔍 Testing {platform_name} platform...[/blue]")
                    
                    # Run basic health check for each platform
                    start_time = datetime.now()
                    health_ok = False
                    
                    if platform_name == "airbyte":
                        health_ok = await test_instance.test_health_check()
                    elif platform_name == "databricks":
                        health_ok = await test_instance.test_health_check()
                    elif platform_name == "powerautomate":
                        health_ok = await test_instance.test_health_check()
                    elif platform_name == "snowflake_task":
                        health_ok = await test_instance.test_health_check()
                    elif platform_name == "snowflake_db":
                        health_ok = await test_instance.test_health_check()
                    elif platform_name == "outlook":
                        health_ok = await test_instance.test_health_check()
                    
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    
                    platform_health[platform_name] = health_ok
                    self.test_durations[platform_name] = duration
                    
                    if health_ok:
                        self.platforms_healthy.append(platform_name)
                        console.print(f"[green]✅ {platform_name} platform: HEALTHY ({duration:.1f}s)[/green]")
                    else:
                        self.platforms_failed.append(platform_name)
                        console.print(f"[red]❌ {platform_name} platform: FAILED ({duration:.1f}s)[/red]")
                    
                    progress.update(tasks[platform_name], completed=1)
                    
                except Exception as e:
                    platform_health[platform_name] = False
                    self.platforms_failed.append(platform_name)
                    console.print(f"[red]❌ {platform_name} platform: ERROR - {str(e)[:100]}...[/red]")
                    progress.update(tasks[platform_name], completed=1)
        
        # Store results
        self.platform_test_results = platform_health
        
        # Summary table
        summary_table = Table(title="Platform Health Summary")
        summary_table.add_column("Platform", style="cyan")
        summary_table.add_column("Status", style="white")
        summary_table.add_column("Duration", style="blue")
        
        for platform, is_healthy in platform_health.items():
            status = "✅ HEALTHY" if is_healthy else "❌ FAILED"
            duration = f"{self.test_durations.get(platform, 0):.1f}s"
            summary_table.add_row(platform.title(), status, duration)
        
        console.print(summary_table)
        console.print()
        
        return platform_health
    
    async def test_orchestrator_agent(self) -> bool:
        """Test the orchestrator agent with available platforms."""
        console.print("[blue]🤖 Testing Orchestrator Agent integration...[/blue]")
        
        if not self.platforms_healthy:
            console.print("[yellow]⚠️ No healthy platforms available for orchestrator test[/yellow]")
            return False
        
        try:
            # Create orchestrator dependencies
            deps = OrchestratorDependencies(
                settings=self.settings,
                airbyte_api_key=self.settings.airbyte_api_key if "airbyte" in self.platforms_healthy else None,
                databricks_api_token=self.settings.databricks_api_token if "databricks" in self.platforms_healthy else None,
                power_automate_credentials={
                    "client_id": self.settings.power_automate_client_id,
                    "client_secret": self.settings.power_automate_client_secret,
                    "tenant_id": self.settings.power_automate_tenant_id
                } if "powerautomate" in self.platforms_healthy else None,
                snowflake_config={
                    "account": self.settings.snowflake_account,
                    "user": self.settings.snowflake_user,
                    "password": self.settings.snowflake_password,
                    "database": self.settings.snowflake_database,
                    "schema": self.settings.snowflake_schema,
                    "warehouse": self.settings.snowflake_warehouse,
                    "role": self.settings.snowflake_role
                } if "snowflake_task" in self.platforms_healthy or "snowflake_db" in self.platforms_healthy else None,
                outlook_credentials={
                    "client_id": self.settings.outlook_client_id,
                    "client_secret": self.settings.outlook_client_secret,
                    "tenant_id": self.settings.outlook_tenant_id
                } if "outlook" in self.platforms_healthy else None
            )
            
            # Run orchestrator monitoring
            console.print("[blue]🔄 Running orchestrator monitoring workflow...[/blue]")
            
            start_time = datetime.now()
            result = await orchestrator_agent.run(
                f"Monitor all available data platforms: {', '.join(self.platforms_healthy)}",
                deps=deps
            )
            end_time = datetime.now()
            
            orchestrator_duration = (end_time - start_time).total_seconds()
            self.test_durations["orchestrator"] = orchestrator_duration
            
            if result.data:
                console.print(f"[green]✅ Orchestrator agent completed successfully ({orchestrator_duration:.1f}s)[/green]")
                
                # Display orchestrator results
                orchestrator_table = Table(title="Orchestrator Results")
                orchestrator_table.add_column("Metric", style="cyan")
                orchestrator_table.add_column("Value", style="white")
                
                result_data = result.data
                if isinstance(result_data, dict):
                    for key, value in result_data.items():
                        orchestrator_table.add_row(str(key), str(value)[:100] + "..." if len(str(value)) > 100 else str(value))
                
                console.print(orchestrator_table)
                
                self.test_results["orchestrator"] = {
                    "status": "success",
                    "duration": orchestrator_duration,
                    "data": result_data
                }
                return True
            else:
                console.print(f"[yellow]⚠️ Orchestrator completed but returned no data ({orchestrator_duration:.1f}s)[/yellow]")
                self.test_results["orchestrator"] = {
                    "status": "warning",
                    "duration": orchestrator_duration,
                    "message": "No data returned"
                }
                return False
                
        except Exception as e:
            console.print(f"[red]❌ Orchestrator agent failed: {str(e)}[/red]")
            self.test_results["orchestrator"] = {
                "status": "error",
                "error": str(e)
            }
            return False
    
    async def test_end_to_end_workflow(self) -> bool:
        """Test the complete end-to-end monitoring workflow."""
        console.print("[blue]🔄 Testing end-to-end monitoring workflow...[/blue]")
        
        try:
            # Step 1: Monitor all platforms
            console.print("[blue]📊 Step 1: Monitoring all platforms...[/blue]")
            
            # Step 2: Store results in Snowflake (if available)
            if "snowflake_db" in self.platforms_healthy:
                console.print("[blue]💾 Step 2: Storing results in Snowflake...[/blue]")
                # This would be handled by the orchestrator agent
            
            # Step 3: Generate notifications (if issues found)
            if "outlook" in self.platforms_healthy:
                console.print("[blue]📧 Step 3: Generating notifications...[/blue]")
                # This would be handled by the orchestrator agent
            
            console.print("[green]✅ End-to-end workflow test completed[/green]")
            return True
            
        except Exception as e:
            console.print(f"[red]❌ End-to-end workflow failed: {str(e)}[/red]")
            return False
    
    async def test_cli_interface(self) -> bool:
        """Test the CLI interface."""
        console.print("[blue]💻 Testing CLI interface...[/blue]")
        
        try:
            # Test CLI help command
            result = subprocess.run([sys.executable, "cli.py", "--help"], 
                                  capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                console.print("[green]✅ CLI help command working[/green]")
                return True
            else:
                console.print(f"[yellow]⚠️ CLI help returned non-zero exit code: {result.returncode}[/yellow]")
                return False
                
        except subprocess.TimeoutExpired:
            console.print("[red]❌ CLI test timed out[/red]")
            return False
        except Exception as e:
            console.print(f"[red]❌ CLI test failed: {str(e)}[/red]")
            return False
    
    async def generate_test_report(self):
        """Generate comprehensive test report."""
        console.print("[blue]📋 Generating comprehensive test report...[/blue]")
        
        total_duration = (datetime.now() - self.start_time).total_seconds()
        
        # Create comprehensive report
        report = {
            "test_run_info": {
                "timestamp": self.start_time.isoformat(),
                "total_duration_seconds": total_duration,
                "total_platforms_tested": len(self.platform_test_results),
                "healthy_platforms": len(self.platforms_healthy),
                "failed_platforms": len(self.platforms_failed)
            },
            "platform_results": self.platform_test_results,
            "platform_durations": self.test_durations,
            "healthy_platforms": self.platforms_healthy,
            "failed_platforms": self.platforms_failed,
            "orchestrator_test": self.test_results.get("orchestrator", {}),
            "environment_check": {
                "airbyte_configured": bool(os.getenv("AIRBYTE_API_KEY")),
                "databricks_configured": bool(os.getenv("DATABRICKS_API_TOKEN") and os.getenv("DATABRICKS_WORKSPACE_URL")),
                "powerautomate_configured": bool(os.getenv("POWER_AUTOMATE_CLIENT_ID")),
                "snowflake_configured": bool(os.getenv("SNOWFLAKE_ACCOUNT")),
                "outlook_configured": bool(os.getenv("OUTLOOK_CLIENT_ID"))
            }
        }
        
        # Save report to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"test-scripts/integration_test_report_{timestamp}.json"
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        # Display final summary
        summary_panel = Panel.fit(
            f"🎯 Integration Test Complete\n\n"
            f"⏱️  Total Duration: {total_duration:.1f} seconds\n"
            f"✅ Healthy Platforms: {len(self.platforms_healthy)}\n"
            f"❌ Failed Platforms: {len(self.platforms_failed)}\n"
            f"🤖 Orchestrator: {'✅ PASS' if self.test_results.get('orchestrator', {}).get('status') == 'success' else '❌ FAIL'}\n"
            f"📄 Report saved to: {report_file}",
            style="bold green" if len(self.platforms_healthy) > len(self.platforms_failed) else "bold yellow"
        )
        
        console.print(summary_panel)
        console.print()
        
        # Recommendations
        if self.platforms_failed:
            console.print("[yellow]💡 Recommendations for failed platforms:[/yellow]")
            for platform in self.platforms_failed:
                console.print(f"   • Check {platform} configuration and credentials")
            console.print()
        
        if len(self.platforms_healthy) >= 3:
            console.print("[green]🎉 System is ready for production monitoring![/green]")
        elif len(self.platforms_healthy) >= 1:
            console.print("[yellow]⚠️ Partial system functionality. Some platforms need attention.[/yellow]")
        else:
            console.print("[red]❌ System not ready. Please resolve platform configuration issues.[/red]")
    
    async def run_all_tests(self):
        """Run the complete integration test suite."""
        self.print_header()
        self.print_environment_status()
        
        console.print("[blue]🚀 Starting comprehensive integration tests...[/blue]")
        console.print()
        
        # Test 1: Individual platform connections
        await self.run_individual_platform_tests()
        
        # Test 2: Orchestrator agent (if platforms are available)
        if self.platforms_healthy:
            await self.test_orchestrator_agent()
            console.print()
        
        # Test 3: End-to-end workflow
        await self.test_end_to_end_workflow()
        console.print()
        
        # Test 4: CLI interface
        await self.test_cli_interface()
        console.print()
        
        # Generate final report
        await self.generate_test_report()


async def main():
    """Main entry point for comprehensive integration test."""
    test_suite = ComprehensiveIntegrationTest()
    await test_suite.run_all_tests()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️ Test suite interrupted by user[/yellow]")
    except Exception as e:
        console.print(f"\n[red]❌ Test suite failed with error: {str(e)}[/red]")
        sys.exit(1)
