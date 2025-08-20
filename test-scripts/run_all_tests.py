#!/usr/bin/env python3
"""
Test runner script that executes all platform tests with organized output.
This script provides a unified interface to run all individual and integration tests.
"""

import asyncio
import os
import sys
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple
import json
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, TaskID
from rich.live import Live
from rich.layout import Layout
from rich import print as rprint

console = Console()

class TestRunner:
    """Unified test runner for all platform tests."""
    
    def __init__(self):
        load_dotenv()
        self.test_scripts = [
            ("Airbyte", "test_airbyte_connection.py", "AIRBYTE_API_KEY"),
            ("Databricks", "test_databricks_connection.py", "DATABRICKS_API_TOKEN"),
            ("Power Automate", "test_powerautomate_connection.py", "POWER_AUTOMATE_CLIENT_ID"),
            ("Snowflake Task", "test_snowflake_task_connection.py", "SNOWFLAKE_ACCOUNT"),
            ("Snowflake DB", "test_snowflake_db_connection.py", "SNOWFLAKE_ACCOUNT"),
            ("Outlook", "test_outlook_connection.py", "OUTLOOK_CLIENT_ID"),
        ]
        
        self.integration_script = ("Integration", "test_all_platforms_integration.py", None)
        
        self.test_results = {}
        self.start_time = datetime.now()
        self.script_dir = Path(__file__).parent
    
    def check_environment(self) -> Dict[str, bool]:
        """Check which platforms are properly configured."""
        env_status = {}
        
        for platform_name, script_name, env_var in self.test_scripts:
            if env_var:
                env_status[platform_name] = bool(os.getenv(env_var))
            else:
                env_status[platform_name] = True
        
        return env_status
    
    def print_header(self):
        """Print test runner header."""
        console.print(Panel.fit(
            "🧪 Data Pipeline Monitoring Test Runner\n"
            "Automated execution of all platform connection tests",
            style="bold blue"
        ))
        console.print()
    
    def print_environment_summary(self, env_status: Dict[str, bool]):
        """Print environment configuration summary."""
        env_table = Table(title="Environment Configuration Summary")
        env_table.add_column("Platform", style="cyan")
        env_table.add_column("Status", style="white")
        env_table.add_column("Will Run", style="green")
        
        configured_count = 0
        for platform, is_configured in env_status.items():
            status = "✅ CONFIGURED" if is_configured else "❌ MISSING CONFIG"
            will_run = "Yes" if is_configured else "Skip"
            
            if is_configured:
                configured_count += 1
            
            env_table.add_row(platform, status, will_run)
        
        console.print(env_table)
        console.print(f"[blue]📊 {configured_count}/{len(env_status)} platforms configured[/blue]")
        console.print()
    
    async def run_test_script(self, platform_name: str, script_name: str) -> Tuple[bool, Dict]:
        """Run an individual test script."""
        script_path = self.script_dir / script_name
        
        try:
            # Run the test script
            result = subprocess.run([
                sys.executable, str(script_path)
            ], capture_output=True, text=True, timeout=300)  # 5 minute timeout
            
            success = result.returncode == 0
            
            # Try to find and parse the result file
            result_data = {}
            timestamp_pattern = datetime.now().strftime("%Y%m%d")
            
            # Look for result files created today
            result_files = list(self.script_dir.glob(f"*test_results_{timestamp_pattern}_*.json"))
            
            if result_files:
                # Get the most recent result file
                latest_file = max(result_files, key=lambda f: f.stat().st_mtime)
                try:
                    with open(latest_file, 'r') as f:
                        result_data = json.load(f)
                except:
                    pass
            
            return success, {
                "success": success,
                "return_code": result.returncode,
                "stdout": result.stdout[-1000:] if result.stdout else "",  # Last 1000 chars
                "stderr": result.stderr[-1000:] if result.stderr else "",  # Last 1000 chars
                "result_data": result_data
            }
            
        except subprocess.TimeoutExpired:
            return False, {
                "success": False,
                "error": "Test timed out after 5 minutes",
                "return_code": -1
            }
        except Exception as e:
            return False, {
                "success": False,
                "error": str(e),
                "return_code": -1
            }
    
    async def run_individual_tests(self, env_status: Dict[str, bool]) -> Dict[str, Dict]:
        """Run all individual platform tests."""
        console.print("[blue]🔍 Running individual platform tests...[/blue]")
        console.print()
        
        results = {}
        
        with Progress() as progress:
            # Create progress tasks
            tasks = {}
            for platform_name, script_name, env_var in self.test_scripts:
                if env_status.get(platform_name, False):
                    tasks[platform_name] = progress.add_task(f"Testing {platform_name}...", total=1)
            
            # Run tests sequentially to avoid resource conflicts
            for platform_name, script_name, env_var in self.test_scripts:
                if not env_status.get(platform_name, False):
                    console.print(f"[yellow]⏭️  Skipping {platform_name} - not configured[/yellow]")
                    continue
                
                console.print(f"[blue]🧪 Running {platform_name} test...[/blue]")
                
                start_time = datetime.now()
                success, result_data = await self.run_test_script(platform_name, script_name)
                end_time = datetime.now()
                
                duration = (end_time - start_time).total_seconds()
                result_data["duration"] = duration
                
                results[platform_name] = result_data
                
                if success:
                    console.print(f"[green]✅ {platform_name} test completed successfully ({duration:.1f}s)[/green]")
                else:
                    console.print(f"[red]❌ {platform_name} test failed ({duration:.1f}s)[/red]")
                    if result_data.get("stderr"):
                        console.print(f"[red]   Error: {result_data['stderr'][:200]}...[/red]")
                
                progress.update(tasks[platform_name], completed=1)
                console.print()
        
        return results
    
    async def run_integration_test(self, individual_results: Dict[str, Dict]) -> Dict:
        """Run the integration test."""
        console.print("[blue]🔗 Running integration test...[/blue]")
        
        # Check if we have enough successful individual tests
        successful_tests = [name for name, result in individual_results.items() if result.get("success", False)]
        
        if len(successful_tests) < 2:
            console.print("[yellow]⚠️ Skipping integration test - need at least 2 successful platform tests[/yellow]")
            return {"skipped": True, "reason": "Insufficient successful platform tests"}
        
        console.print(f"[blue]📊 Running integration test with {len(successful_tests)} successful platforms[/blue]")
        
        start_time = datetime.now()
        success, result_data = await self.run_test_script("Integration", self.integration_script[1])
        end_time = datetime.now()
        
        duration = (end_time - start_time).total_seconds()
        result_data["duration"] = duration
        
        if success:
            console.print(f"[green]✅ Integration test completed successfully ({duration:.1f}s)[/green]")
        else:
            console.print(f"[red]❌ Integration test failed ({duration:.1f}s)[/red]")
        
        console.print()
        return result_data
    
    def generate_summary_report(self, env_status: Dict[str, bool], individual_results: Dict[str, Dict], integration_result: Dict):
        """Generate final summary report."""
        total_duration = (datetime.now() - self.start_time).total_seconds()
        
        # Summary statistics
        configured_platforms = sum(1 for configured in env_status.values() if configured)
        successful_tests = sum(1 for result in individual_results.values() if result.get("success", False))
        failed_tests = len(individual_results) - successful_tests
        
        # Create summary table
        summary_table = Table(title="Test Execution Summary")
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="white")
        summary_table.add_column("Status", style="green")
        
        summary_table.add_row("Total Duration", f"{total_duration:.1f} seconds", "⏱️")
        summary_table.add_row("Configured Platforms", str(configured_platforms), "🔧")
        summary_table.add_row("Successful Tests", str(successful_tests), "✅")
        summary_table.add_row("Failed Tests", str(failed_tests), "❌" if failed_tests > 0 else "✅")
        summary_table.add_row("Integration Test", 
                             "PASS" if integration_result.get("success") else "FAIL/SKIP",
                             "✅" if integration_result.get("success") else "❌")
        
        console.print(summary_table)
        console.print()
        
        # Detailed results table
        details_table = Table(title="Detailed Test Results")
        details_table.add_column("Platform", style="cyan")
        details_table.add_column("Status", style="white")
        details_table.add_column("Duration", style="blue")
        details_table.add_column("Notes", style="yellow")
        
        for platform_name, script_name, env_var in self.test_scripts:
            if not env_status.get(platform_name, False):
                details_table.add_row(platform_name, "SKIPPED", "0.0s", "Not configured")
            elif platform_name in individual_results:
                result = individual_results[platform_name]
                status = "PASS" if result.get("success") else "FAIL"
                duration = f"{result.get('duration', 0):.1f}s"
                notes = "Completed successfully" if result.get("success") else "Check logs for details"
                details_table.add_row(platform_name, status, duration, notes)
            else:
                details_table.add_row(platform_name, "ERROR", "0.0s", "Test not executed")
        
        # Add integration test row
        if integration_result.get("skipped"):
            details_table.add_row("Integration", "SKIPPED", "0.0s", integration_result.get("reason", ""))
        else:
            status = "PASS" if integration_result.get("success") else "FAIL"
            duration = f"{integration_result.get('duration', 0):.1f}s"
            notes = "Full workflow tested" if integration_result.get("success") else "Check integration logs"
            details_table.add_row("Integration", status, duration, notes)
        
        console.print(details_table)
        console.print()
        
        # Final assessment
        if successful_tests >= 4 and integration_result.get("success"):
            assessment_panel = Panel.fit(
                "🎉 EXCELLENT! System is ready for production monitoring.\n"
                "All major platforms are working and integration test passed.",
                style="bold green"
            )
        elif successful_tests >= 2:
            assessment_panel = Panel.fit(
                "⚠️ PARTIAL SUCCESS. Core functionality is working.\n"
                "Some platforms may need configuration attention.",
                style="bold yellow"
            )
        else:
            assessment_panel = Panel.fit(
                "❌ NEEDS ATTENTION. Multiple platform connection issues detected.\n"
                "Please review configuration and try again.",
                style="bold red"
            )
        
        console.print(assessment_panel)
        console.print()
        
        # Save comprehensive report
        report = {
            "test_run_info": {
                "timestamp": self.start_time.isoformat(),
                "total_duration_seconds": total_duration,
                "configured_platforms": configured_platforms,
                "successful_tests": successful_tests,
                "failed_tests": failed_tests
            },
            "environment_status": env_status,
            "individual_results": individual_results,
            "integration_result": integration_result
        }
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.script_dir / f"test_runner_report_{timestamp}.json"
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        console.print(f"[blue]📄 Comprehensive report saved to: {report_file}[/blue]")
    
    async def run_all_tests(self):
        """Run all tests with organized output."""
        self.print_header()
        
        # Check environment
        env_status = self.check_environment()
        self.print_environment_summary(env_status)
        
        # Ask for confirmation if many tests will be skipped
        configured_count = sum(1 for configured in env_status.values() if configured)
        if configured_count < 3:
            console.print(f"[yellow]⚠️ Only {configured_count} platforms are configured.[/yellow]")
            response = input("Continue with limited testing? (y/N): ").strip().lower()
            if response != 'y':
                console.print("[blue]ℹ️ Test execution cancelled by user[/blue]")
                return
            console.print()
        
        # Run individual platform tests
        individual_results = await self.run_individual_tests(env_status)
        
        # Run integration test
        integration_result = await self.run_integration_test(individual_results)
        
        # Generate summary report
        self.generate_summary_report(env_status, individual_results, integration_result)


async def main():
    """Main entry point for test runner."""
    runner = TestRunner()
    await runner.run_all_tests()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️ Test execution interrupted by user[/yellow]")
    except Exception as e:
        console.print(f"\n[red]❌ Test runner failed with error: {str(e)}[/red]")
        sys.exit(1)
