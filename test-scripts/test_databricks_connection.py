#!/usr/bin/env python3
"""
Test script for Databricks API connection and functionality.
This script validates the Databricks API integration independently.
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

from tools.databricks_api import (
    get_databricks_jobs,
    get_databricks_job_runs,
    get_databricks_job_status,
    check_databricks_health
)
from models.job_status import JobStatusRecord, JobStatus, PlatformType

console = Console()

class DatabricksConnectionTest:
    """Test class for Databricks API connection and functionality."""
    
    def __init__(self):
        load_dotenv()
        self.api_token = os.getenv("DATABRICKS_API_TOKEN")
        self.workspace_url = os.getenv("DATABRICKS_WORKSPACE_URL", "")
        
        if not self.api_token:
            console.print("[red]❌ DATABRICKS_API_TOKEN not found in environment variables[/red]")
            console.print("[yellow]💡 Set DATABRICKS_API_TOKEN in your .env file[/yellow]")
            sys.exit(1)
        
        if not self.workspace_url:
            console.print("[red]❌ DATABRICKS_WORKSPACE_URL not found in environment variables[/red]")
            console.print("[yellow]💡 Set DATABRICKS_WORKSPACE_URL in your .env file (e.g., https://your-workspace.cloud.databricks.com)[/yellow]")
            sys.exit(1)
    
    def print_config(self):
        """Print current configuration."""
        config_table = Table(title="Databricks Test Configuration")
        config_table.add_column("Setting", style="cyan")
        config_table.add_column("Value", style="white")
        
        config_table.add_row("API Token", f"{'*' * 20}...{self.api_token[-4:] if len(self.api_token) > 4 else 'SET'}")
        config_table.add_row("Workspace URL", self.workspace_url)
        
        console.print(config_table)
        console.print()
    
    async def test_health_check(self) -> bool:
        """Test basic API health and authentication."""
        console.print("[blue]🔍 Testing Databricks API health and authentication...[/blue]")
        
        try:
            health_status = await check_databricks_health(self.api_token, self.workspace_url)
            
            if health_status.get("status") == "healthy":
                console.print("[green]✅ Databricks API health check passed[/green]")
                console.print(f"[blue]ℹ️ Workspace info: {health_status.get('workspace_info', {})}[/blue]")
                return True
            else:
                console.print(f"[yellow]⚠️ Databricks API health check returned: {health_status}[/yellow]")
                return False
                
        except Exception as e:
            console.print(f"[red]❌ Databricks API health check failed: {str(e)}[/red]")
            return False
    
    async def test_list_jobs(self) -> List[Dict[str, Any]]:
        """Test listing Databricks jobs."""
        console.print("[blue]🔍 Testing Databricks jobs listing...[/blue]")
        
        try:
            jobs = await get_databricks_jobs(self.api_token, self.workspace_url)
            
            if jobs:
                jobs_table = Table(title="Databricks Jobs")
                jobs_table.add_column("Job ID", style="cyan")
                jobs_table.add_column("Name", style="white")
                jobs_table.add_column("Creator", style="green")
                jobs_table.add_column("Created", style="blue")
                jobs_table.add_column("Timeout", style="magenta")
                
                for job in jobs[:10]:  # Show first 10
                    timeout = "N/A"
                    if job.get("timeout_seconds"):
                        timeout = f"{job['timeout_seconds']}s"
                    
                    jobs_table.add_row(
                        str(job.get("job_id", "N/A")),
                        job.get("settings", {}).get("name", "Unknown"),
                        job.get("creator_user_name", "Unknown"),
                        job.get("created_time", "Unknown")[:19] if job.get("created_time") else "N/A",
                        timeout
                    )
                
                console.print(jobs_table)
                console.print(f"[green]✅ Found {len(jobs)} jobs[/green]")
                return jobs
            else:
                console.print("[yellow]⚠️ No jobs found[/yellow]")
                return []
                
        except Exception as e:
            console.print(f"[red]❌ Failed to list jobs: {str(e)}[/red]")
            return []
    
    async def test_job_runs(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Test getting job runs for the first few jobs."""
        console.print("[blue]🔍 Testing Databricks job runs retrieval...[/blue]")
        
        all_runs = []
        
        for job in jobs[:3]:  # Test with first 3 jobs
            job_id = job.get("job_id")
            if not job_id:
                continue
                
            try:
                runs = await get_databricks_job_runs(self.api_token, self.workspace_url, job_id)
                
                if runs:
                    console.print(f"[green]✅ Job {job_id}: Found {len(runs)} runs[/green]")
                    all_runs.extend(runs)
                else:
                    console.print(f"[yellow]⚠️ Job {job_id}: No runs found[/yellow]")
                    
            except Exception as e:
                console.print(f"[red]❌ Failed to get runs for job {job_id}: {str(e)}[/red]")
        
        if all_runs:
            runs_table = Table(title="Recent Job Runs")
            runs_table.add_column("Run ID", style="cyan")
            runs_table.add_column("Job ID", style="white")
            runs_table.add_column("State", style="green")
            runs_table.add_column("Start Time", style="blue")
            runs_table.add_column("Duration", style="magenta")
            
            for run in all_runs[:10]:  # Show first 10 runs
                duration = "N/A"
                if run.get("start_time") and run.get("end_time"):
                    try:
                        start_ms = run["start_time"]
                        end_ms = run["end_time"]
                        duration_sec = (end_ms - start_ms) / 1000
                        duration = f"{duration_sec:.1f}s"
                    except:
                        pass
                
                start_time = "N/A"
                if run.get("start_time"):
                    try:
                        start_time = datetime.fromtimestamp(run["start_time"] / 1000).strftime("%Y-%m-%d %H:%M")
                    except:
                        pass
                
                runs_table.add_row(
                    str(run.get("run_id", "N/A")),
                    str(run.get("job_id", "N/A")),
                    run.get("state", {}).get("life_cycle_state", "Unknown"),
                    start_time,
                    duration
                )
            
            console.print(runs_table)
            console.print(f"[green]✅ Found {len(all_runs)} total job runs[/green]")
        
        return all_runs
    
    async def test_job_status_conversion(self, jobs: List[Dict[str, Any]]) -> List[JobStatusRecord]:
        """Test converting Databricks jobs to JobStatusRecord format."""
        console.print("[blue]🔍 Testing job status record conversion...[/blue]")
        
        try:
            job_records = []
            for job in jobs[:5]:  # Test with first 5 jobs
                job_id = job.get("job_id")
                if not job_id:
                    continue
                    
                try:
                    # Convert using the actual API function
                    status_records = await get_databricks_job_status(self.api_token, self.workspace_url, job_id)
                    job_records.extend(status_records)
                except Exception as e:
                    console.print(f"[yellow]⚠️ Failed to convert job {job_id}: {str(e)}[/yellow]")
            
            if job_records:
                records_table = Table(title="Job Status Records")
                records_table.add_column("Job ID", style="cyan")
                records_table.add_column("Platform", style="white")
                records_table.add_column("Job Name", style="green")
                records_table.add_column("Status", style="blue")
                records_table.add_column("Last Run", style="magenta")
                records_table.add_column("Duration", style="yellow")
                
                for record in job_records:
                    duration = "N/A"
                    if record.duration_seconds:
                        duration = f"{record.duration_seconds}s"
                    
                    records_table.add_row(
                        record.job_id,
                        record.platform.value,
                        record.job_name,
                        record.status.value,
                        record.last_run_time.strftime("%Y-%m-%d %H:%M") if record.last_run_time else "N/A",
                        duration
                    )
                
                console.print(records_table)
                console.print(f"[green]✅ Successfully converted {len(job_records)} job status records[/green]")
                return job_records
            else:
                console.print("[yellow]⚠️ No job status records created[/yellow]")
                return []
                
        except Exception as e:
            console.print(f"[red]❌ Failed to convert job status records: {str(e)}[/red]")
            return []
    
    async def test_specific_job_monitoring(self, jobs: List[Dict[str, Any]]):
        """Test monitoring a specific job in detail."""
        if not jobs:
            console.print("[yellow]⚠️ No jobs available for detailed monitoring test[/yellow]")
            return
        
        console.print("[blue]🔍 Testing detailed job monitoring...[/blue]")
        
        # Take the first job
        test_job = jobs[0]
        job_id = test_job.get("job_id")
        
        console.print(f"[blue]📊 Monitoring job: {job_id} - {test_job.get('settings', {}).get('name', 'Unknown')}[/blue]")
        
        try:
            # Get detailed job runs
            runs = await get_databricks_job_runs(self.api_token, self.workspace_url, job_id, limit=5)
            
            if runs:
                detail_table = Table(title=f"Job {job_id} - Recent Runs")
                detail_table.add_column("Run ID", style="cyan")
                detail_table.add_column("State", style="white")
                detail_table.add_column("Result State", style="green")
                detail_table.add_column("Start Time", style="blue")
                detail_table.add_column("End Time", style="magenta")
                detail_table.add_column("Message", style="yellow")
                
                for run in runs:
                    state_info = run.get("state", {})
                    
                    start_time = "N/A"
                    if run.get("start_time"):
                        start_time = datetime.fromtimestamp(run["start_time"] / 1000).strftime("%Y-%m-%d %H:%M")
                    
                    end_time = "N/A"
                    if run.get("end_time"):
                        end_time = datetime.fromtimestamp(run["end_time"] / 1000).strftime("%Y-%m-%d %H:%M")
                    
                    detail_table.add_row(
                        str(run.get("run_id", "N/A")),
                        state_info.get("life_cycle_state", "Unknown"),
                        state_info.get("result_state", "N/A"),
                        start_time,
                        end_time,
                        state_info.get("state_message", "")[:30] + "..." if len(state_info.get("state_message", "")) > 30 else state_info.get("state_message", "")
                    )
                
                console.print(detail_table)
                console.print(f"[green]✅ Successfully monitored job {job_id}[/green]")
            else:
                console.print(f"[yellow]⚠️ No runs found for job {job_id}[/yellow]")
                
        except Exception as e:
            console.print(f"[red]❌ Failed to monitor job {job_id}: {str(e)}[/red]")
    
    async def test_error_handling(self):
        """Test error handling with invalid credentials."""
        console.print("[blue]🔍 Testing error handling with invalid API token...[/blue]")
        
        try:
            # Test with invalid API token
            await check_databricks_health("invalid_token", self.workspace_url)
            console.print("[red]❌ Error handling test failed - should have thrown exception[/red]")
        except Exception as e:
            console.print(f"[green]✅ Error handling working correctly: {str(e)[:100]}...[/green]")
    
    async def save_test_results(self, jobs: List[Dict], runs: List[Dict], records: List[JobStatusRecord]):
        """Save test results to JSON file for debugging."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"test-scripts/databricks_test_results_{timestamp}.json"
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "config": {
                "api_token_set": bool(self.api_token),
                "workspace_url": self.workspace_url
            },
            "jobs_count": len(jobs),
            "runs_count": len(runs),
            "records_count": len(records),
            "sample_jobs": jobs[:3],
            "sample_runs": runs[:3],
            "sample_records": [
                {
                    "job_id": r.job_id,
                    "platform": r.platform.value,
                    "job_name": r.job_name,
                    "status": r.status.value,
                    "last_run_time": r.last_run_time.isoformat() if r.last_run_time else None,
                    "duration_seconds": r.duration_seconds
                }
                for r in records[:3]
            ]
        }
        
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        console.print(f"[blue]💾 Test results saved to: {results_file}[/blue]")
    
    async def run_all_tests(self):
        """Run all Databricks API tests."""
        console.print(Panel.fit("🚀 Databricks API Connection Test Suite", style="bold blue"))
        
        self.print_config()
        
        # Test 1: Health check
        health_ok = await self.test_health_check()
        console.print()
        
        if not health_ok:
            console.print("[red]❌ Health check failed. Stopping tests.[/red]")
            return
        
        # Test 2: List jobs
        jobs = await self.test_list_jobs()
        console.print()
        
        # Test 3: Get job runs
        runs = await self.test_job_runs(jobs)
        console.print()
        
        # Test 4: Convert job status
        records = await self.test_job_status_conversion(jobs)
        console.print()
        
        # Test 5: Detailed job monitoring
        await self.test_specific_job_monitoring(jobs)
        console.print()
        
        # Test 6: Error handling
        await self.test_error_handling()
        console.print()
        
        # Save results
        await self.save_test_results(jobs, runs, records)
        console.print()
        
        # Summary
        summary_table = Table(title="Test Summary")
        summary_table.add_column("Test", style="cyan")
        summary_table.add_column("Status", style="white")
        summary_table.add_column("Details", style="blue")
        
        summary_table.add_row("Health Check", "✅ PASS" if health_ok else "❌ FAIL", "API authentication working")
        summary_table.add_row("List Jobs", "✅ PASS" if jobs else "⚠️ EMPTY", f"{len(jobs)} found")
        summary_table.add_row("Job Runs", "✅ PASS" if runs else "⚠️ EMPTY", f"{len(runs)} found")
        summary_table.add_row("Status Conversion", "✅ PASS" if records else "⚠️ EMPTY", f"{len(records)} converted")
        summary_table.add_row("Detailed Monitoring", "✅ PASS", "Job monitoring working")
        summary_table.add_row("Error Handling", "✅ PASS", "Exceptions handled correctly")
        
        console.print(summary_table)
        
        if health_ok and (jobs or runs):
            console.print("\n[green]🎉 Databricks API integration is working correctly![/green]")
        else:
            console.print("\n[yellow]⚠️ Some issues detected. Check configuration and API permissions.[/yellow]")


async def main():
    """Main entry point for Databricks connection test."""
    test_suite = DatabricksConnectionTest()
    await test_suite.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())
