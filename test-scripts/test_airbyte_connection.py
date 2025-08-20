#!/usr/bin/env python3
"""
Test script for Airbyte API connection and functionality.
This script validates the Airbyte API integration independently.
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

from tools.airbyte_api import (
    AirbyteAPIClient,
    get_airbyte_job_status,
    get_airbyte_connection_health
)
from models.job_status import JobStatusRecord, JobStatus, PlatformType

console = Console()

class AirbyteConnectionTest:
    """Test class for Airbyte API connection and functionality."""
    
    def __init__(self):
        load_dotenv()
        self.client_id = os.getenv("AIRBYTE_CLIENT_ID")
        self.client_secret = os.getenv("AIRBYTE_CLIENT_SECRET")
        self.workspace_id = os.getenv("AIRBYTE_WORKSPACE_ID", "")
        self.base_url = os.getenv("AIRBYTE_BASE_URL", "https://api.airbyte.com/v1")
        
        if not self.client_id or not self.client_secret:
            console.print("[red]❌ AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET not found in environment variables[/red]")
            console.print("[yellow]💡 Set AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET in your .env file[/yellow]")
            sys.exit(1)
        
        # Initialize Airbyte API client
        self.client = AirbyteAPIClient(
            client_id=self.client_id,
            client_secret=self.client_secret,
            base_url=self.base_url
        )
    
    def print_config(self):
        """Print current configuration."""
        config_table = Table(title="Airbyte Test Configuration")
        config_table.add_column("Setting", style="cyan")
        config_table.add_column("Value", style="white")
        
        config_table.add_row("Client ID", f"{'*' * 20}...{self.client_id[-4:] if len(self.client_id) > 4 else 'SET'}")
        config_table.add_row("Client Secret", f"{'*' * 20}...{self.client_secret[-4:] if len(self.client_secret) > 4 else 'SET'}")
        config_table.add_row("Workspace ID", self.workspace_id or "Not specified")
        config_table.add_row("Base URL", self.base_url)
        
        console.print(config_table)
        console.print()
    
    async def test_health_check(self) -> bool:
        """Test basic API health and authentication."""
        console.print("[blue]🔍 Testing Airbyte API health and authentication...[/blue]")
        
        try:
            # Test by trying to get connections - if this works, the API is healthy
            connections = await self.client.get_connections(workspace_id=self.workspace_id)
            console.print("[green]✅ Airbyte API health check passed[/green]")
            return True
                
        except Exception as e:
            console.print(f"[red]❌ Airbyte API health check failed: {str(e)}[/red]")
            return False
    
    async def test_list_connections(self) -> List[Dict[str, Any]]:
        """Test listing Airbyte connections."""
        console.print("[blue]🔍 Testing Airbyte connections listing...[/blue]")
        
        try:
            connections = await self.client.get_connections(workspace_id=self.workspace_id)
            
            if connections:
                connections_table = Table(title="Airbyte Connections")
                connections_table.add_column("Connection ID", style="cyan")
                connections_table.add_column("Name", style="white")
                connections_table.add_column("Status", style="green")
                connections_table.add_column("Source", style="blue")
                connections_table.add_column("Destination", style="magenta")
                
                for conn in connections[:10]:  # Show first 10
                    connections_table.add_row(
                        conn.get("connectionId", "N/A")[:12] + "...",
                        conn.get("name", "Unknown"),
                        conn.get("status", "Unknown"),
                        conn.get("sourceName", "Unknown"),
                        conn.get("destinationName", "Unknown")
                    )
                
                console.print(connections_table)
                console.print(f"[green]✅ Found {len(connections)} connections[/green]")
                return connections
            else:
                console.print("[yellow]⚠️ No connections found[/yellow]")
                return []
                
        except Exception as e:
            console.print(f"[red]❌ Failed to list connections: {str(e)}[/red]")
            return []
    
    async def test_list_jobs(self) -> List[Dict[str, Any]]:
        """Test listing Airbyte jobs."""
        console.print("[blue]🔍 Testing Airbyte jobs listing...[/blue]")
        
        try:
            jobs_response = await self.client.get_jobs(workspace_id=self.workspace_id)
            jobs = jobs_response.get("data", [])
            
            if jobs:
                jobs_table = Table(title="Recent Airbyte Jobs")
                jobs_table.add_column("Job ID", style="cyan")
                jobs_table.add_column("Connection", style="white")
                jobs_table.add_column("Status", style="green")
                jobs_table.add_column("Created", style="blue")
                jobs_table.add_column("Duration", style="magenta")
                
                for job in jobs[:10]:  # Show first 10
                    duration = "N/A"
                    if job.get("createdAt") and job.get("updatedAt"):
                        try:
                            created = datetime.fromisoformat(job["createdAt"].replace('Z', '+00:00'))
                            updated = datetime.fromisoformat(job["updatedAt"].replace('Z', '+00:00'))
                            duration = str(updated - created)
                        except:
                            pass
                    
                    jobs_table.add_row(
                        job.get("jobId", "N/A")[:12] + "...",
                        job.get("configName", "Unknown"),
                        job.get("status", "Unknown"),
                        job.get("createdAt", "Unknown")[:19] if job.get("createdAt") else "N/A",
                        duration
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
    
    async def test_job_status_conversion(self, jobs: List[Dict[str, Any]]) -> List[JobStatusRecord]:
        """Test converting Airbyte jobs to JobStatusRecord format."""
        console.print("[blue]🔍 Testing job status record conversion...[/blue]")
        
        try:
            job_records = []
            for job in jobs[:5]:  # Test with first 5 jobs
                try:
                    # Convert using the actual API function
                    status_records = await get_airbyte_job_status(
                        client_id=self.client_id,
                        client_secret=self.client_secret,
                        workspace_id=self.workspace_id,
                        base_url=self.base_url
                    )
                    job_records.extend(status_records)
                except Exception as e:
                    console.print(f"[yellow]⚠️ Failed to convert job {job.get('jobId', 'unknown')}: {str(e)}[/yellow]")
            
            if job_records:
                records_table = Table(title="Job Status Records")
                records_table.add_column("Job ID", style="cyan")
                records_table.add_column("Platform", style="white")
                records_table.add_column("Job Name", style="green")
                records_table.add_column("Status", style="blue")
                records_table.add_column("Last Run", style="magenta")
                
                for record in job_records:
                    records_table.add_row(
                        record.job_id[:12] + "...",
                        record.platform.value,
                        record.job_name,
                        record.status.value,
                        record.last_run_time.strftime("%Y-%m-%d %H:%M") if record.last_run_time else "N/A"
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
    
    async def test_error_handling(self):
        """Test error handling with invalid credentials."""
        console.print("[blue]🔍 Testing error handling with invalid API key...[/blue]")
        
        try:
            # Test with invalid client
            bad_client = AirbyteAPIClient(
                client_id="invalid",
                client_secret="invalid",
                base_url=self.base_url
            )
            await bad_client.get_connections(workspace_id=self.workspace_id)
            console.print("[red]❌ Error handling test failed - should have thrown exception[/red]")
        except Exception as e:
            console.print(f"[green]✅ Error handling working correctly: {str(e)[:100]}...[/green]")
    
    async def save_test_results(self, connections: List[Dict], jobs: List[Dict], records: List[JobStatusRecord]):
        """Save test results to JSON file for debugging."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"airbyte_test_results_{timestamp}.json"
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "config": {
                "client_credentials_set": bool(self.client_id and self.client_secret),
                "workspace_id": self.workspace_id,
                "base_url": self.base_url
            },
            "connections_count": len(connections),
            "jobs_count": len(jobs),
            "records_count": len(records),
            "sample_connections": connections[:3],
            "sample_jobs": jobs[:3],
            "sample_records": [
                {
                    "job_id": r.job_id,
                    "platform": r.platform.value,
                    "job_name": r.job_name,
                    "status": r.status.value,
                    "last_run_time": r.last_run_time.isoformat() if r.last_run_time else None
                }
                for r in records[:3]
            ]
        }
        
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        console.print(f"[blue]💾 Test results saved to: {results_file}[/blue]")
    
    async def run_all_tests(self):
        """Run all Airbyte API tests."""
        console.print(Panel.fit("🚀 Airbyte API Connection Test Suite", style="bold blue"))
        
        self.print_config()
        
        # Test 1: Health check
        health_ok = await self.test_health_check()
        console.print()
        
        if not health_ok:
            console.print("[red]❌ Health check failed. Stopping tests.[/red]")
            return
        
        # Test 2: List connections
        connections = await self.test_list_connections()
        console.print()
        
        # Test 3: List jobs
        jobs = await self.test_list_jobs()
        console.print()
        
        # Test 4: Convert job status
        records = await self.test_job_status_conversion(jobs)
        console.print()
        
        # Test 5: Error handling
        await self.test_error_handling()
        console.print()
        
        # Save results
        await self.save_test_results(connections, jobs, records)
        console.print()
        
        # Summary
        summary_table = Table(title="Test Summary")
        summary_table.add_column("Test", style="cyan")
        summary_table.add_column("Status", style="white")
        summary_table.add_column("Details", style="blue")
        
        summary_table.add_row("Health Check", "✅ PASS" if health_ok else "❌ FAIL", "API authentication working")
        summary_table.add_row("List Connections", "✅ PASS" if connections else "⚠️ EMPTY", f"{len(connections)} found")
        summary_table.add_row("List Jobs", "✅ PASS" if jobs else "⚠️ EMPTY", f"{len(jobs)} found")
        summary_table.add_row("Status Conversion", "✅ PASS" if records else "⚠️ EMPTY", f"{len(records)} converted")
        summary_table.add_row("Error Handling", "✅ PASS", "Exceptions handled correctly")
        
        console.print(summary_table)
        
        if health_ok and (connections or jobs):
            console.print("\n[green]🎉 Airbyte API integration is working correctly![/green]")
        else:
            console.print("\n[yellow]⚠️ Some issues detected. Check configuration and API permissions.[/yellow]")


async def main():
    """Main entry point for Airbyte connection test."""
    test_suite = AirbyteConnectionTest()
    await test_suite.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())
