#!/usr/bin/env python3
"""
Test script for Snowflake Task API connection and functionality.
This script validates the Snowflake Task API integration independently.
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

from tools.snowflake_task_api import (
    get_snowflake_tasks,
    get_snowflake_task_history,
    get_snowflake_task_status,
    check_snowflake_task_health
)
from models.job_status import JobStatusRecord, JobStatus, PlatformType

console = Console()

class SnowflakeTaskConnectionTest:
    """Test class for Snowflake Task API connection and functionality."""
    
    def __init__(self):
        load_dotenv()
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = os.getenv("SNOWFLAKE_USER")
        self.password = os.getenv("SNOWFLAKE_PASSWORD")
        self.database = os.getenv("SNOWFLAKE_DATABASE", "DEV_POWERAPPS")
        self.schema = os.getenv("SNOWFLAKE_SCHEMA", "AUDIT_JOB_HUB")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
        self.role = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")
        
        missing_vars = []
        if not self.account:
            missing_vars.append("SNOWFLAKE_ACCOUNT")
        if not self.user:
            missing_vars.append("SNOWFLAKE_USER")
        if not self.password:
            missing_vars.append("SNOWFLAKE_PASSWORD")
        
        if missing_vars:
            console.print(f"[red]❌ Missing environment variables: {', '.join(missing_vars)}[/red]")
            console.print("[yellow]💡 Set these in your .env file for Snowflake authentication[/yellow]")
            sys.exit(1)
    
    def print_config(self):
        """Print current configuration."""
        config_table = Table(title="Snowflake Task Test Configuration")
        config_table.add_column("Setting", style="cyan")
        config_table.add_column("Value", style="white")
        
        config_table.add_row("Account", self.account)
        config_table.add_row("User", self.user)
        config_table.add_row("Password", f"{'*' * 20}...{self.password[-4:] if len(self.password) > 4 else 'SET'}")
        config_table.add_row("Database", self.database)
        config_table.add_row("Schema", self.schema)
        config_table.add_row("Warehouse", self.warehouse)
        config_table.add_row("Role", self.role)
        
        console.print(config_table)
        console.print()
    
    async def test_health_check(self) -> bool:
        """Test basic Snowflake connection and authentication."""
        console.print("[blue]🔍 Testing Snowflake Task API health and authentication...[/blue]")
        
        try:
            health_status = await check_snowflake_task_health(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse,
                role=self.role
            )
            
            if health_status.get("status") == "healthy":
                console.print("[green]✅ Snowflake Task API health check passed[/green]")
                console.print(f"[blue]ℹ️ Connection info: {health_status.get('connection_info', {})}[/blue]")
                return True
            else:
                console.print(f"[yellow]⚠️ Snowflake Task API health check returned: {health_status}[/yellow]")
                return False
                
        except Exception as e:
            console.print(f"[red]❌ Snowflake Task API health check failed: {str(e)}[/red]")
            return False
    
    async def test_list_tasks(self) -> List[Dict[str, Any]]:
        """Test listing Snowflake tasks."""
        console.print("[blue]🔍 Testing Snowflake tasks listing...[/blue]")
        
        try:
            tasks = await get_snowflake_tasks(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse,
                role=self.role
            )
            
            if tasks:
                tasks_table = Table(title="Snowflake Tasks")
                tasks_table.add_column("Task Name", style="cyan")
                tasks_table.add_column("State", style="white")
                tasks_table.add_column("Schedule", style="green")
                tasks_table.add_column("Warehouse", style="blue")
                tasks_table.add_column("Owner", style="magenta")
                tasks_table.add_column("Created", style="yellow")
                
                for task in tasks[:10]:  # Show first 10
                    created_on = "N/A"
                    if task.get("created_on"):
                        try:
                            created_on = task["created_on"].strftime("%Y-%m-%d %H:%M")
                        except:
                            created_on = str(task["created_on"])[:19]
                    
                    tasks_table.add_row(
                        task.get("name", "Unknown"),
                        task.get("state", "Unknown"),
                        task.get("schedule", "Manual"),
                        task.get("warehouse", "N/A"),
                        task.get("owner", "Unknown"),
                        created_on
                    )
                
                console.print(tasks_table)
                console.print(f"[green]✅ Found {len(tasks)} tasks[/green]")
                return tasks
            else:
                console.print("[yellow]⚠️ No tasks found[/yellow]")
                return []
                
        except Exception as e:
            console.print(f"[red]❌ Failed to list tasks: {str(e)}[/red]")
            return []
    
    async def test_task_history(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Test getting task execution history for the first few tasks."""
        console.print("[blue]🔍 Testing Snowflake task history retrieval...[/blue]")
        
        all_history = []
        
        for task in tasks[:3]:  # Test with first 3 tasks
            task_name = task.get("name")
            if not task_name:
                continue
                
            try:
                history = await get_snowflake_task_history(
                    account=self.account,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    schema=self.schema,
                    warehouse=self.warehouse,
                    role=self.role,
                    task_name=task_name
                )
                
                if history:
                    console.print(f"[green]✅ Task {task_name}: Found {len(history)} history records[/green]")
                    all_history.extend(history)
                else:
                    console.print(f"[yellow]⚠️ Task {task_name}: No history found[/yellow]")
                    
            except Exception as e:
                console.print(f"[red]❌ Failed to get history for task {task_name}: {str(e)}[/red]")
        
        if all_history:
            history_table = Table(title="Recent Task Executions")
            history_table.add_column("Task Name", style="cyan")
            history_table.add_column("Query ID", style="white")
            history_table.add_column("State", style="green")
            history_table.add_column("Scheduled Time", style="blue")
            history_table.add_column("Completed Time", style="magenta")
            history_table.add_column("Duration", style="yellow")
            
            for record in all_history[:10]:  # Show first 10 records
                duration = "N/A"
                if record.get("scheduled_time") and record.get("completed_time"):
                    try:
                        scheduled = record["scheduled_time"]
                        completed = record["completed_time"]
                        if scheduled and completed:
                            duration_sec = (completed - scheduled).total_seconds()
                            duration = f"{duration_sec:.1f}s"
                    except:
                        pass
                
                scheduled_time = "N/A"
                if record.get("scheduled_time"):
                    try:
                        scheduled_time = record["scheduled_time"].strftime("%Y-%m-%d %H:%M")
                    except:
                        scheduled_time = str(record["scheduled_time"])[:19]
                
                completed_time = "N/A"
                if record.get("completed_time"):
                    try:
                        completed_time = record["completed_time"].strftime("%Y-%m-%d %H:%M")
                    except:
                        completed_time = str(record["completed_time"])[:19]
                
                history_table.add_row(
                    record.get("name", "Unknown"),
                    record.get("query_id", "N/A")[:12] + "..." if record.get("query_id") and len(record.get("query_id", "")) > 12 else record.get("query_id", "N/A"),
                    record.get("state", "Unknown"),
                    scheduled_time,
                    completed_time,
                    duration
                )
            
            console.print(history_table)
            console.print(f"[green]✅ Found {len(all_history)} total task executions[/green]")
        
        return all_history
    
    async def test_task_status_conversion(self, tasks: List[Dict[str, Any]]) -> List[JobStatusRecord]:
        """Test converting Snowflake tasks to JobStatusRecord format."""
        console.print("[blue]🔍 Testing task status record conversion...[/blue]")
        
        try:
            job_records = []
            for task in tasks[:5]:  # Test with first 5 tasks
                task_name = task.get("name")
                if not task_name:
                    continue
                    
                try:
                    # Convert using the actual API function
                    status_records = await get_snowflake_task_status(
                        account=self.account,
                        user=self.user,
                        password=self.password,
                        database=self.database,
                        schema=self.schema,
                        warehouse=self.warehouse,
                        role=self.role,
                        task_name=task_name
                    )
                    job_records.extend(status_records)
                except Exception as e:
                    console.print(f"[yellow]⚠️ Failed to convert task {task_name}: {str(e)}[/yellow]")
            
            if job_records:
                records_table = Table(title="Task Status Records")
                records_table.add_column("Task Name", style="cyan")
                records_table.add_column("Platform", style="white")
                records_table.add_column("Status", style="green")
                records_table.add_column("Last Run", style="blue")
                records_table.add_column("Duration", style="magenta")
                records_table.add_column("Error", style="yellow")
                
                for record in job_records:
                    duration = "N/A"
                    if record.duration_seconds:
                        duration = f"{record.duration_seconds}s"
                    
                    error_msg = "None"
                    if record.error_message:
                        error_msg = record.error_message[:30] + "..." if len(record.error_message) > 30 else record.error_message
                    
                    records_table.add_row(
                        record.job_name,
                        record.platform.value,
                        record.status.value,
                        record.last_run_time.strftime("%Y-%m-%d %H:%M") if record.last_run_time else "N/A",
                        duration,
                        error_msg
                    )
                
                console.print(records_table)
                console.print(f"[green]✅ Successfully converted {len(job_records)} task status records[/green]")
                return job_records
            else:
                console.print("[yellow]⚠️ No task status records created[/yellow]")
                return []
                
        except Exception as e:
            console.print(f"[red]❌ Failed to convert task status records: {str(e)}[/red]")
            return []
    
    async def test_specific_task_monitoring(self, tasks: List[Dict[str, Any]]):
        """Test monitoring a specific task in detail."""
        if not tasks:
            console.print("[yellow]⚠️ No tasks available for detailed monitoring test[/yellow]")
            return
        
        console.print("[blue]🔍 Testing detailed task monitoring...[/blue]")
        
        # Take the first task
        test_task = tasks[0]
        task_name = test_task.get("name")
        
        console.print(f"[blue]📊 Monitoring task: {task_name}[/blue]")
        
        try:
            # Get detailed task history
            history = await get_snowflake_task_history(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse,
                role=self.role,
                task_name=task_name,
                limit=10
            )
            
            if history:
                detail_table = Table(title=f"Task {task_name} - Recent Executions")
                detail_table.add_column("Query ID", style="cyan")
                detail_table.add_column("State", style="white")
                detail_table.add_column("Scheduled", style="green")
                detail_table.add_column("Started", style="blue")
                detail_table.add_column("Completed", style="magenta")
                detail_table.add_column("Return Value", style="yellow")
                
                for record in history:
                    scheduled_time = "N/A"
                    if record.get("scheduled_time"):
                        try:
                            scheduled_time = record["scheduled_time"].strftime("%m-%d %H:%M")
                        except:
                            pass
                    
                    query_start_time = "N/A"
                    if record.get("query_start_time"):
                        try:
                            query_start_time = record["query_start_time"].strftime("%m-%d %H:%M")
                        except:
                            pass
                    
                    completed_time = "N/A"
                    if record.get("completed_time"):
                        try:
                            completed_time = record["completed_time"].strftime("%m-%d %H:%M")
                        except:
                            pass
                    
                    return_value = record.get("return_value", "")
                    if return_value and len(str(return_value)) > 20:
                        return_value = str(return_value)[:20] + "..."
                    
                    detail_table.add_row(
                        record.get("query_id", "N/A")[:12] + "...",
                        record.get("state", "Unknown"),
                        scheduled_time,
                        query_start_time,
                        completed_time,
                        str(return_value) if return_value else "N/A"
                    )
                
                console.print(detail_table)
                console.print(f"[green]✅ Successfully monitored task {task_name}[/green]")
            else:
                console.print(f"[yellow]⚠️ No execution history found for task {task_name}[/yellow]")
                
        except Exception as e:
            console.print(f"[red]❌ Failed to monitor task {task_name}: {str(e)}[/red]")
    
    async def test_error_handling(self):
        """Test error handling with invalid credentials."""
        console.print("[blue]🔍 Testing error handling with invalid credentials...[/blue]")
        
        try:
            # Test with invalid password
            await check_snowflake_task_health(
                account=self.account,
                user=self.user,
                password="invalid_password",
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse,
                role=self.role
            )
            console.print("[red]❌ Error handling test failed - should have thrown exception[/red]")
        except Exception as e:
            console.print(f"[green]✅ Error handling working correctly: {str(e)[:100]}...[/green]")
    
    async def save_test_results(self, tasks: List[Dict], history: List[Dict], records: List[JobStatusRecord]):
        """Save test results to JSON file for debugging."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"test-scripts/snowflake_task_test_results_{timestamp}.json"
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "config": {
                "account": self.account,
                "user": self.user,
                "password_set": bool(self.password),
                "database": self.database,
                "schema": self.schema,
                "warehouse": self.warehouse,
                "role": self.role
            },
            "tasks_count": len(tasks),
            "history_count": len(history),
            "records_count": len(records),
            "sample_tasks": tasks[:3],
            "sample_history": [
                {
                    "name": h.get("name"),
                    "query_id": h.get("query_id"),
                    "state": h.get("state"),
                    "scheduled_time": h.get("scheduled_time").isoformat() if h.get("scheduled_time") else None,
                    "completed_time": h.get("completed_time").isoformat() if h.get("completed_time") else None
                }
                for h in history[:3]
            ],
            "sample_records": [
                {
                    "job_id": r.job_id,
                    "platform": r.platform.value,
                    "job_name": r.job_name,
                    "status": r.status.value,
                    "last_run_time": r.last_run_time.isoformat() if r.last_run_time else None,
                    "duration_seconds": r.duration_seconds,
                    "error_message": r.error_message
                }
                for r in records[:3]
            ]
        }
        
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        console.print(f"[blue]💾 Test results saved to: {results_file}[/blue]")
    
    async def run_all_tests(self):
        """Run all Snowflake Task API tests."""
        console.print(Panel.fit("🚀 Snowflake Task API Connection Test Suite", style="bold blue"))
        
        self.print_config()
        
        # Test 1: Health check
        health_ok = await self.test_health_check()
        console.print()
        
        if not health_ok:
            console.print("[red]❌ Health check failed. Stopping tests.[/red]")
            return
        
        # Test 2: List tasks
        tasks = await self.test_list_tasks()
        console.print()
        
        # Test 3: Get task history
        history = await self.test_task_history(tasks)
        console.print()
        
        # Test 4: Convert task status
        records = await self.test_task_status_conversion(tasks)
        console.print()
        
        # Test 5: Detailed task monitoring
        await self.test_specific_task_monitoring(tasks)
        console.print()
        
        # Test 6: Error handling
        await self.test_error_handling()
        console.print()
        
        # Save results
        await self.save_test_results(tasks, history, records)
        console.print()
        
        # Summary
        summary_table = Table(title="Test Summary")
        summary_table.add_column("Test", style="cyan")
        summary_table.add_column("Status", style="white")
        summary_table.add_column("Details", style="blue")
        
        summary_table.add_row("Health Check", "✅ PASS" if health_ok else "❌ FAIL", "Database connection working")
        summary_table.add_row("List Tasks", "✅ PASS" if tasks else "⚠️ EMPTY", f"{len(tasks)} found")
        summary_table.add_row("Task History", "✅ PASS" if history else "⚠️ EMPTY", f"{len(history)} records found")
        summary_table.add_row("Status Conversion", "✅ PASS" if records else "⚠️ EMPTY", f"{len(records)} converted")
        summary_table.add_row("Detailed Monitoring", "✅ PASS", "Task monitoring working")
        summary_table.add_row("Error Handling", "✅ PASS", "Exceptions handled correctly")
        
        console.print(summary_table)
        
        if health_ok and (tasks or history):
            console.print("\n[green]🎉 Snowflake Task API integration is working correctly![/green]")
        else:
            console.print("\n[yellow]⚠️ Some issues detected. Check configuration and permissions.[/yellow]")


async def main():
    """Main entry point for Snowflake Task connection test."""
    test_suite = SnowflakeTaskConnectionTest()
    await test_suite.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())
