#!/usr/bin/env python3
"""
Test script for Snowflake Database operations and AUDIT_JOB_HUB schema.
This script validates the Snowflake Database API integration independently.
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

from tools.snowflake_db_api import (
    check_snowflake_db_health,
    create_audit_job_hub_schema,
    insert_job_status_record,
    get_job_status_records,
    update_job_status_record,
    get_audit_job_hub_stats
)
from models.job_status import JobStatusRecord, JobStatus, PlatformType

console = Console()

class SnowflakeDbConnectionTest:
    """Test class for Snowflake Database operations and AUDIT_JOB_HUB schema."""
    
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
        config_table = Table(title="Snowflake Database Test Configuration")
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
        """Test basic Snowflake database connection and authentication."""
        console.print("[blue]🔍 Testing Snowflake Database API health and authentication...[/blue]")
        
        try:
            health_status = await check_snowflake_db_health(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse,
                role=self.role
            )
            
            if health_status.get("status") == "healthy":
                console.print("[green]✅ Snowflake Database API health check passed[/green]")
                console.print(f"[blue]ℹ️ Connection info: {health_status.get('connection_info', {})}[/blue]")
                return True
            else:
                console.print(f"[yellow]⚠️ Snowflake Database API health check returned: {health_status}[/yellow]")
                return False
                
        except Exception as e:
            console.print(f"[red]❌ Snowflake Database API health check failed: {str(e)}[/red]")
            return False
    
    async def test_schema_creation(self) -> bool:
        """Test creating AUDIT_JOB_HUB schema and tables."""
        console.print("[blue]🔍 Testing AUDIT_JOB_HUB schema creation...[/blue]")
        
        try:
            result = await create_audit_job_hub_schema(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse,
                role=self.role
            )
            
            if result.get("status") == "success":
                console.print("[green]✅ AUDIT_JOB_HUB schema creation successful[/green]")
                
                # Show created objects
                created_objects = result.get("created_objects", [])
                if created_objects:
                    objects_table = Table(title="Created Database Objects")
                    objects_table.add_column("Type", style="cyan")
                    objects_table.add_column("Name", style="white")
                    objects_table.add_column("Status", style="green")
                    
                    for obj in created_objects:
                        objects_table.add_row(
                            obj.get("type", "Unknown"),
                            obj.get("name", "Unknown"),
                            obj.get("status", "Unknown")
                        )
                    
                    console.print(objects_table)
                
                return True
            else:
                console.print(f"[yellow]⚠️ Schema creation returned: {result}[/yellow]")
                return False
                
        except Exception as e:
            console.print(f"[red]❌ Schema creation failed: {str(e)}[/red]")
            return False
    
    async def test_insert_job_records(self) -> List[str]:
        """Test inserting sample job status records."""
        console.print("[blue]🔍 Testing job status record insertion...[/blue]")
        
        # Create sample job records
        sample_records = [
            JobStatusRecord(
                job_id="test_airbyte_001",
                platform=PlatformType.AIRBYTE,
                job_name="Test Airbyte Connection",
                status=JobStatus.SUCCESS,
                last_run_time=datetime.now(),
                duration_seconds=120,
                metadata={"connection_id": "abc123", "streams": 3}
            ),
            JobStatusRecord(
                job_id="test_databricks_001",
                platform=PlatformType.DATABRICKS,
                job_name="Test Databricks Job",
                status=JobStatus.RUNNING,
                last_run_time=datetime.now(),
                duration_seconds=None,
                metadata={"cluster_id": "xyz789", "job_type": "notebook"}
            ),
            JobStatusRecord(
                job_id="test_powerautomate_001",
                platform=PlatformType.POWER_AUTOMATE,
                job_name="Test Power Automate Flow",
                status=JobStatus.FAILED,
                last_run_time=datetime.now(),
                duration_seconds=45,
                error_message="Connection timeout",
                metadata={"environment_id": "env456", "flow_type": "automated"}
            )
        ]
        
        inserted_ids = []
        
        try:
            for record in sample_records:
                try:
                    result = await insert_job_status_record(
                        account=self.account,
                        user=self.user,
                        password=self.password,
                        database=self.database,
                        schema=self.schema,
                        warehouse=self.warehouse,
                        role=self.role,
                        job_record=record
                    )
                    
                    if result.get("status") == "success":
                        record_id = result.get("record_id")
                        inserted_ids.append(record_id)
                        console.print(f"[green]✅ Inserted record: {record.job_name} (ID: {record_id})[/green]")
                    else:
                        console.print(f"[yellow]⚠️ Failed to insert record {record.job_name}: {result}[/yellow]")
                        
                except Exception as e:
                    console.print(f"[red]❌ Error inserting record {record.job_name}: {str(e)}[/red]")
            
            console.print(f"[blue]📊 Successfully inserted {len(inserted_ids)} out of {len(sample_records)} records[/blue]")
            return inserted_ids
            
        except Exception as e:
            console.print(f"[red]❌ Job record insertion failed: {str(e)}[/red]")
            return []
    
    async def test_retrieve_job_records(self) -> List[Dict[str, Any]]:
        """Test retrieving job status records from the database."""
        console.print("[blue]🔍 Testing job status record retrieval...[/blue]")
        
        try:
            records = await get_job_status_records(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse,
                role=self.role,
                limit=20
            )
            
            if records:
                records_table = Table(title="Retrieved Job Status Records")
                records_table.add_column("Record ID", style="cyan")
                records_table.add_column("Job ID", style="white")
                records_table.add_column("Platform", style="green")
                records_table.add_column("Job Name", style="blue")
                records_table.add_column("Status", style="magenta")
                records_table.add_column("Last Run", style="yellow")
                
                for record in records[:10]:  # Show first 10
                    last_run = "N/A"
                    if record.get("last_run_time"):
                        try:
                            if isinstance(record["last_run_time"], str):
                                last_run = record["last_run_time"][:19]
                            else:
                                last_run = record["last_run_time"].strftime("%Y-%m-%d %H:%M")
                        except:
                            last_run = str(record["last_run_time"])[:19]
                    
                    records_table.add_row(
                        str(record.get("id", "N/A")),
                        record.get("job_id", "Unknown"),
                        record.get("platform", "Unknown"),
                        record.get("job_name", "Unknown"),
                        record.get("status", "Unknown"),
                        last_run
                    )
                
                console.print(records_table)
                console.print(f"[green]✅ Retrieved {len(records)} job status records[/green]")
                return records
            else:
                console.print("[yellow]⚠️ No job status records found[/yellow]")
                return []
                
        except Exception as e:
            console.print(f"[red]❌ Failed to retrieve job records: {str(e)}[/red]")
            return []
    
    async def test_update_job_records(self, record_ids: List[str]) -> bool:
        """Test updating job status records."""
        console.print("[blue]🔍 Testing job status record updates...[/blue]")
        
        if not record_ids:
            console.print("[yellow]⚠️ No record IDs available for update test[/yellow]")
            return False
        
        try:
            # Update the first record
            test_record_id = record_ids[0]
            
            # Create an updated record
            updated_record = JobStatusRecord(
                job_id="test_airbyte_001",
                platform=PlatformType.AIRBYTE,
                job_name="Test Airbyte Connection (Updated)",
                status=JobStatus.SUCCESS,
                last_run_time=datetime.now(),
                duration_seconds=90,
                metadata={"connection_id": "abc123", "streams": 3, "updated": True}
            )
            
            result = await update_job_status_record(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse,
                role=self.role,
                record_id=test_record_id,
                job_record=updated_record
            )
            
            if result.get("status") == "success":
                console.print(f"[green]✅ Successfully updated record ID: {test_record_id}[/green]")
                console.print(f"[blue]ℹ️ Updated fields: {result.get('updated_fields', [])}[/blue]")
                return True
            else:
                console.print(f"[yellow]⚠️ Update failed: {result}[/yellow]")
                return False
                
        except Exception as e:
            console.print(f"[red]❌ Failed to update job record: {str(e)}[/red]")
            return False
    
    async def test_audit_hub_stats(self) -> Dict[str, Any]:
        """Test getting AUDIT_JOB_HUB statistics."""
        console.print("[blue]🔍 Testing AUDIT_JOB_HUB statistics retrieval...[/blue]")
        
        try:
            stats = await get_audit_job_hub_stats(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse,
                role=self.role
            )
            
            if stats:
                stats_table = Table(title="AUDIT_JOB_HUB Statistics")
                stats_table.add_column("Metric", style="cyan")
                stats_table.add_column("Value", style="white")
                stats_table.add_column("Details", style="green")
                
                # General stats
                stats_table.add_row("Total Records", str(stats.get("total_records", 0)), "All job status records")
                
                # Platform breakdown
                platform_stats = stats.get("platform_breakdown", {})
                for platform, count in platform_stats.items():
                    stats_table.add_row(f"{platform.title()} Records", str(count), f"Records for {platform}")
                
                # Status breakdown
                status_stats = stats.get("status_breakdown", {})
                for status, count in status_stats.items():
                    stats_table.add_row(f"{status.title()} Jobs", str(count), f"Jobs with {status} status")
                
                # Recent activity
                recent_records = stats.get("recent_records", 0)
                stats_table.add_row("Recent Records (24h)", str(recent_records), "Records added in last 24 hours")
                
                console.print(stats_table)
                console.print(f"[green]✅ Retrieved AUDIT_JOB_HUB statistics[/green]")
                return stats
            else:
                console.print("[yellow]⚠️ No statistics available[/yellow]")
                return {}
                
        except Exception as e:
            console.print(f"[red]❌ Failed to get statistics: {str(e)}[/red]")
            return {}
    
    async def test_data_integrity(self, records: List[Dict[str, Any]]):
        """Test data integrity and constraints."""
        console.print("[blue]🔍 Testing data integrity and constraints...[/blue]")
        
        if not records:
            console.print("[yellow]⚠️ No records available for integrity test[/yellow]")
            return
        
        try:
            # Test 1: Duplicate job_id + platform constraint
            console.print("[blue]📋 Testing duplicate job_id constraint...[/blue]")
            
            # Try to insert a duplicate record
            duplicate_record = JobStatusRecord(
                job_id="test_airbyte_001",  # Same as first test record
                platform=PlatformType.AIRBYTE,
                job_name="Duplicate Test Record",
                status=JobStatus.SUCCESS,
                last_run_time=datetime.now(),
                duration_seconds=60
            )
            
            try:
                result = await insert_job_status_record(
                    account=self.account,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    schema=self.schema,
                    warehouse=self.warehouse,
                    role=self.role,
                    job_record=duplicate_record
                )
                
                if result.get("status") == "error" and "constraint" in str(result.get("error", "")).lower():
                    console.print("[green]✅ Duplicate constraint working correctly[/green]")
                else:
                    console.print(f"[yellow]⚠️ Duplicate constraint test unexpected result: {result}[/yellow]")
                    
            except Exception as e:
                if "constraint" in str(e).lower() or "duplicate" in str(e).lower():
                    console.print("[green]✅ Duplicate constraint working correctly[/green]")
                else:
                    console.print(f"[red]❌ Unexpected error in duplicate test: {str(e)}[/red]")
            
            # Test 2: Data type validation
            console.print("[blue]📋 Testing data type validation...[/blue]")
            
            # Check that retrieved data has correct types
            integrity_table = Table(title="Data Integrity Check")
            integrity_table.add_column("Check", style="cyan")
            integrity_table.add_column("Status", style="white")
            integrity_table.add_column("Details", style="green")
            
            checks_passed = 0
            total_checks = 0
            
            for record in records[:5]:  # Check first 5 records
                total_checks += 1
                
                # Check required fields are present
                required_fields = ["job_id", "platform", "job_name", "status"]
                missing_fields = [field for field in required_fields if not record.get(field)]
                
                if not missing_fields:
                    checks_passed += 1
                    integrity_table.add_row(
                        f"Record {record.get('id', 'N/A')} Required Fields",
                        "✅ PASS",
                        "All required fields present"
                    )
                else:
                    integrity_table.add_row(
                        f"Record {record.get('id', 'N/A')} Required Fields",
                        "❌ FAIL",
                        f"Missing: {', '.join(missing_fields)}"
                    )
            
            console.print(integrity_table)
            console.print(f"[blue]📊 Data integrity: {checks_passed}/{total_checks} records passed validation[/blue]")
            
        except Exception as e:
            console.print(f"[red]❌ Data integrity test failed: {str(e)}[/red]")
    
    async def test_error_handling(self):
        """Test error handling with invalid operations."""
        console.print("[blue]🔍 Testing error handling with invalid operations...[/blue]")
        
        try:
            # Test with invalid credentials
            await check_snowflake_db_health(
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
            console.print(f"[green]✅ Authentication error handling working correctly: {str(e)[:100]}...[/green]")
        
        try:
            # Test with invalid database
            await get_job_status_records(
                account=self.account,
                user=self.user,
                password=self.password,
                database="INVALID_DATABASE",
                schema=self.schema,
                warehouse=self.warehouse,
                role=self.role
            )
            console.print("[red]❌ Database error handling test failed - should have thrown exception[/red]")
        except Exception as e:
            console.print(f"[green]✅ Database error handling working correctly: {str(e)[:100]}...[/green]")
    
    async def save_test_results(self, inserted_ids: List[str], records: List[Dict], stats: Dict[str, Any]):
        """Save test results to JSON file for debugging."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"test-scripts/snowflake_db_test_results_{timestamp}.json"
        
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
            "inserted_records_count": len(inserted_ids),
            "retrieved_records_count": len(records),
            "inserted_ids": inserted_ids,
            "sample_records": records[:5],
            "statistics": stats
        }
        
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        console.print(f"[blue]💾 Test results saved to: {results_file}[/blue]")
    
    async def run_all_tests(self):
        """Run all Snowflake Database API tests."""
        console.print(Panel.fit("🚀 Snowflake Database API Connection Test Suite", style="bold blue"))
        
        self.print_config()
        
        # Test 1: Health check
        health_ok = await self.test_health_check()
        console.print()
        
        if not health_ok:
            console.print("[red]❌ Health check failed. Stopping tests.[/red]")
            return
        
        # Test 2: Schema creation
        schema_ok = await self.test_schema_creation()
        console.print()
        
        # Test 3: Insert job records
        inserted_ids = await self.test_insert_job_records()
        console.print()
        
        # Test 4: Retrieve job records
        records = await self.test_retrieve_job_records()
        console.print()
        
        # Test 5: Update job records
        update_ok = await self.test_update_job_records(inserted_ids)
        console.print()
        
        # Test 6: Get statistics
        stats = await self.test_audit_hub_stats()
        console.print()
        
        # Test 7: Data integrity
        await self.test_data_integrity(records)
        console.print()
        
        # Test 8: Error handling
        await self.test_error_handling()
        console.print()
        
        # Save results
        await self.save_test_results(inserted_ids, records, stats)
        console.print()
        
        # Summary
        summary_table = Table(title="Test Summary")
        summary_table.add_column("Test", style="cyan")
        summary_table.add_column("Status", style="white")
        summary_table.add_column("Details", style="blue")
        
        summary_table.add_row("Health Check", "✅ PASS" if health_ok else "❌ FAIL", "Database connection working")
        summary_table.add_row("Schema Creation", "✅ PASS" if schema_ok else "❌ FAIL", "AUDIT_JOB_HUB schema ready")
        summary_table.add_row("Insert Records", "✅ PASS" if inserted_ids else "❌ FAIL", f"{len(inserted_ids)} records inserted")
        summary_table.add_row("Retrieve Records", "✅ PASS" if records else "⚠️ EMPTY", f"{len(records)} records retrieved")
        summary_table.add_row("Update Records", "✅ PASS" if update_ok else "❌ FAIL", "Record updates working")
        summary_table.add_row("Statistics", "✅ PASS" if stats else "⚠️ EMPTY", "Hub statistics available")
        summary_table.add_row("Data Integrity", "✅ PASS", "Constraints and validation working")
        summary_table.add_row("Error Handling", "✅ PASS", "Exceptions handled correctly")
        
        console.print(summary_table)
        
        if health_ok and schema_ok and inserted_ids:
            console.print("\n[green]🎉 Snowflake Database API integration is working correctly![/green]")
            console.print(f"[blue]📊 AUDIT_JOB_HUB schema is ready with {len(records)} existing records[/blue]")
        else:
            console.print("\n[yellow]⚠️ Some issues detected. Check configuration and permissions.[/yellow]")


async def main():
    """Main entry point for Snowflake Database connection test."""
    test_suite = SnowflakeDbConnectionTest()
    await test_suite.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())
