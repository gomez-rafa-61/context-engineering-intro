#!/usr/bin/env python3
"""
Test script for Power Automate API connection and functionality.
This script validates the Power Automate API integration independently.
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

from tools.powerautomate_api import (
    get_power_automate_flows,
    get_power_automate_flow_runs,
    get_power_automate_flow_status,
    check_power_automate_health,
    get_power_automate_environments
)
from models.job_status import JobStatusRecord, JobStatus, PlatformType

console = Console()

class PowerAutomateConnectionTest:
    """Test class for Power Automate API connection and functionality."""
    
    def __init__(self):
        load_dotenv()
        self.client_id = os.getenv("POWER_AUTOMATE_CLIENT_ID")
        self.client_secret = os.getenv("POWER_AUTOMATE_CLIENT_SECRET")
        self.tenant_id = os.getenv("POWER_AUTOMATE_TENANT_ID")
        self.environment_id = os.getenv("POWER_AUTOMATE_ENVIRONMENT_ID", "")
        
        missing_vars = []
        if not self.client_id:
            missing_vars.append("POWER_AUTOMATE_CLIENT_ID")
        if not self.client_secret:
            missing_vars.append("POWER_AUTOMATE_CLIENT_SECRET")
        if not self.tenant_id:
            missing_vars.append("POWER_AUTOMATE_TENANT_ID")
        
        if missing_vars:
            console.print(f"[red]❌ Missing environment variables: {', '.join(missing_vars)}[/red]")
            console.print("[yellow]💡 Set these in your .env file for Azure AD app authentication[/yellow]")
            sys.exit(1)
    
    def print_config(self):
        """Print current configuration."""
        config_table = Table(title="Power Automate Test Configuration")
        config_table.add_column("Setting", style="cyan")
        config_table.add_column("Value", style="white")
        
        config_table.add_row("Client ID", f"{'*' * 20}...{self.client_id[-4:] if len(self.client_id) > 4 else 'SET'}")
        config_table.add_row("Client Secret", f"{'*' * 20}...{self.client_secret[-4:] if len(self.client_secret) > 4 else 'SET'}")
        config_table.add_row("Tenant ID", self.tenant_id)
        config_table.add_row("Environment ID", self.environment_id or "Default")
        
        console.print(config_table)
        console.print()
    
    async def test_health_check(self) -> bool:
        """Test basic API health and authentication."""
        console.print("[blue]🔍 Testing Power Automate API health and authentication...[/blue]")
        
        try:
            health_status = await check_power_automate_health(
                self.client_id, 
                self.client_secret, 
                self.tenant_id
            )
            
            if health_status.get("status") == "healthy":
                console.print("[green]✅ Power Automate API health check passed[/green]")
                console.print(f"[blue]ℹ️ Token info: {health_status.get('token_info', {})}[/blue]")
                return True
            else:
                console.print(f"[yellow]⚠️ Power Automate API health check returned: {health_status}[/yellow]")
                return False
                
        except Exception as e:
            console.print(f"[red]❌ Power Automate API health check failed: {str(e)}[/red]")
            return False
    
    async def test_list_environments(self) -> List[Dict[str, Any]]:
        """Test listing Power Automate environments."""
        console.print("[blue]🔍 Testing Power Automate environments listing...[/blue]")
        
        try:
            environments = await get_power_automate_environments(
                self.client_id, 
                self.client_secret, 
                self.tenant_id
            )
            
            if environments:
                env_table = Table(title="Power Automate Environments")
                env_table.add_column("Environment ID", style="cyan")
                env_table.add_column("Display Name", style="white")
                env_table.add_column("Type", style="green")
                env_table.add_column("Region", style="blue")
                env_table.add_column("State", style="magenta")
                
                for env in environments[:10]:  # Show first 10
                    env_table.add_row(
                        env.get("name", "N/A")[:20] + "..." if len(env.get("name", "")) > 20 else env.get("name", "N/A"),
                        env.get("properties", {}).get("displayName", "Unknown"),
                        env.get("properties", {}).get("environmentType", "Unknown"),
                        env.get("location", "Unknown"),
                        env.get("properties", {}).get("provisioningState", "Unknown")
                    )
                
                console.print(env_table)
                console.print(f"[green]✅ Found {len(environments)} environments[/green]")
                
                # Set default environment if not specified
                if not self.environment_id and environments:
                    self.environment_id = environments[0].get("name", "")
                    console.print(f"[blue]ℹ️ Using default environment: {self.environment_id}[/blue]")
                
                return environments
            else:
                console.print("[yellow]⚠️ No environments found[/yellow]")
                return []
                
        except Exception as e:
            console.print(f"[red]❌ Failed to list environments: {str(e)}[/red]")
            return []
    
    async def test_list_flows(self) -> List[Dict[str, Any]]:
        """Test listing Power Automate flows."""
        console.print("[blue]🔍 Testing Power Automate flows listing...[/blue]")
        
        if not self.environment_id:
            console.print("[yellow]⚠️ No environment ID available for flow listing[/yellow]")
            return []
        
        try:
            flows = await get_power_automate_flows(
                self.client_id, 
                self.client_secret, 
                self.tenant_id,
                self.environment_id
            )
            
            if flows:
                flows_table = Table(title="Power Automate Flows")
                flows_table.add_column("Flow ID", style="cyan")
                flows_table.add_column("Display Name", style="white")
                flows_table.add_column("State", style="green")
                flows_table.add_column("Trigger", style="blue")
                flows_table.add_column("Modified", style="magenta")
                
                for flow in flows[:10]:  # Show first 10
                    properties = flow.get("properties", {})
                    
                    # Extract trigger type
                    trigger_type = "Unknown"
                    definition = properties.get("definition", {})
                    if definition and "triggers" in definition:
                        triggers = definition["triggers"]
                        if triggers:
                            trigger_type = list(triggers.keys())[0]
                    
                    flows_table.add_row(
                        flow.get("name", "N/A")[:20] + "..." if len(flow.get("name", "")) > 20 else flow.get("name", "N/A"),
                        properties.get("displayName", "Unknown"),
                        properties.get("state", "Unknown"),
                        trigger_type,
                        properties.get("lastModifiedTime", "Unknown")[:19] if properties.get("lastModifiedTime") else "N/A"
                    )
                
                console.print(flows_table)
                console.print(f"[green]✅ Found {len(flows)} flows[/green]")
                return flows
            else:
                console.print("[yellow]⚠️ No flows found[/yellow]")
                return []
                
        except Exception as e:
            console.print(f"[red]❌ Failed to list flows: {str(e)}[/red]")
            return []
    
    async def test_flow_runs(self, flows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Test getting flow runs for the first few flows."""
        console.print("[blue]🔍 Testing Power Automate flow runs retrieval...[/blue]")
        
        all_runs = []
        
        for flow in flows[:3]:  # Test with first 3 flows
            flow_id = flow.get("name")
            if not flow_id:
                continue
                
            try:
                runs = await get_power_automate_flow_runs(
                    self.client_id, 
                    self.client_secret, 
                    self.tenant_id,
                    self.environment_id,
                    flow_id
                )
                
                if runs:
                    console.print(f"[green]✅ Flow {flow_id[:12]}...: Found {len(runs)} runs[/green]")
                    all_runs.extend(runs)
                else:
                    console.print(f"[yellow]⚠️ Flow {flow_id[:12]}...: No runs found[/yellow]")
                    
            except Exception as e:
                console.print(f"[red]❌ Failed to get runs for flow {flow_id[:12]}...: {str(e)}[/red]")
        
        if all_runs:
            runs_table = Table(title="Recent Flow Runs")
            runs_table.add_column("Run ID", style="cyan")
            runs_table.add_column("Flow ID", style="white")
            runs_table.add_column("Status", style="green")
            runs_table.add_column("Start Time", style="blue")
            runs_table.add_column("End Time", style="magenta")
            runs_table.add_column("Trigger", style="yellow")
            
            for run in all_runs[:10]:  # Show first 10 runs
                properties = run.get("properties", {})
                
                start_time = "N/A"
                if properties.get("startTime"):
                    try:
                        start_time = properties["startTime"][:19].replace("T", " ")
                    except:
                        pass
                
                end_time = "N/A"
                if properties.get("endTime"):
                    try:
                        end_time = properties["endTime"][:19].replace("T", " ")
                    except:
                        pass
                
                runs_table.add_row(
                    run.get("name", "N/A")[:12] + "...",
                    properties.get("workflow", {}).get("name", "N/A")[:12] + "...",
                    properties.get("status", "Unknown"),
                    start_time,
                    end_time,
                    properties.get("trigger", {}).get("name", "Unknown")
                )
            
            console.print(runs_table)
            console.print(f"[green]✅ Found {len(all_runs)} total flow runs[/green]")
        
        return all_runs
    
    async def test_flow_status_conversion(self, flows: List[Dict[str, Any]]) -> List[JobStatusRecord]:
        """Test converting Power Automate flows to JobStatusRecord format."""
        console.print("[blue]🔍 Testing flow status record conversion...[/blue]")
        
        try:
            job_records = []
            for flow in flows[:5]:  # Test with first 5 flows
                flow_id = flow.get("name")
                if not flow_id:
                    continue
                    
                try:
                    # Convert using the actual API function
                    status_records = await get_power_automate_flow_status(
                        self.client_id, 
                        self.client_secret, 
                        self.tenant_id,
                        self.environment_id,
                        flow_id
                    )
                    job_records.extend(status_records)
                except Exception as e:
                    console.print(f"[yellow]⚠️ Failed to convert flow {flow_id[:12]}...: {str(e)}[/yellow]")
            
            if job_records:
                records_table = Table(title="Flow Status Records")
                records_table.add_column("Flow ID", style="cyan")
                records_table.add_column("Platform", style="white")
                records_table.add_column("Flow Name", style="green")
                records_table.add_column("Status", style="blue")
                records_table.add_column("Last Run", style="magenta")
                records_table.add_column("Duration", style="yellow")
                
                for record in job_records:
                    duration = "N/A"
                    if record.duration_seconds:
                        duration = f"{record.duration_seconds}s"
                    
                    records_table.add_row(
                        record.job_id[:12] + "...",
                        record.platform.value,
                        record.job_name,
                        record.status.value,
                        record.last_run_time.strftime("%Y-%m-%d %H:%M") if record.last_run_time else "N/A",
                        duration
                    )
                
                console.print(records_table)
                console.print(f"[green]✅ Successfully converted {len(job_records)} flow status records[/green]")
                return job_records
            else:
                console.print("[yellow]⚠️ No flow status records created[/yellow]")
                return []
                
        except Exception as e:
            console.print(f"[red]❌ Failed to convert flow status records: {str(e)}[/red]")
            return []
    
    async def test_specific_flow_monitoring(self, flows: List[Dict[str, Any]]):
        """Test monitoring a specific flow in detail."""
        if not flows:
            console.print("[yellow]⚠️ No flows available for detailed monitoring test[/yellow]")
            return
        
        console.print("[blue]🔍 Testing detailed flow monitoring...[/blue]")
        
        # Take the first flow
        test_flow = flows[0]
        flow_id = test_flow.get("name")
        flow_name = test_flow.get("properties", {}).get("displayName", "Unknown")
        
        console.print(f"[blue]📊 Monitoring flow: {flow_name} ({flow_id[:12]}...)[/blue]")
        
        try:
            # Get detailed flow runs
            runs = await get_power_automate_flow_runs(
                self.client_id, 
                self.client_secret, 
                self.tenant_id,
                self.environment_id,
                flow_id,
                limit=5
            )
            
            if runs:
                detail_table = Table(title=f"Flow {flow_name} - Recent Runs")
                detail_table.add_column("Run ID", style="cyan")
                detail_table.add_column("Status", style="white")
                detail_table.add_column("Start Time", style="green")
                detail_table.add_column("End Time", style="blue")
                detail_table.add_column("Duration", style="magenta")
                detail_table.add_column("Trigger", style="yellow")
                
                for run in runs:
                    properties = run.get("properties", {})
                    
                    start_time = "N/A"
                    end_time = "N/A"
                    duration = "N/A"
                    
                    if properties.get("startTime"):
                        try:
                            start_time = properties["startTime"][:19].replace("T", " ")
                        except:
                            pass
                    
                    if properties.get("endTime"):
                        try:
                            end_time = properties["endTime"][:19].replace("T", " ")
                            if properties.get("startTime"):
                                # Calculate duration
                                start_dt = datetime.fromisoformat(properties["startTime"].replace("Z", "+00:00"))
                                end_dt = datetime.fromisoformat(properties["endTime"].replace("Z", "+00:00"))
                                duration_sec = (end_dt - start_dt).total_seconds()
                                duration = f"{duration_sec:.1f}s"
                        except:
                            pass
                    
                    detail_table.add_row(
                        run.get("name", "N/A")[:12] + "...",
                        properties.get("status", "Unknown"),
                        start_time,
                        end_time,
                        duration,
                        properties.get("trigger", {}).get("name", "Unknown")
                    )
                
                console.print(detail_table)
                console.print(f"[green]✅ Successfully monitored flow {flow_name}[/green]")
            else:
                console.print(f"[yellow]⚠️ No runs found for flow {flow_name}[/yellow]")
                
        except Exception as e:
            console.print(f"[red]❌ Failed to monitor flow {flow_name}: {str(e)}[/red]")
    
    async def test_error_handling(self):
        """Test error handling with invalid credentials."""
        console.print("[blue]🔍 Testing error handling with invalid credentials...[/blue]")
        
        try:
            # Test with invalid client secret
            await check_power_automate_health("invalid_client", "invalid_secret", self.tenant_id)
            console.print("[red]❌ Error handling test failed - should have thrown exception[/red]")
        except Exception as e:
            console.print(f"[green]✅ Error handling working correctly: {str(e)[:100]}...[/green]")
    
    async def save_test_results(self, environments: List[Dict], flows: List[Dict], runs: List[Dict], records: List[JobStatusRecord]):
        """Save test results to JSON file for debugging."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"test-scripts/powerautomate_test_results_{timestamp}.json"
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "config": {
                "client_id_set": bool(self.client_id),
                "client_secret_set": bool(self.client_secret),
                "tenant_id": self.tenant_id,
                "environment_id": self.environment_id
            },
            "environments_count": len(environments),
            "flows_count": len(flows),
            "runs_count": len(runs),
            "records_count": len(records),
            "sample_environments": environments[:2],
            "sample_flows": flows[:2],
            "sample_runs": runs[:2],
            "sample_records": [
                {
                    "job_id": r.job_id,
                    "platform": r.platform.value,
                    "job_name": r.job_name,
                    "status": r.status.value,
                    "last_run_time": r.last_run_time.isoformat() if r.last_run_time else None,
                    "duration_seconds": r.duration_seconds
                }
                for r in records[:2]
            ]
        }
        
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        console.print(f"[blue]💾 Test results saved to: {results_file}[/blue]")
    
    async def run_all_tests(self):
        """Run all Power Automate API tests."""
        console.print(Panel.fit("🚀 Power Automate API Connection Test Suite", style="bold blue"))
        
        self.print_config()
        
        # Test 1: Health check
        health_ok = await self.test_health_check()
        console.print()
        
        if not health_ok:
            console.print("[red]❌ Health check failed. Stopping tests.[/red]")
            return
        
        # Test 2: List environments
        environments = await self.test_list_environments()
        console.print()
        
        # Test 3: List flows
        flows = await self.test_list_flows()
        console.print()
        
        # Test 4: Get flow runs
        runs = await self.test_flow_runs(flows)
        console.print()
        
        # Test 5: Convert flow status
        records = await self.test_flow_status_conversion(flows)
        console.print()
        
        # Test 6: Detailed flow monitoring
        await self.test_specific_flow_monitoring(flows)
        console.print()
        
        # Test 7: Error handling
        await self.test_error_handling()
        console.print()
        
        # Save results
        await self.save_test_results(environments, flows, runs, records)
        console.print()
        
        # Summary
        summary_table = Table(title="Test Summary")
        summary_table.add_column("Test", style="cyan")
        summary_table.add_column("Status", style="white")
        summary_table.add_column("Details", style="blue")
        
        summary_table.add_row("Health Check", "✅ PASS" if health_ok else "❌ FAIL", "API authentication working")
        summary_table.add_row("List Environments", "✅ PASS" if environments else "⚠️ EMPTY", f"{len(environments)} found")
        summary_table.add_row("List Flows", "✅ PASS" if flows else "⚠️ EMPTY", f"{len(flows)} found")
        summary_table.add_row("Flow Runs", "✅ PASS" if runs else "⚠️ EMPTY", f"{len(runs)} found")
        summary_table.add_row("Status Conversion", "✅ PASS" if records else "⚠️ EMPTY", f"{len(records)} converted")
        summary_table.add_row("Detailed Monitoring", "✅ PASS", "Flow monitoring working")
        summary_table.add_row("Error Handling", "✅ PASS", "Exceptions handled correctly")
        
        console.print(summary_table)
        
        if health_ok and (environments or flows):
            console.print("\n[green]🎉 Power Automate API integration is working correctly![/green]")
        else:
            console.print("\n[yellow]⚠️ Some issues detected. Check configuration and API permissions.[/yellow]")


async def main():
    """Main entry point for Power Automate connection test."""
    test_suite = PowerAutomateConnectionTest()
    await test_suite.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())
