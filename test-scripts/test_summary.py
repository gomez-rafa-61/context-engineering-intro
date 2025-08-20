#!/usr/bin/env python3
"""
Test summary script that provides quick status overview without running tests.
This script checks configuration and provides guidance on what tests can be run.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import json
import glob

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

console = Console()

class TestSummary:
    """Provides overview of test configuration and recent results."""
    
    def __init__(self):
        load_dotenv()
        self.script_dir = Path(__file__).parent
        
        # Platform requirements
        self.platform_requirements = {
            "Airbyte": {
                "script": "test_airbyte_connection.py",
                "env_vars": ["AIRBYTE_API_KEY"],
                "optional_vars": ["AIRBYTE_WORKSPACE_ID", "AIRBYTE_BASE_URL"],
                "description": "Tests Airbyte connections and job status"
            },
            "Databricks": {
                "script": "test_databricks_connection.py", 
                "env_vars": ["DATABRICKS_API_TOKEN", "DATABRICKS_WORKSPACE_URL"],
                "optional_vars": [],
                "description": "Tests Databricks job runs and monitoring"
            },
            "Power Automate": {
                "script": "test_powerautomate_connection.py",
                "env_vars": ["POWER_AUTOMATE_CLIENT_ID", "POWER_AUTOMATE_CLIENT_SECRET", "POWER_AUTOMATE_TENANT_ID"],
                "optional_vars": ["POWER_AUTOMATE_ENVIRONMENT_ID"],
                "description": "Tests Power Automate flow monitoring"
            },
            "Snowflake Task": {
                "script": "test_snowflake_task_connection.py",
                "env_vars": ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"],
                "optional_vars": ["SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_ROLE"],
                "description": "Tests Snowflake task execution monitoring"
            },
            "Snowflake DB": {
                "script": "test_snowflake_db_connection.py",
                "env_vars": ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"],
                "optional_vars": ["SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_ROLE"],
                "description": "Tests AUDIT_JOB_HUB database operations"
            },
            "Outlook": {
                "script": "test_outlook_connection.py",
                "env_vars": ["OUTLOOK_CLIENT_ID", "OUTLOOK_CLIENT_SECRET", "OUTLOOK_TENANT_ID"],
                "optional_vars": ["OUTLOOK_TEST_RECIPIENT"],
                "description": "Tests email notification functionality"
            }
        }
    
    def check_configuration(self) -> Dict[str, Dict]:
        """Check configuration status for all platforms."""
        config_status = {}
        
        for platform, requirements in self.platform_requirements.items():
            # Check required variables
            missing_required = [var for var in requirements["env_vars"] if not os.getenv(var)]
            present_optional = [var for var in requirements["optional_vars"] if os.getenv(var)]
            
            config_status[platform] = {
                "can_run": len(missing_required) == 0,
                "missing_required": missing_required,
                "present_optional": present_optional,
                "requirements": requirements
            }
        
        return config_status
    
    def find_recent_test_results(self) -> Dict[str, str]:
        """Find most recent test result files."""
        recent_results = {}
        
        # Look for test result files
        result_patterns = [
            "airbyte_test_results_*.json",
            "databricks_test_results_*.json", 
            "powerautomate_test_results_*.json",
            "snowflake_task_test_results_*.json",
            "snowflake_db_test_results_*.json",
            "outlook_test_results_*.json",
            "integration_test_report_*.json",
            "test_runner_report_*.json"
        ]
        
        for pattern in result_patterns:
            files = list(self.script_dir.glob(pattern))
            if files:
                # Get most recent file
                latest_file = max(files, key=lambda f: f.stat().st_mtime)
                platform_name = pattern.split('_')[0].title()
                
                # Special handling for integration and runner reports
                if "integration" in pattern:
                    platform_name = "Integration Test"
                elif "runner" in pattern:
                    platform_name = "Test Runner"
                    
                recent_results[platform_name] = str(latest_file.name)
        
        return recent_results
    
    def analyze_recent_results(self, result_files: Dict[str, str]) -> Dict[str, Dict]:
        """Analyze recent test results for success/failure status."""
        analysis = {}
        
        for platform, filename in result_files.items():
            try:
                file_path = self.script_dir / filename
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                # Extract key metrics based on file type
                if "integration" in filename:
                    analysis[platform] = {
                        "timestamp": data.get("test_run_info", {}).get("timestamp", "Unknown"),
                        "healthy_platforms": data.get("healthy_platforms", []),
                        "failed_platforms": data.get("failed_platforms", []),
                        "status": "success" if len(data.get("healthy_platforms", [])) > 0 else "failed"
                    }
                elif "runner" in filename:
                    analysis[platform] = {
                        "timestamp": data.get("test_run_info", {}).get("timestamp", "Unknown"),
                        "successful_tests": data.get("test_run_info", {}).get("successful_tests", 0),
                        "failed_tests": data.get("test_run_info", {}).get("failed_tests", 0),
                        "status": "success" if data.get("test_run_info", {}).get("failed_tests", 1) == 0 else "failed"
                    }
                else:
                    # Individual platform result
                    analysis[platform] = {
                        "timestamp": data.get("timestamp", "Unknown"),
                        "status": "success" if data.get("healthy_platforms") or data.get("jobs_count", 0) >= 0 else "failed"
                    }
                    
            except Exception as e:
                analysis[platform] = {
                    "timestamp": "Unknown",
                    "status": "error",
                    "error": str(e)
                }
        
        return analysis
    
    def print_configuration_status(self, config_status: Dict[str, Dict]):
        """Print configuration status table."""
        console.print(Panel.fit("📋 Platform Configuration Status", style="bold blue"))
        
        config_table = Table(title="Platform Test Readiness")
        config_table.add_column("Platform", style="cyan")
        config_table.add_column("Status", style="white")
        config_table.add_column("Missing Required", style="red")
        config_table.add_column("Optional Configured", style="green")
        config_table.add_column("Description", style="blue")
        
        ready_count = 0
        for platform, status in config_status.items():
            is_ready = status["can_run"]
            if is_ready:
                ready_count += 1
                
            status_text = "✅ READY" if is_ready else "❌ NOT READY"
            missing_text = ", ".join(status["missing_required"]) if status["missing_required"] else "None"
            optional_text = f"{len(status['present_optional'])}/{len(status['requirements']['optional_vars'])}"
            
            config_table.add_row(
                platform,
                status_text,
                missing_text,
                optional_text,
                status["requirements"]["description"]
            )
        
        console.print(config_table)
        console.print(f"[blue]📊 {ready_count}/{len(config_status)} platforms ready for testing[/blue]")
        console.print()
    
    def print_recent_results(self, result_files: Dict[str, str], analysis: Dict[str, Dict]):
        """Print recent test results."""
        if not result_files:
            console.print("[yellow]⚠️ No recent test results found[/yellow]")
            console.print("[blue]💡 Run 'python test-scripts/run_all_tests.py' to execute tests[/blue]")
            console.print()
            return
        
        console.print(Panel.fit("📊 Recent Test Results", style="bold green"))
        
        results_table = Table(title="Latest Test Executions")
        results_table.add_column("Test Type", style="cyan")
        results_table.add_column("Status", style="white")
        results_table.add_column("Timestamp", style="blue")
        results_table.add_column("Result File", style="green")
        
        for platform, filename in result_files.items():
            analysis_data = analysis.get(platform, {})
            status = analysis_data.get("status", "unknown")
            
            status_text = {
                "success": "✅ SUCCESS",
                "failed": "❌ FAILED", 
                "error": "⚠️ ERROR",
                "unknown": "❓ UNKNOWN"
            }.get(status, "❓ UNKNOWN")
            
            timestamp = analysis_data.get("timestamp", "Unknown")
            if timestamp != "Unknown":
                try:
                    # Format timestamp
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    timestamp = dt.strftime("%Y-%m-%d %H:%M")
                except:
                    pass
            
            results_table.add_row(platform, status_text, timestamp, filename)
        
        console.print(results_table)
        console.print()
    
    def print_quick_start_guide(self, config_status: Dict[str, Dict]):
        """Print quick start guide based on configuration."""
        ready_platforms = [name for name, status in config_status.items() if status["can_run"]]
        
        console.print(Panel.fit("🚀 Quick Start Guide", style="bold magenta"))
        
        if len(ready_platforms) >= 4:
            console.print("[green]🎉 Excellent! Most platforms are configured.[/green]")
            console.print("[blue]Recommended next steps:[/blue]")
            console.print("1. Run full test suite: `python test-scripts/run_all_tests.py`")
            console.print("2. Check individual platforms: `python test-scripts/test_<platform>_connection.py`")
            console.print("3. Test integration: `python test-scripts/test_all_platforms_integration.py`")
        elif len(ready_platforms) >= 2:
            console.print("[yellow]⚠️ Some platforms are configured.[/yellow]")
            console.print("[blue]Recommended next steps:[/blue]")
            console.print("1. Test configured platforms first")
            console.print("2. Configure missing platforms:")
            
            for platform, status in config_status.items():
                if not status["can_run"]:
                    console.print(f"   • {platform}: Missing {', '.join(status['missing_required'])}")
            
            console.print("3. Run partial test suite: `python test-scripts/run_all_tests.py`")
        else:
            console.print("[red]❌ Most platforms need configuration.[/red]")
            console.print("[blue]Recommended next steps:[/blue]")
            console.print("1. Copy `.env.example` to `.env` in project root")
            console.print("2. Fill in API keys and credentials")
            console.print("3. Start with one platform test first")
        
        console.print()
        
        # Show individual test commands
        console.print("[blue]Individual platform test commands:[/blue]")
        for platform, status in config_status.items():
            script_name = status["requirements"]["script"]
            ready_indicator = "✅" if status["can_run"] else "❌"
            console.print(f"  {ready_indicator} `python test-scripts/{script_name}`")
        
        console.print()
    
    def print_missing_variables_help(self, config_status: Dict[str, Dict]):
        """Print help for configuring missing variables."""
        missing_any = any(not status["can_run"] for status in config_status.values())
        
        if not missing_any:
            return
        
        console.print(Panel.fit("🔧 Configuration Help", style="bold yellow"))
        
        help_table = Table(title="Missing Environment Variables")
        help_table.add_column("Variable", style="cyan")
        help_table.add_column("Platform", style="white")
        help_table.add_column("How to Get", style="green")
        
        # Collect all missing variables
        all_missing = set()
        for platform, status in config_status.items():
            if not status["can_run"]:
                for var in status["missing_required"]:
                    all_missing.add((var, platform))
        
        # Provide help for each missing variable
        help_info = {
            "AIRBYTE_API_KEY": "Airbyte Cloud → Settings → Account → Applications",
            "DATABRICKS_API_TOKEN": "Databricks → User Settings → Access Tokens", 
            "DATABRICKS_WORKSPACE_URL": "Your Databricks workspace URL (e.g., https://xyz.cloud.databricks.com)",
            "POWER_AUTOMATE_CLIENT_ID": "Azure Portal → App Registrations → New Registration",
            "POWER_AUTOMATE_CLIENT_SECRET": "Azure Portal → App Registration → Certificates & Secrets",
            "POWER_AUTOMATE_TENANT_ID": "Azure Portal → Azure Active Directory → Properties",
            "SNOWFLAKE_ACCOUNT": "Your Snowflake account URL",
            "SNOWFLAKE_USER": "Your Snowflake username",
            "SNOWFLAKE_PASSWORD": "Your Snowflake password",
            "OUTLOOK_CLIENT_ID": "Azure Portal → App Registrations (can reuse Power Automate app)",
            "OUTLOOK_CLIENT_SECRET": "Azure Portal → App Registration → Certificates & Secrets",
            "OUTLOOK_TENANT_ID": "Azure Portal → Azure Active Directory → Properties"
        }
        
        for var, platform in sorted(all_missing):
            help_text = help_info.get(var, "Check platform documentation")
            help_table.add_row(var, platform, help_text)
        
        console.print(help_table)
        console.print()
    
    def run_summary(self):
        """Run complete test summary."""
        console.print(Panel.fit(
            "🧪 Data Pipeline Monitoring Test Summary\n"
            "Configuration status and recent test results overview",
            style="bold blue"
        ))
        console.print()
        
        # Check configuration
        config_status = self.check_configuration()
        self.print_configuration_status(config_status)
        
        # Find and analyze recent results
        result_files = self.find_recent_test_results()
        analysis = self.analyze_recent_results(result_files)
        self.print_recent_results(result_files, analysis)
        
        # Print guides
        self.print_quick_start_guide(config_status)
        self.print_missing_variables_help(config_status)


def main():
    """Main entry point for test summary."""
    summary = TestSummary()
    summary.run_summary()


if __name__ == "__main__":
    main()
