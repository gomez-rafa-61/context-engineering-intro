#!/usr/bin/env python3
"""
Data Pipeline Monitoring CLI with real-time streaming and tool call visibility.
"""

import asyncio
import sys
import os
from typing import List
from datetime import datetime
from uuid import uuid4

# Add current directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table

from pydantic_ai import Agent
from agents.orchestrator_agent import orchestrator_agent
from agents.dependencies import OrchestratorDependencies
from config.settings import settings

console = Console()


async def stream_agent_interaction(user_input: str, conversation_history: List[str]) -> tuple[str, str]:
    """Stream agent interaction with real-time tool call display."""
    
    try:
        # Set up orchestrator dependencies
        orchestrator_deps = OrchestratorDependencies.from_settings(
            session_id=f"cli_{uuid4().hex[:8]}",
            monitoring_id=f"mon_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            from_email="monitoring@company.com"  # Default from email
        )
        
        # Build context with conversation history
        context = "\n".join(conversation_history[-6:]) if conversation_history else ""
        
        prompt = f"""Previous conversation:
{context}

User: {user_input}

Respond naturally and helpfully. For monitoring requests, coordinate the appropriate platform agents."""

        # Stream the agent execution
        async with orchestrator_agent.iter(prompt, deps=orchestrator_deps) as run:
            
            async for node in run:
                
                # Handle user prompt node
                if Agent.is_user_prompt_node(node):
                    pass  # Clean start - no processing messages
                
                # Handle model request node - stream the thinking process
                elif Agent.is_model_request_node(node):
                    # Show assistant prefix at the start
                    console.print("[bold blue]🤖 Orchestrator:[/bold blue] ", end="")
                    
                    # Stream model request events for real-time text
                    response_text = ""
                    async with node.stream(run.ctx) as request_stream:
                        async for event in request_stream:
                            # Handle different event types based on their type
                            event_type = type(event).__name__
                            
                            if event_type == "PartDeltaEvent":
                                # Extract content from delta
                                if hasattr(event, 'delta') and hasattr(event.delta, 'content_delta'):
                                    delta_text = event.delta.content_delta
                                    if delta_text:
                                        console.print(delta_text, end="")
                                        response_text += delta_text
                            elif event_type == "FinalResultEvent":
                                console.print()  # New line after streaming
                
                # Handle tool calls - this is the key part
                elif Agent.is_call_tools_node(node):
                    # Stream tool execution events
                    async with node.stream(run.ctx) as tool_stream:
                        async for event in tool_stream:
                            event_type = type(event).__name__
                            
                            if event_type == "FunctionToolCallEvent":
                                # Extract tool name from the part attribute  
                                tool_name = "Unknown Tool"
                                args = None
                                
                                # Check if the part attribute contains the tool call
                                if hasattr(event, 'part'):
                                    part = event.part
                                    
                                    # Check if part has tool_name directly
                                    if hasattr(part, 'tool_name'):
                                        tool_name = part.tool_name
                                    elif hasattr(part, 'function_name'):
                                        tool_name = part.function_name
                                    elif hasattr(part, 'name'):
                                        tool_name = part.name
                                    
                                    # Check for arguments in part
                                    if hasattr(part, 'args'):
                                        args = part.args
                                    elif hasattr(part, 'arguments'):
                                        args = part.arguments
                                
                                console.print(f"  🔹 [cyan]Calling tool:[/cyan] [bold]{tool_name}[/bold]")
                                
                                # Show tool args if available (truncated for readability)
                                if args and isinstance(args, dict):
                                    # Show first few characters of each arg
                                    arg_preview = []
                                    for key, value in list(args.items())[:3]:
                                        val_str = str(value)
                                        if len(val_str) > 50:
                                            val_str = val_str[:47] + "..."
                                        arg_preview.append(f"{key}={val_str}")
                                    if arg_preview:
                                        console.print(f"    [dim]Args: {', '.join(arg_preview)}[/dim]")
                            
                            elif event_type == "FunctionToolResultEvent":
                                # Display tool result
                                result = str(event.tool_return) if hasattr(event, 'tool_return') else "No result"
                                if len(result) > 150:
                                    result = result[:147] + "..."
                                console.print(f"  ✅ [green]Tool result:[/green] [dim]{result}[/dim]")
                
                # Handle end node  
                elif Agent.is_end_node(node):
                    # Don't show "Processing complete" - keep it clean
                    pass
        
        # Get final result
        final_result = run.result
        final_output = final_result.output if hasattr(final_result, 'output') else str(final_result)
        
        # Return both streamed and final content
        return (response_text.strip(), final_output)
        
    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")
        return ("", f"Error: {e}")


def show_help():
    """Display help information."""
    help_panel = Panel(
        """[bold cyan]Data Pipeline Monitoring Commands[/bold cyan]

[bold green]Monitoring Commands:[/bold green]
• [yellow]monitor all[/yellow] - Monitor all data platforms comprehensively
• [yellow]monitor airbyte[/yellow] - Monitor only Airbyte platform
• [yellow]monitor status[/yellow] - Get quick status overview
• [yellow]health check[/yellow] - Perform system health assessment

[bold green]Utility Commands:[/bold green]  
• [yellow]help[/yellow] - Show this help message
• [yellow]config[/yellow] - Show current configuration
• [yellow]test connection[/yellow] - Test API connections
• [yellow]exit[/yellow] or [yellow]quit[/yellow] - Exit the CLI

[bold green]Example Usage:[/bold green]
• "Monitor all platforms and send notifications"
• "Check Airbyte job status for the last 24 hours"
• "Show me the health summary"
• "Test Snowflake database connection"

[dim]The system monitors Airbyte, Databricks, Power Automate, and Snowflake Tasks.[/dim]
        """,
        style="blue",
        padding=(1, 2)
    )
    console.print(help_panel)


def show_config():
    """Display current configuration."""
    config_table = Table(title="Current Configuration")
    config_table.add_column("Setting", style="cyan")
    config_table.add_column("Value", style="green")
    
    # Show key configuration items (without sensitive data)
    config_items = [
        ("LLM Provider", settings.llm_provider),
        ("LLM Model", settings.llm_model),
        ("Environment", settings.app_env),
        ("Debug Mode", str(settings.debug)),
        ("Monitoring Interval", f"{settings.monitoring_interval_minutes} minutes"),
        ("Snowflake Database", f"{settings.snowflake_database}.{settings.snowflake_schema}"),
        ("Airbyte Base URL", settings.airbyte_base_url),
        ("Databricks Base URL", settings.databricks_base_url if hasattr(settings, 'databricks_base_url') else "Not configured"),
    ]
    
    for setting, value in config_items:
        config_table.add_row(setting, value)
    
    console.print(config_table)


async def main():
    """Main conversation loop."""
    
    # Show welcome
    welcome = Panel(
        "[bold blue]🔍 Data Pipeline Monitoring System[/bold blue]\n\n"
        "[green]Real-time monitoring across multiple data platforms[/green]\n"
        "[yellow]Monitoring: Airbyte • Databricks • Power Automate • Snowflake[/yellow]\n\n"
        "[dim]Type 'help' for commands or 'exit' to quit[/dim]",
        style="blue",
        padding=(1, 2)
    )
    console.print(welcome)
    console.print()
    
    conversation_history = []
    
    while True:
        try:
            # Get user input
            user_input = Prompt.ask("[bold green]You").strip()
            
            # Handle built-in commands
            if user_input.lower() in ['exit', 'quit']:
                console.print("\n[yellow]👋 Monitoring session ended. Goodbye![/yellow]")
                break
                
            if user_input.lower() == 'help':
                show_help()
                continue
                
            if user_input.lower() == 'config':
                show_config()
                continue
                
            if not user_input:
                continue
            
            # Add to history
            conversation_history.append(f"User: {user_input}")
            
            # Stream the interaction and get response
            streamed_text, final_response = await stream_agent_interaction(user_input, conversation_history)
            
            # Handle the response display
            if streamed_text:
                # Response was streamed, just add spacing
                console.print()
                conversation_history.append(f"Assistant: {streamed_text}")
            elif final_response and final_response.strip():
                # Response wasn't streamed, display with proper formatting
                console.print(f"[bold blue]🤖 Orchestrator:[/bold blue] {final_response}")
                console.print()
                conversation_history.append(f"Assistant: {final_response}")
            else:
                # No response
                console.print()
            
        except KeyboardInterrupt:
            console.print("\n[yellow]Use 'exit' to quit[/yellow]")
            continue
            
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")
            continue


if __name__ == "__main__":
    # Show startup information
    startup_info = Panel(
        f"[bold green]🚀 Starting Data Pipeline Monitor[/bold green]\n"
        f"[dim]Environment: {settings.app_env}[/dim]\n"
        f"[dim]Model: {settings.llm_model}[/dim]\n"
        f"[dim]Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/dim]",
        style="green"
    )
    console.print(startup_info)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[yellow]Monitoring interrupted by user[/yellow]")
    except Exception as e:
        console.print(f"\n[red]Fatal error: {e}[/red]")
        sys.exit(1)