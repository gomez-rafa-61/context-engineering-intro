#!/usr/bin/env python3
"""
Airbyte Status Checker MCP Server

A comprehensive MCP server for monitoring Airbyte connections, jobs, and streams.
Provides real-time status information and the ability to trigger syncs.
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
import logging

import httpx
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    Resource,
    Tool,
    TextContent,
    ImageContent,
    EmbeddedResource,
    LoggingLevel
)
from pydantic import BaseModel, Field

# Configure logging to avoid stdout interference
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger(__name__)

# Initialize MCP server
server = Server("airbyte-status-checker")

# Airbyte API Configuration
class AirbyteConfig:
    def __init__(self):
        self.base_url = "https://api.airbyte.com/v1"
        self.auth_url = "https://api.airbyte.com/v1/applications/token"
        self.workspace_id = os.getenv("AIRBYTE_WORKSPACE_ID")
        self.client_id = os.getenv("AIRBYTE_CLIENT_ID")
        self.client_secret = os.getenv("AIRBYTE_CLIENT_SECRET")
        self.auth_token = os.getenv("AIRBYTE_AUTH_TOKEN")
        self.token_expiry = None
        
        if not all([self.workspace_id, self.client_id, self.client_secret, self.auth_token]):
            raise ValueError(f"Missing required Airbyte configuration. Check your .env file. workspace-{self.workspace_id}-{self.client_id}-{self.client_secret}")

# Pydantic models for type safety
class StreamStatus(BaseModel):
    stream_name: str
    status: str
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration_seconds: Optional[int] = None

class ConnectionJobStatus(BaseModel):
    connection_name: str
    job_id: int
    job_status: str
    job_start_time: Optional[str] = None
    job_end_time: Optional[str] = None
    duration_seconds: Optional[int] = None
    streams: List[StreamStatus] = []

# Tool parameter models
class ConnectionNameInput(BaseModel):
    connection_name: str = Field(description="Name of the connection to operate on")

class JobIdInput(BaseModel):
    job_id: int = Field(description="Unique identifier of the job to retrieve details for")

class EmptyInput(BaseModel):
    random_string: str = Field(default="dummy", description="Dummy parameter for no-parameter tools")

# Global configuration instance
config = AirbyteConfig()

class AirbyteAPI:
    """Airbyte API client with token refresh capability"""
    
    def __init__(self, config: AirbyteConfig):
        self.config = config
        self.client = httpx.AsyncClient(timeout=30.0)

    async def _refresh_token(self) -> str:
        """Refresh the authentication token"""
        auth_url = f"{self.config.base_url}/applications/token"
        
        payload = {
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret
        }
        
        response = await self.client.post(auth_url, json=payload)
        response.raise_for_status()
        
        token_data = response.json()
        self.config.auth_token = token_data["access_token"]
        
        # Set token expiry (assuming 1 hour, adjust based on actual response)
        expires_in = token_data.get("expires_in", 3600)
        self.config.token_expiry = datetime.now() + timedelta(seconds=expires_in - 300)  # 5 min buffer
        
        return self.config.auth_token
        
    async def _get_headers(self) -> Dict[str, str]:
        """Get authorization headers with token refresh if needed"""
        if not self.config.auth_token or (self.config.token_expiry is None) or (datetime.now() >= self.config.token_expiry):
            self.config.auth_token = await self._refresh_token()
        
        return {
            "Authorization": f"Bearer {self.config.auth_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    async def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make authenticated request to Airbyte API"""
        url = f"{self.config.base_url}/{endpoint.lstrip('/')}"
        headers = await self._get_headers()
        
        try:
            logger.debug(f"Method:{method}, URL:{url}, Headers:{headers}, Params:{kwargs}")
            response = await self.client.request(method, url, headers=headers, **kwargs)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                # Token might be expired, you can implement refresh logic here
                logger.error("Authentication failed. Token may be expired.")
                raise Exception("Authentication failed. Please check your token.")
            raise Exception(f"API request failed: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            logger.error(f"Request failed: {str(e)}")
            raise
    
    async def get_connections(self, workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all connections for a workspace"""

        params = {
            "limit": 100
        }

        response = await self._make_request("GET", "connections", params=params)
        return response.get("data", [])
    
    async def get_connection_by_name(self, connection_name: str) -> Optional[Dict[str, Any]]:
        """Get connection details by name"""
        connections = await self.get_connections(self.config.workspace_id)
        for conn in connections:
            if conn.get("name") == connection_name:
                return conn
        return None
    
    async def get_jobs(self, connection_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get jobs for a specific connection"""
        params = {
            "connectionId": connection_id,
            "limit": limit
        }
        response = await self._make_request("GET", "jobs", params=params)
        return response.get("data", [])
    
    async def get_job_details(self, job_id: int) -> Dict[str, Any]:
        """Get detailed information about a specific job"""
        response = await self._make_request("GET", f"jobs/{job_id}")
        return response
    
    async def trigger_sync(self, connection_id: str) -> Dict[str, Any]:
        """Trigger a sync for a connection"""
        payload = {
            "connectionId": connection_id,
            "jobType": "sync"
        }
        response = await self._make_request("POST", "jobs", json=payload)
        return response
    
    async def get_connection_schema(self, connection_id: str) -> List[str]:
        """Get stream names for a connection"""
        try:
            # First try to get from connection details
            response = await self._make_request("GET", f"connections/{connection_id}")
            
            # Extract stream names from the connection configuration
            streams = []
            if "configurations" in response and "streams" in response["configurations"]:
                for stream in response["configurations"]["streams"]:
                    if "stream" in stream and "name" in stream["stream"]:
                        streams.append(stream["stream"]["name"])
            
            return streams
        except Exception as e:
            logger.error(f"Failed to get connection schema: {str(e)}")
            return []

# Initialize API client
api_client = AirbyteAPI(config)

@server.list_tools()
async def handle_list_tools() -> list[Tool]:
    """List available tools"""
    return [
        Tool(
            name="list_all_connections",
            description="List all connections in the workspace with their basic information",
            inputSchema={
                "type": "object",
                "properties": {
                    "random_string": {
                        "type": "string",
                        "description": "Dummy parameter for no-parameter tools"
                    }
                },
                "required": ["random_string"]
            }
        ),
        Tool(
            name="get_connection_streams",
            description="Get the list of stream sources associated with a specific connection name",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection_name": {
                        "type": "string",
                        "description": "Name of the connection to operate on"
                    }
                },
                "required": ["connection_name"]
            }
        ),
        Tool(
            name="check_connection_status",
            description="Check the status of a connection and its latest job with stream-level details",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection_name": {
                        "type": "string",
                        "description": "Name of the connection to operate on"
                    }
                },
                "required": ["connection_name"]
            }
        ),
        Tool(
            name="trigger_sync",
            description="Trigger a sync for a specific connection by name",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection_name": {
                        "type": "string",
                        "description": "Name of the connection to operate on"
                    }
                },
                "required": ["connection_name"]
            }
        ),
        # Tool(
        #     name="list_all_streams",
        #     description="List all streams across all connections in the workspace",
        #     inputSchema={
        #         "type": "object",
        #         "properties": {
        #             "random_string": {
        #                 "type": "string",
        #                 "description": "Dummy parameter for no-parameter tools"
        #             }
        #         },
        #         "required": ["random_string"]
        #     }
        # ),
        Tool(
            name="check_jobs_health",
            description="Check the health status of recent jobs across all connections",
            inputSchema={
                "type": "object",
                "properties": {
                    "random_string": {
                        "type": "string",
                        "description": "Dummy parameter for no-parameter tools"
                    }
                },
                "required": ["random_string"]
            }
        ),
        Tool(
            name="get_job_details",
            description="Get detailed information about a specific job by ID",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "integer",
                        "description": "Unique identifier of the job to retrieve details for"
                    }
                },
                "required": ["job_id"]
            }
        )
        # ,
        # Tool(
        #     name="check_source_status",
        #     description="Check the status of all source connectors in the workspace",
        #     inputSchema={
        #         "type": "object",
        #         "properties": {
        #             "random_string": {
        #                 "type": "string",
        #                 "description": "Dummy parameter for no-parameter tools"
        #             }
        #         },
        #         "required": ["random_string"]
        #     }
        # )
        # ,
        # Tool(
        #     name="list_all_sources",
        #     description="List all source connectors in the workspace",
        #     inputSchema={
        #         "type": "object",
        #         "properties": {
        #             "random_string": {
        #                 "type": "string",
        #                 "description": "Dummy parameter for no-parameter tools"
        #             }
        #         },
        #         "required": ["random_string"]
        #     }
        # )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls"""
    try:
        if name == "list_all_connections":
            result = await list_all_connections()
        elif name == "get_connection_streams":
            result = await get_connection_streams(arguments["connection_name"])
        elif name == "check_connection_status":
            result = await check_connection_status(arguments["connection_name"])
        elif name == "trigger_sync":
            result = await trigger_sync(arguments["connection_name"])
        # elif name == "list_all_streams":
        #     result = await list_all_streams()
        elif name == "check_jobs_health":
            result = await check_jobs_health()
        elif name == "get_job_details":
            result = await get_job_details(arguments["job_id"])
        # elif name == "check_source_status":
        #     result = await check_source_status()
        # elif name == "list_all_sources":
        #     result = await list_all_sources()
        else:
            result = f"Unknown tool: {name}"
        
        return [TextContent(type="text", text=result)]
    except Exception as e:
        return [TextContent(type="text", text=f"Error: {str(e)}")]

# Tool implementations
async def list_all_connections() -> str:
    """List all connections in the workspace with their basic information"""
    try:
        connections = await api_client.get_connections(config.workspace_id)
        
        if not connections:
            return "No connections found in the workspace."
        
        result = []
        for conn in connections:
            result.append({
                "connection_id": conn.get("connectionId"),
                "name": conn.get("name"),
                "status": conn.get("status"),
                "source_name": conn.get("source", {}).get("name"),
                "destination_name": conn.get("destination", {}).get("name")
            })
        
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error listing connections: {str(e)}"

async def get_connection_streams(connection_name: str) -> str:
    """Get the list of stream sources associated with a specific connection name"""
    try:
        connection = await api_client.get_connection_by_name(connection_name)
        if not connection:
            return f"Connection '{connection_name}' not found."
        
        connection_id = connection.get("connectionId")
        streams = await api_client.get_connection_schema(connection_id)
        
        if not streams:
            return f"No streams found for connection '{connection_name}' or unable to retrieve stream information."
        
        result = {
            "connection_name": connection_name,
            "connection_id": connection_id,
            "streams": streams
        }
        
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error getting connection streams: {str(e)}"

async def check_connection_status(connection_name: str) -> str:
    """Check the status of a connection and its latest job with stream-level details"""
    try:
        connection = await api_client.get_connection_by_name(connection_name)
        if not connection:
            return f"Connection '{connection_name}' not found."
        
        connection_id = connection.get("connectionId")
        jobs = await api_client.get_jobs(connection_id, limit=1)
        
        if not jobs:
            return f"No jobs found for connection '{connection_name}'."
        
        latest_job = jobs[0]
        job_id = latest_job.get("jobId")
        
        # Get detailed job information
        job_details = await api_client.get_job_details(job_id)
        
        # Calculate duration
        start_time = latest_job.get("startTime")
        end_time = latest_job.get("endTime")
        duration_seconds = None
        
        if start_time and end_time:
            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            duration_seconds = int((end_dt - start_dt).total_seconds())
        
        # Get streams for this connection
        stream_names = await api_client.get_connection_schema(connection_id)
        
        # Create stream status objects (simplified - real implementation would need stream-level job data)
        streams = []
        for stream_name in stream_names:
            stream_status = StreamStatus(
                stream_name=stream_name,
                status=latest_job.get("status", "unknown"),
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration_seconds
            )
            streams.append(stream_status.dict())
        
        result = ConnectionJobStatus(
            connection_name=connection_name,
            job_id=job_id,
            job_status=latest_job.get("status"),
            job_start_time=start_time,
            job_end_time=end_time,
            duration_seconds=duration_seconds,
            streams=streams
        )
        
        return json.dumps(result.dict(), indent=2)
    except Exception as e:
        return f"Error checking connection status: {str(e)}"

async def trigger_sync(connection_name: str) -> str:
    """Trigger a sync for a specific connection by name"""
    try:
        connection = await api_client.get_connection_by_name(connection_name)
        if not connection:
            return f"Connection '{connection_name}' not found."
        
        connection_id = connection.get("connectionId")
        result = await api_client.trigger_sync(connection_id)
        
        response = {
            "connection_name": connection_name,
            "connection_id": connection_id,
            "job_id": result.get("jobId"),
            "status": "triggered",
            "message": f"Sync triggered successfully for connection '{connection_name}'"
        }
        
        return json.dumps(response, indent=2)
    except Exception as e:
        return f"Error triggering sync: {str(e)}"

async def check_jobs_health() -> str:
    """Check the health status of recent jobs across all connections"""
    try:
        connections = await api_client.get_connections(config.workspace_id)
        
        if not connections:
            return "No connections found in the workspace."
        
        health_report = []
        for conn in connections:
            connection_name = conn.get("name")
            connection_id = conn.get("connectionId")
            
            try:
                jobs = await api_client.get_jobs(connection_id, limit=5)
                
                if jobs:
                    latest_job = jobs[0]
                    job_health = {
                        "connection_name": connection_name,
                        "connection_id": connection_id,
                        "latest_job_status": latest_job.get("status"),
                        "latest_job_time": latest_job.get("startTime"),
                        "recent_jobs_count": len(jobs),
                        "failed_jobs": len([j for j in jobs if j.get("status") == "failed"]),
                        "success_rate": f"{(len([j for j in jobs if j.get('status') == 'succeeded']) / len(jobs) * 100):.1f}%"
                    }
                else:
                    job_health = {
                        "connection_name": connection_name,
                        "connection_id": connection_id,
                        "latest_job_status": "no_jobs",
                        "latest_job_time": None,
                        "recent_jobs_count": 0,
                        "failed_jobs": 0,
                        "success_rate": "N/A"
                    }
                
                health_report.append(job_health)
            except Exception as e:
                logger.error(f"Error checking jobs for connection {connection_name}: {str(e)}")
                continue
        
        return json.dumps(health_report, indent=2)
    except Exception as e:
        return f"Error checking jobs health: {str(e)}"

async def get_job_details(job_id: int) -> str:
    """Get detailed information about a specific job by ID"""
    try:
        job_details = await api_client.get_job_details(job_id)
        
        # Format the response for better readability
        formatted_details = {
            "job_id": job_details.get("jobId"),
            "status": job_details.get("status"),
            "job_type": job_details.get("jobType"),
            "start_time": job_details.get("startTime"),
            "end_time": job_details.get("endTime"),
            "connection_id": job_details.get("connectionId"),
            "bytes_synced": job_details.get("bytesSynced"),
            "records_synced": job_details.get("recordsSynced")
        }
        
        return json.dumps(formatted_details, indent=2)
    except Exception as e:
        return f"Error getting job details: {str(e)}"

# async def list_all_streams() -> str:
#     """List all streams across all connections in the workspace"""
#     try:
#         connections = await api_client.get_connections(config.workspace_id)
        
#         if not connections:
#             return "No connections found in the workspace."
        
#         all_streams = {}
#         for conn in connections:
#             connection_name = conn.get("name")
#             connection_id = conn.get("connectionId")
#             streams = await api_client.get_connection_schema(connection_id)
            
#             if streams:
#                 all_streams[connection_name] = {
#                     "connection_id": connection_id,
#                     "streams": streams
#                 }
        
#         return json.dumps(all_streams, indent=2)
#     except Exception as e:
#         return f"Error listing all streams: {str(e)}"

# async def check_source_status() -> str:
#     """Check the status of all source connectors in the workspace"""
#     try:
#         # This would require additional API calls to get source information
#         # For now, we'll get it from connections
#         connections = await api_client.get_connections(config.workspace_id)
        
#         sources = {}
#         for conn in connections:
#             source_info = conn.get("source", {})
#             source_name = source_info.get("name")
            
#             if source_name and source_name not in sources:
#                 sources[source_name] = {
#                     "source_id": source_info.get("sourceId"),
#                     "source_type": source_info.get("sourceType"),
#                     "connections": []
#                 }
            
#             if source_name:
#                 sources[source_name]["connections"].append({
#                     "connection_name": conn.get("name"),
#                     "connection_status": conn.get("status")
#                 })
        
#         return json.dumps(sources, indent=2)
#     except Exception as e:
#         return f"Error checking source status: {str(e)}"

# async def list_all_sources() -> str:
#     """List all source connectors in the workspace"""
#     try:
#         connections = await api_client.get_connections(config.workspace_id)
        
#         sources = set()
#         for conn in connections:
#             source_info = conn.get("source", {})
#             if source_info.get("name"):
#                 sources.add(source_info.get("name"))
        
#         sources_list = list(sources)
#         return json.dumps({"sources": sources_list}, indent=2)
#     except Exception as e:
#         return f"Error listing sources: {str(e)}"

# All tools now use @server.tool() decorators with Pydantic models

async def main():
    """Main entry point for the MCP server"""
    try:
        # Test configuration
        logger.info("Starting Airbyte Status Checker MCP Server...")
        logger.info(f"Workspace ID: {config.workspace_id}")
        
        # Run the MCP server
        async with stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                server.create_initialization_options()
            )
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())