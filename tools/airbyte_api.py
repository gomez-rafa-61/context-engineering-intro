"""
Airbyte API integration tools for job status monitoring.
"""

import asyncio
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone, timedelta
import httpx

from models.job_status import JobStatusRecord, PlatformType
from models.platform_models import (
    AirbyteJobsListResponse,
    AirbyteJobResponse,
    AirbyteConnectionResponse,
    map_airbyte_status,
)

logger = logging.getLogger(__name__)


class AirbyteAPIError(Exception):
    """Custom exception for Airbyte API errors."""
    pass


class AirbyteAPIClient:
    """Airbyte API client with retry logic, error handling, and token refresh capability."""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        base_url: str = "https://api.airbyte.com/v1",
        timeout: float = 30.0,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        """
        Initialize Airbyte API client.
        
        Args:
            api_key: Static Airbyte access token (for backwards compatibility)
            client_id: OAuth2 client ID for token refresh
            client_secret: OAuth2 client secret for token refresh
            base_url: Airbyte API base URL
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            retry_delay: Base delay between retries in seconds
        """
        # Support both static API key and OAuth2 token refresh
        if api_key and api_key.strip():
            # Static API key mode (backwards compatibility)
            self.api_key = api_key.strip()
            self.client_id = None
            self.client_secret = None
            self.token_expiry = None
            self.use_oauth = False
        elif client_id and client_secret:
            # OAuth2 mode with token refresh
            self.api_key = None
            self.client_id = client_id.strip()
            self.client_secret = client_secret.strip()
            self.token_expiry = None
            self.use_oauth = True
        else:
            raise ValueError("Either api_key or both client_id and client_secret are required")
            
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
    
    async def _refresh_token(self) -> str:
        """
        Refresh the OAuth2 authentication token.
        
        Returns:
            New access token
            
        Raises:
            AirbyteAPIError: On token refresh failure
        """
        if not self.use_oauth:
            raise AirbyteAPIError("Token refresh not available in static API key mode")
            
        auth_url = f"{self.base_url}/applications/token"
        
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    auth_url, 
                    json=payload,
                    timeout=self.timeout
                )
                response.raise_for_status()
                
                token_data = response.json()
                self.api_key = token_data["access_token"]
                
                # Set token expiry (default 1 hour with 5 min buffer)
                expires_in = token_data.get("expires_in", 3600)
                self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 300)
                
                logger.info("Successfully refreshed Airbyte API token")
                return self.api_key
                
        except httpx.HTTPStatusError as e:
            logger.error(f"Token refresh failed: {e.response.status_code} - {e.response.text}")
            raise AirbyteAPIError(f"Token refresh failed: {e.response.status_code}")
        except Exception as e:
            logger.error(f"Token refresh error: {str(e)}")
            raise AirbyteAPIError(f"Token refresh error: {str(e)}")
    
    async def _get_headers(self) -> Dict[str, str]:
        """
        Get authorization headers with automatic token refresh if needed.
        
        Returns:
            Headers dictionary with current access token
        """
        # Check if token refresh is needed (OAuth2 mode only)
        if self.use_oauth and (not self.api_key or 
                              (self.token_expiry and datetime.now() >= self.token_expiry)):
            await self._refresh_token()
        
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
    
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic and error handling.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            params: Query parameters
            json_data: JSON request body
            
        Returns:
            Response JSON data
            
        Raises:
            AirbyteAPIError: On API errors or failures
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        for attempt in range(self.max_retries + 1):
            try:
                headers = await self._get_headers()
                async with httpx.AsyncClient() as client:
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=headers,
                        params=params,
                        json=json_data,
                        timeout=self.timeout,
                    )
                    
                    # Handle rate limiting with exponential backoff
                    if response.status_code == 429:
                        if attempt < self.max_retries:
                            delay = self.retry_delay * (2 ** attempt)
                            logger.warning(f"Rate limited, retrying in {delay}s (attempt {attempt + 1})")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            raise AirbyteAPIError("Rate limit exceeded. Check your Airbyte API quota.")
                    
                    # Handle authentication errors with token refresh retry
                    if response.status_code == 401:
                        if self.use_oauth and attempt < self.max_retries:
                            logger.warning("Authentication failed, attempting token refresh")
                            try:
                                await self._refresh_token()
                                continue  # Retry with new token
                            except Exception as refresh_error:
                                logger.error(f"Token refresh failed: {refresh_error}")
                        raise AirbyteAPIError("Invalid Airbyte API key or token expired")
                    
                    # Handle forbidden access
                    if response.status_code == 403:
                        raise AirbyteAPIError("Forbidden: Insufficient permissions for Airbyte API")
                    
                    # Handle not found
                    if response.status_code == 404:
                        raise AirbyteAPIError(f"Resource not found: {endpoint}")
                    
                    # Handle server errors with retry
                    if 500 <= response.status_code < 600:
                        if attempt < self.max_retries:
                            delay = self.retry_delay * (2 ** attempt)
                            logger.warning(f"Server error {response.status_code}, retrying in {delay}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            raise AirbyteAPIError(f"Server error: {response.status_code} - {response.text}")
                    
                    # Handle other client errors
                    if 400 <= response.status_code < 500:
                        try:
                            error_data = response.json()
                            error_msg = error_data.get("message", response.text)
                        except Exception:
                            error_msg = response.text
                        raise AirbyteAPIError(f"Client error {response.status_code}: {error_msg}")
                    
                    # Handle success
                    if response.status_code == 200:
                        return response.json()
                    
                    # Handle unexpected status codes
                    raise AirbyteAPIError(f"Unexpected status code: {response.status_code}")
                    
            except httpx.RequestError as e:
                if attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Request error {e}, retrying in {delay}s")
                    await asyncio.sleep(delay)
                    continue
                else:
                    raise AirbyteAPIError(f"Request failed after {self.max_retries} retries: {str(e)}")
        
        raise AirbyteAPIError("Unexpected error in request handling")
    
    async def get_jobs(
        self,
        workspace_id: Optional[str] = None,
        job_type: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> AirbyteJobsListResponse:
        """
        Get list of jobs from Airbyte API.
        
        Args:
            workspace_id: Optional workspace ID filter
            job_type: Optional job type filter (sync, reset, etc.)
            limit: Maximum number of results to return
            offset: Pagination offset
            
        Returns:
            AirbyteJobsListResponse with job data
        """
        params = {
            "limit": min(max(limit, 1), 100),  # Enforce reasonable limits
            "offset": max(offset, 0),
        }
        
        if workspace_id:
            params["workspaceId"] = workspace_id
        if job_type:
            params["jobType"] = job_type
        
        logger.info(f"Fetching Airbyte jobs with params: {params}")
        
        try:
            response_data = await self._make_request("GET", "/jobs", params=params)
            return AirbyteJobsListResponse(**response_data)
        except Exception as e:
            logger.error(f"Failed to get Airbyte jobs: {e}")
            raise AirbyteAPIError(f"Failed to get jobs: {str(e)}")
    
    async def get_job(self, job_id: str) -> AirbyteJobResponse:
        """
        Get specific job details from Airbyte API.
        
        Args:
            job_id: Airbyte job ID
            
        Returns:
            AirbyteJobResponse with job details
        """
        logger.info(f"Fetching Airbyte job: {job_id}")
        
        try:
            response_data = await self._make_request("GET", f"/jobs/{job_id}")
            return AirbyteJobResponse(**response_data)
        except Exception as e:
            logger.error(f"Failed to get Airbyte job {job_id}: {e}")
            raise AirbyteAPIError(f"Failed to get job {job_id}: {str(e)}")
    
    async def get_connections(
        self,
        workspace_id: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> List[AirbyteConnectionResponse]:
        """
        Get list of connections from Airbyte API.
        
        Args:
            workspace_id: Optional workspace ID filter
            limit: Maximum number of results to return
            offset: Pagination offset
            
        Returns:
            List of AirbyteConnectionResponse objects
        """
        params = {
            "limit": min(max(limit, 1), 100),
            "offset": max(offset, 0),
        }
        
        if workspace_id:
            params["workspaceId"] = workspace_id
        
        logger.info(f"Fetching Airbyte connections with params: {params}")
        
        try:
            response_data = await self._make_request("GET", "/connections", params=params)
            connections_data = response_data.get("data", [])
            return [AirbyteConnectionResponse(**conn) for conn in connections_data]
        except Exception as e:
            logger.error(f"Failed to get Airbyte connections: {e}")
            raise AirbyteAPIError(f"Failed to get connections: {str(e)}")


# Convenience functions for use in agents
async def get_airbyte_job_status(
    api_key: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    workspace_id: Optional[str] = None,
    job_type: str = "sync",
    limit: int = 50,
) -> List[JobStatusRecord]:
    """
    Get job status records from Airbyte API.
    
    Args:
        api_key: Static Airbyte API access token (for backwards compatibility)
        client_id: OAuth2 client ID for token refresh
        client_secret: OAuth2 client secret for token refresh
        workspace_id: Optional workspace ID
        job_type: Type of jobs to fetch
        limit: Maximum number of jobs to fetch
        
    Returns:
        List of JobStatusRecord objects
    """
    client = AirbyteAPIClient(
        api_key=api_key,
        client_id=client_id,
        client_secret=client_secret
    )
    
    try:
        jobs_response = await client.get_jobs(
            workspace_id=workspace_id,
            job_type=job_type,
            limit=limit
        )
        
        job_records = []
        for job in jobs_response.data:
            # Parse timestamps
            last_run_time = None
            if job.started_at:
                try:
                    last_run_time = datetime.fromisoformat(
                        job.started_at.replace('Z', '+00:00')
                    )
                except ValueError:
                    logger.warning(f"Failed to parse start time for job {job.job_id}")
            
            # Calculate duration
            duration_seconds = None
            if job.started_at and job.ended_at:
                try:
                    start_time = datetime.fromisoformat(job.started_at.replace('Z', '+00:00'))
                    end_time = datetime.fromisoformat(job.ended_at.replace('Z', '+00:00'))
                    duration_seconds = int((end_time - start_time).total_seconds())
                except ValueError:
                    logger.warning(f"Failed to calculate duration for job {job.job_id}")
            
            # Create job status record
            record = JobStatusRecord(
                job_id=job.job_id,
                platform=PlatformType.AIRBYTE,
                job_name=job.config_name or f"Job {job.job_id}",
                status=map_airbyte_status(job.status),
                last_run_time=last_run_time,
                duration_seconds=duration_seconds,
                error_message=None,  # Airbyte API doesn't always provide error details
                metadata={
                    "config_id": job.config_id,
                    "job_type": job.job_type,
                    "created_at": job.created_at,
                    "updated_at": job.updated_at,
                },
                checked_at=datetime.now(timezone.utc),
            )
            job_records.append(record)
        
        logger.info(f"Successfully retrieved {len(job_records)} Airbyte job records")
        return job_records
        
    except Exception as e:
        logger.error(f"Failed to get Airbyte job status: {e}")
        raise AirbyteAPIError(f"Failed to get job status: {str(e)}")


async def get_airbyte_connection_health(
    api_key: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    workspace_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Get connection health information from Airbyte.
    
    Args:
        api_key: Static Airbyte API access token (for backwards compatibility)
        client_id: OAuth2 client ID for token refresh
        client_secret: OAuth2 client secret for token refresh
        workspace_id: Optional workspace ID
        
    Returns:
        List of connection health dictionaries
    """
    client = AirbyteAPIClient(
        api_key=api_key,
        client_id=client_id,
        client_secret=client_secret
    )
    
    try:
        connections = await client.get_connections(workspace_id=workspace_id)
        
        connection_health = []
        for conn in connections:
            health_info = {
                "connection_id": conn.connection_id,
                "connection_name": conn.name,
                "status": conn.status,
                "source_id": conn.source_id,
                "destination_id": conn.destination_id,
                "is_healthy": conn.status.lower() == "active",
            }
            connection_health.append(health_info)
        
        logger.info(f"Retrieved health info for {len(connection_health)} connections")
        return connection_health
        
    except Exception as e:
        logger.error(f"Failed to get Airbyte connection health: {e}")
        raise AirbyteAPIError(f"Failed to get connection health: {str(e)}")