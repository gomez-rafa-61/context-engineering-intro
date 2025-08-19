"""
Databricks API integration tools for job status monitoring.
"""

import asyncio
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import httpx

from models.job_status import JobStatusRecord, PlatformType
from models.platform_models import (
    DatabricksJobRun,
    DatabricksJobRunsResponse,
    DatabricksJobDetails,
    map_databricks_status,
)

logger = logging.getLogger(__name__)


class DatabricksAPIError(Exception):
    """Custom exception for Databricks API errors."""
    pass


class DatabricksAPIClient:
    """Databricks API client with retry logic and error handling."""
    
    def __init__(
        self,
        api_key: str,
        base_url: str,
        timeout: float = 30.0,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        """
        Initialize Databricks API client.
        
        Args:
            api_key: Databricks personal access token
            base_url: Databricks workspace URL (e.g., https://workspace.cloud.databricks.com)
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            retry_delay: Base delay between retries in seconds
        """
        if not api_key or not api_key.strip():
            raise ValueError("Databricks API key is required")
        if not base_url or not base_url.strip():
            raise ValueError("Databricks base URL is required")
            
        self.api_key = api_key.strip()
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # Default headers for all requests
        self.headers = {
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
            DatabricksAPIError: On API errors or failures
        """
        # Ensure endpoint starts with /api/
        if not endpoint.startswith('/api/'):
            endpoint = f"/api/2.1/{endpoint.lstrip('/')}"
        
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(self.max_retries + 1):
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=self.headers,
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
                            raise DatabricksAPIError("Rate limit exceeded. Check your Databricks API quota.")
                    
                    # Handle authentication errors
                    if response.status_code == 401:
                        raise DatabricksAPIError("Invalid Databricks API token")
                    
                    # Handle forbidden access
                    if response.status_code == 403:
                        raise DatabricksAPIError("Forbidden: Insufficient permissions for Databricks API")
                    
                    # Handle not found
                    if response.status_code == 404:
                        raise DatabricksAPIError(f"Resource not found: {endpoint}")
                    
                    # Handle server errors with retry
                    if 500 <= response.status_code < 600:
                        if attempt < self.max_retries:
                            delay = self.retry_delay * (2 ** attempt)
                            logger.warning(f"Server error {response.status_code}, retrying in {delay}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            raise DatabricksAPIError(f"Server error: {response.status_code} - {response.text}")
                    
                    # Handle other client errors
                    if 400 <= response.status_code < 500:
                        try:
                            error_data = response.json()
                            error_msg = error_data.get("error_code", response.text)
                        except Exception:
                            error_msg = response.text
                        raise DatabricksAPIError(f"Client error {response.status_code}: {error_msg}")
                    
                    # Handle success
                    if response.status_code == 200:
                        return response.json()
                    
                    # Handle unexpected status codes
                    raise DatabricksAPIError(f"Unexpected status code: {response.status_code}")
                    
            except httpx.RequestError as e:
                if attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Request error {e}, retrying in {delay}s")
                    await asyncio.sleep(delay)
                    continue
                else:
                    raise DatabricksAPIError(f"Request failed after {self.max_retries} retries: {str(e)}")
        
        raise DatabricksAPIError("Unexpected error in request handling")
    
    async def get_job_runs(
        self,
        job_id: Optional[int] = None,
        active_only: bool = False,
        completed_only: bool = False,
        limit: int = 25,
        offset: int = 0,
    ) -> DatabricksJobRunsResponse:
        """
        Get list of job runs from Databricks API.
        
        Args:
            job_id: Optional specific job ID to filter by
            active_only: Only return active runs
            completed_only: Only return completed runs
            limit: Maximum number of results to return
            offset: Pagination offset
            
        Returns:
            DatabricksJobRunsResponse with job run data
        """
        params = {
            "limit": min(max(limit, 1), 1000),  # Databricks allows up to 1000
            "offset": max(offset, 0),
        }
        
        if job_id is not None:
            params["job_id"] = job_id
        if active_only:
            params["active_only"] = "true"
        if completed_only:
            params["completed_only"] = "true"
        
        logger.info(f"Fetching Databricks job runs with params: {params}")
        
        try:
            response_data = await self._make_request("GET", "/jobs/runs/list", params=params)
            return DatabricksJobRunsResponse(**response_data)
        except Exception as e:
            logger.error(f"Failed to get Databricks job runs: {e}")
            raise DatabricksAPIError(f"Failed to get job runs: {str(e)}")
    
    async def get_job_run(self, run_id: int) -> DatabricksJobRun:
        """
        Get specific job run details from Databricks API.
        
        Args:
            run_id: Databricks job run ID
            
        Returns:
            DatabricksJobRun with run details
        """
        logger.info(f"Fetching Databricks job run: {run_id}")
        
        try:
            params = {"run_id": run_id}
            response_data = await self._make_request("GET", "/jobs/runs/get", params=params)
            return DatabricksJobRun(**response_data)
        except Exception as e:
            logger.error(f"Failed to get Databricks job run {run_id}: {e}")
            raise DatabricksAPIError(f"Failed to get job run {run_id}: {str(e)}")
    
    async def get_job(self, job_id: int) -> DatabricksJobDetails:
        """
        Get job details from Databricks API.
        
        Args:
            job_id: Databricks job ID
            
        Returns:
            DatabricksJobDetails with job information
        """
        logger.info(f"Fetching Databricks job: {job_id}")
        
        try:
            params = {"job_id": job_id}
            response_data = await self._make_request("GET", "/jobs/get", params=params)
            return DatabricksJobDetails(**response_data)
        except Exception as e:
            logger.error(f"Failed to get Databricks job {job_id}: {e}")
            raise DatabricksAPIError(f"Failed to get job {job_id}: {str(e)}")
    
    async def list_jobs(
        self,
        limit: int = 25,
        offset: int = 0,
        expand_tasks: bool = False,
    ) -> List[DatabricksJobDetails]:
        """
        List all jobs in the workspace.
        
        Args:
            limit: Maximum number of results to return
            offset: Pagination offset
            expand_tasks: Whether to include task details
            
        Returns:
            List of DatabricksJobDetails objects
        """
        params = {
            "limit": min(max(limit, 1), 25),  # Databricks default limit is 25
            "offset": max(offset, 0),
        }
        
        if expand_tasks:
            params["expand_tasks"] = "true"
        
        logger.info(f"Listing Databricks jobs with params: {params}")
        
        try:
            response_data = await self._make_request("GET", "/jobs/list", params=params)
            jobs_data = response_data.get("jobs", [])
            return [DatabricksJobDetails(**job) for job in jobs_data]
        except Exception as e:
            logger.error(f"Failed to list Databricks jobs: {e}")
            raise DatabricksAPIError(f"Failed to list jobs: {str(e)}")


# Convenience functions for use in agents
async def get_databricks_job_status(
    api_key: str,
    base_url: str,
    job_id: Optional[int] = None,
    limit: int = 50,
) -> List[JobStatusRecord]:
    """
    Get job status records from Databricks API.
    
    Args:
        api_key: Databricks personal access token
        base_url: Databricks workspace base URL
        job_id: Optional specific job ID to monitor
        limit: Maximum number of job runs to fetch
        
    Returns:
        List of JobStatusRecord objects
    """
    client = DatabricksAPIClient(api_key, base_url)
    
    try:
        # Get recent job runs
        runs_response = await client.get_job_runs(
            job_id=job_id,
            limit=limit
        )
        
        # Get job details for naming (cache job info)
        job_cache = {}
        
        job_records = []
        for run in runs_response.runs:
            # Get job name from cache or fetch
            job_name = f"Job {run.job_id}"
            if run.job_id not in job_cache:
                try:
                    job_details = await client.get_job(run.job_id)
                    job_name = job_details.settings.get("name", job_name)
                    job_cache[run.job_id] = job_name
                except Exception as e:
                    logger.warning(f"Failed to get job details for {run.job_id}: {e}")
                    job_cache[run.job_id] = job_name
            else:
                job_name = job_cache[run.job_id]
            
            # Parse timestamps (Databricks uses epoch milliseconds)
            last_run_time = None
            if run.start_time:
                try:
                    last_run_time = datetime.fromtimestamp(
                        run.start_time / 1000, tz=timezone.utc
                    )
                except (ValueError, TypeError):
                    logger.warning(f"Failed to parse start time for run {run.run_id}")
            
            # Calculate duration
            duration_seconds = None
            if run.execution_duration:
                duration_seconds = run.execution_duration // 1000  # Convert ms to seconds
            elif run.start_time and run.end_time:
                try:
                    duration_ms = run.end_time - run.start_time
                    duration_seconds = duration_ms // 1000
                except (TypeError, ValueError):
                    logger.warning(f"Failed to calculate duration for run {run.run_id}")
            
            # Extract error message if failed
            error_message = None
            if run.state.get("result_state") == "FAILED":
                state_message = run.state.get("state_message", "")
                if state_message:
                    error_message = state_message
            
            # Create job status record
            record = JobStatusRecord(
                job_id=f"databricks_{run.job_id}_{run.run_id}",
                platform=PlatformType.DATABRICKS,
                job_name=job_name,
                status=map_databricks_status(run.state),
                last_run_time=last_run_time,
                duration_seconds=duration_seconds,
                error_message=error_message,
                metadata={
                    "job_id": run.job_id,
                    "run_id": run.run_id,
                    "run_name": run.run_name,
                    "state": run.state,
                    "setup_duration": run.setup_duration,
                    "cleanup_duration": run.cleanup_duration,
                },
                checked_at=datetime.now(timezone.utc),
            )
            job_records.append(record)
        
        logger.info(f"Successfully retrieved {len(job_records)} Databricks job records")
        return job_records
        
    except Exception as e:
        logger.error(f"Failed to get Databricks job status: {e}")
        raise DatabricksAPIError(f"Failed to get job status: {str(e)}")


async def get_databricks_cluster_health(
    api_key: str,
    base_url: str,
) -> List[Dict[str, Any]]:
    """
    Get cluster health information from Databricks.
    
    Args:
        api_key: Databricks personal access token
        base_url: Databricks workspace base URL
        
    Returns:
        List of cluster health dictionaries
    """
    client = DatabricksAPIClient(api_key, base_url)
    
    try:
        # Get cluster list
        response_data = await client._make_request("GET", "/clusters/list")
        clusters = response_data.get("clusters", [])
        
        cluster_health = []
        for cluster in clusters:
            health_info = {
                "cluster_id": cluster.get("cluster_id"),
                "cluster_name": cluster.get("cluster_name"),
                "state": cluster.get("state"),
                "is_healthy": cluster.get("state") in ["RUNNING", "RESIZING"],
                "node_type": cluster.get("node_type_id"),
                "driver_node_type": cluster.get("driver_node_type_id"),
                "spark_version": cluster.get("spark_version"),
                "num_workers": cluster.get("num_workers", 0),
            }
            cluster_health.append(health_info)
        
        logger.info(f"Retrieved health info for {len(cluster_health)} clusters")
        return cluster_health
        
    except Exception as e:
        logger.error(f"Failed to get Databricks cluster health: {e}")
        raise DatabricksAPIError(f"Failed to get cluster health: {str(e)}")