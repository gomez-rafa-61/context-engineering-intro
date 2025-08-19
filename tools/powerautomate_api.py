"""
Power Automate (Microsoft Graph) API integration tools for flow monitoring.
"""

import asyncio
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import httpx

from models.job_status import JobStatusRecord, PlatformType
from models.platform_models import (
    PowerAutomateFlowRunsResponse,
    PowerAutomateFlow,
    map_powerautomate_status,
)

logger = logging.getLogger(__name__)


class PowerAutomateAPIError(Exception):
    """Custom exception for Power Automate API errors."""
    pass


class PowerAutomateAPIClient:
    """Power Automate API client using Microsoft Graph with retry logic."""
    
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        base_url: str = "https://graph.microsoft.com/v1.0",
        timeout: float = 30.0,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        """
        Initialize Power Automate API client.
        
        Args:
            client_id: Azure AD application client ID
            client_secret: Azure AD application client secret
            tenant_id: Azure AD tenant ID
            base_url: Microsoft Graph API base URL
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            retry_delay: Base delay between retries in seconds
        """
        if not all([client_id, client_secret, tenant_id]):
            raise ValueError("Client ID, client secret, and tenant ID are required")
            
        self.client_id = client_id.strip()
        self.client_secret = client_secret.strip()
        self.tenant_id = tenant_id.strip()
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.access_token = None
        self.token_expires_at = None
    
    async def _get_access_token(self) -> str:
        """Get OAuth2 access token for Microsoft Graph API."""
        if self.access_token and self.token_expires_at:
            if datetime.now(timezone.utc) < self.token_expires_at:
                return self.access_token
        
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(token_url, data=data, timeout=self.timeout)
                
                if response.status_code != 200:
                    raise PowerAutomateAPIError(f"Token request failed: {response.status_code} - {response.text}")
                
                token_data = response.json()
                self.access_token = token_data["access_token"]
                expires_in = token_data.get("expires_in", 3600)
                self.token_expires_at = datetime.now(timezone.utc).replace(
                    microsecond=0
                ) + datetime.timedelta(seconds=expires_in - 60)  # 60s buffer
                
                return self.access_token
                
        except Exception as e:
            raise PowerAutomateAPIError(f"Failed to get access token: {str(e)}")
    
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request with retry logic and error handling."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        for attempt in range(self.max_retries + 1):
            try:
                access_token = await self._get_access_token()
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                }
                
                async with httpx.AsyncClient() as client:
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=headers,
                        params=params,
                        json=json_data,
                        timeout=self.timeout,
                    )
                    
                    # Handle rate limiting
                    if response.status_code == 429:
                        if attempt < self.max_retries:
                            delay = self.retry_delay * (2 ** attempt)
                            logger.warning(f"Rate limited, retrying in {delay}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            raise PowerAutomateAPIError("Rate limit exceeded")
                    
                    # Handle authentication errors
                    if response.status_code == 401:
                        self.access_token = None  # Force token refresh
                        if attempt < self.max_retries:
                            continue
                        raise PowerAutomateAPIError("Authentication failed")
                    
                    # Handle other errors
                    if response.status_code >= 400:
                        error_msg = f"API error {response.status_code}: {response.text}"
                        raise PowerAutomateAPIError(error_msg)
                    
                    return response.json()
                    
            except httpx.RequestError as e:
                if attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** attempt)
                    await asyncio.sleep(delay)
                    continue
                raise PowerAutomateAPIError(f"Request failed: {str(e)}")
        
        raise PowerAutomateAPIError("Max retries exceeded")
    
    async def get_flows(self) -> List[PowerAutomateFlow]:
        """Get list of Power Automate flows."""
        try:
            response_data = await self._make_request("GET", "solutions/flows")
            flows_data = response_data.get("value", [])
            return [PowerAutomateFlow(**flow) for flow in flows_data]
        except Exception as e:
            logger.error(f"Failed to get Power Automate flows: {e}")
            raise PowerAutomateAPIError(f"Failed to get flows: {str(e)}")
    
    async def get_flow_runs(self, flow_id: str, limit: int = 50) -> PowerAutomateFlowRunsResponse:
        """Get flow run history for a specific flow."""
        params = {"$top": min(max(limit, 1), 1000)}
        
        try:
            endpoint = f"solutions/flows/{flow_id}/runs"
            response_data = await self._make_request("GET", endpoint, params=params)
            return PowerAutomateFlowRunsResponse(**response_data)
        except Exception as e:
            logger.error(f"Failed to get flow runs for {flow_id}: {e}")
            raise PowerAutomateAPIError(f"Failed to get flow runs: {str(e)}")


# Convenience functions for use in agents
async def get_powerautomate_job_status(
    client_id: str,
    client_secret: str,
    tenant_id: str,
    limit: int = 50,
) -> List[JobStatusRecord]:
    """Get job status records from Power Automate API."""
    client = PowerAutomateAPIClient(client_id, client_secret, tenant_id)
    
    try:
        flows = await client.get_flows()
        job_records = []
        
        for flow in flows[:10]:  # Limit to first 10 flows to avoid timeout
            try:
                runs_response = await client.get_flow_runs(flow.id, limit=10)
                
                for run in runs_response.value:
                    # Parse timestamps
                    last_run_time = None
                    if run.start_time:
                        try:
                            last_run_time = datetime.fromisoformat(
                                run.start_time.replace('Z', '+00:00')
                            )
                        except ValueError:
                            logger.warning(f"Failed to parse start time for run {run.run_id}")
                    
                    # Calculate duration
                    duration_seconds = None
                    if run.start_time and run.end_time:
                        try:
                            start = datetime.fromisoformat(run.start_time.replace('Z', '+00:00'))
                            end = datetime.fromisoformat(run.end_time.replace('Z', '+00:00'))
                            duration_seconds = int((end - start).total_seconds())
                        except ValueError:
                            pass
                    
                    record = JobStatusRecord(
                        job_id=f"powerautomate_{flow.id}_{run.run_id}",
                        platform=PlatformType.POWER_AUTOMATE,
                        job_name=flow.display_name,
                        status=map_powerautomate_status(run.status),
                        last_run_time=last_run_time,
                        duration_seconds=duration_seconds,
                        metadata={
                            "flow_id": flow.id,
                            "run_id": run.run_id,
                            "flow_state": flow.state,
                        },
                        checked_at=datetime.now(timezone.utc),
                    )
                    job_records.append(record)
                    
            except Exception as e:
                logger.warning(f"Failed to get runs for flow {flow.id}: {e}")
                continue
        
        logger.info(f"Successfully retrieved {len(job_records)} Power Automate records")
        return job_records
        
    except Exception as e:
        logger.error(f"Failed to get Power Automate job status: {e}")
        raise PowerAutomateAPIError(f"Failed to get job status: {str(e)}")