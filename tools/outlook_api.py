"""
Outlook API integration tools for email notifications.
"""

import asyncio
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import httpx
import base64

from models.notification_models import (
    EmailNotification,
    NotificationResult,
)

logger = logging.getLogger(__name__)


class OutlookAPIError(Exception):
    """Custom exception for Outlook API errors."""
    pass


class OutlookAPIClient:
    """Outlook API client using Microsoft Graph with OAuth2."""
    
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
        Initialize Outlook API client.
        
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
                    raise OutlookAPIError(f"Token request failed: {response.status_code} - {response.text}")
                
                token_data = response.json()
                self.access_token = token_data["access_token"]
                expires_in = token_data.get("expires_in", 3600)
                self.token_expires_at = datetime.now(timezone.utc).replace(
                    microsecond=0
                ) + datetime.timedelta(seconds=expires_in - 60)  # 60s buffer
                
                return self.access_token
                
        except Exception as e:
            raise OutlookAPIError(f"Failed to get access token: {str(e)}")
    
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request with retry logic and error handling."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        for attempt in range(self.max_retries + 1):
            try:
                access_token = await self._get_access_token()
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }
                
                async with httpx.AsyncClient() as client:
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=headers,
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
                            raise OutlookAPIError("Rate limit exceeded")
                    
                    # Handle authentication errors
                    if response.status_code == 401:
                        self.access_token = None  # Force token refresh
                        if attempt < self.max_retries:
                            continue
                        raise OutlookAPIError("Authentication failed")
                    
                    # Handle other errors
                    if response.status_code >= 400:
                        error_msg = f"API error {response.status_code}: {response.text}"
                        raise OutlookAPIError(error_msg)
                    
                    return response.json() if response.content else {}
                    
            except httpx.RequestError as e:
                if attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** attempt)
                    await asyncio.sleep(delay)
                    continue
                raise OutlookAPIError(f"Request failed: {str(e)}")
        
        raise OutlookAPIError("Max retries exceeded")
    
    async def send_email(
        self,
        from_email: str,
        notification: EmailNotification,
    ) -> NotificationResult:
        """
        Send email notification via Microsoft Graph API.
        
        Args:
            from_email: Sender email address
            notification: Email notification object
            
        Returns:
            NotificationResult with send status
        """
        try:
            # Prepare recipients by type
            to_recipients = []
            cc_recipients = []
            bcc_recipients = []
            
            for recipient in notification.recipients:
                recipient_obj = {
                    "emailAddress": {
                        "address": recipient.email,
                        "name": recipient.name or recipient.email,
                    }
                }
                
                if recipient.type == "to":
                    to_recipients.append(recipient_obj)
                elif recipient.type == "cc":
                    cc_recipients.append(recipient_obj)
                elif recipient.type == "bcc":
                    bcc_recipients.append(recipient_obj)
            
            # Prepare email message
            message = {
                "subject": notification.subject,
                "body": {
                    "contentType": "HTML" if "<html>" in notification.body.lower() else "Text",
                    "content": notification.body,
                },
                "toRecipients": to_recipients,
            }
            
            if cc_recipients:
                message["ccRecipients"] = cc_recipients
            if bcc_recipients:
                message["bccRecipients"] = bcc_recipients
            
            # Set importance based on priority
            if notification.priority.value == "urgent":
                message["importance"] = "high"
            elif notification.priority.value == "high":
                message["importance"] = "high"
            elif notification.priority.value == "low":
                message["importance"] = "low"
            else:
                message["importance"] = "normal"
            
            # Add attachments if any
            if notification.attachments:
                attachments = []
                for attachment_path in notification.attachments:
                    try:
                        # Read attachment file and encode
                        with open(attachment_path, 'rb') as f:
                            attachment_content = base64.b64encode(f.read()).decode('utf-8')
                        
                        attachment_name = attachment_path.split('/')[-1]  # Get filename
                        attachments.append({
                            "@odata.type": "#microsoft.graph.fileAttachment",
                            "name": attachment_name,
                            "contentBytes": attachment_content,
                        })
                    except Exception as e:
                        logger.warning(f"Failed to attach file {attachment_path}: {e}")
                
                if attachments:
                    message["attachments"] = attachments
            
            # Send email using application permissions (send as user requires different endpoint)
            endpoint = f"users/{from_email}/sendMail"
            request_body = {"message": message}
            
            await self._make_request("POST", endpoint, json_data=request_body)
            
            # Create success result
            result = NotificationResult(
                notification_id=notification.notification_id,
                success=True,
                message_id=None,  # Graph API doesn't return message ID for sendMail
                sent_at=datetime.now(timezone.utc),
                recipients_count=len(notification.recipients),
            )
            
            logger.info(f"Successfully sent email notification: {notification.notification_id}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to send email notification {notification.notification_id}: {e}")
            
            # Create failure result
            result = NotificationResult(
                notification_id=notification.notification_id,
                success=False,
                error_message=str(e),
                sent_at=None,
                recipients_count=len(notification.recipients),
            )
            return result
    
    async def create_draft_email(
        self,
        from_email: str,
        notification: EmailNotification,
    ) -> NotificationResult:
        """
        Create email draft via Microsoft Graph API.
        
        Args:
            from_email: Sender email address
            notification: Email notification object
            
        Returns:
            NotificationResult with draft creation status
        """
        try:
            # Prepare recipients (similar to send_email)
            to_recipients = []
            cc_recipients = []
            bcc_recipients = []
            
            for recipient in notification.recipients:
                recipient_obj = {
                    "emailAddress": {
                        "address": recipient.email,
                        "name": recipient.name or recipient.email,
                    }
                }
                
                if recipient.type == "to":
                    to_recipients.append(recipient_obj)
                elif recipient.type == "cc":
                    cc_recipients.append(recipient_obj)
                elif recipient.type == "bcc":
                    bcc_recipients.append(recipient_obj)
            
            # Prepare draft message
            draft_message = {
                "subject": notification.subject,
                "body": {
                    "contentType": "HTML" if "<html>" in notification.body.lower() else "Text",
                    "content": notification.body,
                },
                "toRecipients": to_recipients,
            }
            
            if cc_recipients:
                draft_message["ccRecipients"] = cc_recipients
            if bcc_recipients:
                draft_message["bccRecipients"] = bcc_recipients
            
            # Set importance
            if notification.priority.value == "urgent":
                draft_message["importance"] = "high"
            elif notification.priority.value == "high":
                draft_message["importance"] = "high"
            elif notification.priority.value == "low":
                draft_message["importance"] = "low"
            else:
                draft_message["importance"] = "normal"
            
            # Create draft
            endpoint = f"users/{from_email}/messages"
            response_data = await self._make_request("POST", endpoint, json_data=draft_message)
            
            draft_id = response_data.get("id")
            
            # Create success result
            result = NotificationResult(
                notification_id=notification.notification_id,
                success=True,
                message_id=draft_id,
                sent_at=datetime.now(timezone.utc),
                recipients_count=len(notification.recipients),
            )
            
            logger.info(f"Successfully created email draft: {notification.notification_id}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to create email draft {notification.notification_id}: {e}")
            
            # Create failure result
            result = NotificationResult(
                notification_id=notification.notification_id,
                success=False,
                error_message=str(e),
                sent_at=None,
                recipients_count=len(notification.recipients),
            )
            return result


# Convenience functions for use in agents
async def send_notification_email(
    notification: EmailNotification,
    client_id: str,
    client_secret: str,
    tenant_id: str,
    from_email: str,
) -> NotificationResult:
    """Send email notification via Outlook API."""
    client = OutlookAPIClient(client_id, client_secret, tenant_id)
    
    try:
        return await client.send_email(from_email, notification)
    except Exception as e:
        logger.error(f"Failed to send notification email: {e}")
        return NotificationResult(
            notification_id=notification.notification_id,
            success=False,
            error_message=str(e),
            sent_at=None,
            recipients_count=len(notification.recipients),
        )


async def create_notification_draft(
    notification: EmailNotification,
    client_id: str,
    client_secret: str,
    tenant_id: str,
    from_email: str,
) -> NotificationResult:
    """Create email draft via Outlook API."""
    client = OutlookAPIClient(client_id, client_secret, tenant_id)
    
    try:
        return await client.create_draft_email(from_email, notification)
    except Exception as e:
        logger.error(f"Failed to create notification draft: {e}")
        return NotificationResult(
            notification_id=notification.notification_id,
            success=False,
            error_message=str(e),
            sent_at=None,
            recipients_count=len(notification.recipients),
        )