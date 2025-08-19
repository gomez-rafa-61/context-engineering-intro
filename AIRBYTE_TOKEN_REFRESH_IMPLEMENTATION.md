# Airbyte API Token Refresh Implementation

## Overview

This document describes the implementation of OAuth2 token refresh functionality for the Airbyte API integration in the audit automation framework. The implementation provides automatic token refresh while maintaining backward compatibility with static API keys.

## Changes Made

### 1. Enhanced AirbyteAPIClient (`tools/airbyte_api.py`)

**Key Features:**
- **Dual Authentication Support**: Supports both static API keys (legacy) and OAuth2 token refresh
- **Automatic Token Refresh**: Automatically refreshes tokens when they expire or become invalid
- **Retry Logic**: Retries API calls with fresh tokens on authentication failures
- **Backward Compatibility**: Existing code using static API keys continues to work

**New Methods:**
- `_refresh_token()`: Handles OAuth2 token refresh using client credentials
- `_get_headers()`: Returns headers with automatically refreshed tokens
- Enhanced `_make_request()`: Includes token refresh retry logic

**Constructor Updates:**
```python
AirbyteAPIClient(
    api_key: Optional[str] = None,           # Static API key (legacy)
    client_id: Optional[str] = None,         # OAuth2 client ID
    client_secret: Optional[str] = None,     # OAuth2 client secret
    base_url: str = "https://api.airbyte.com/v1",
    timeout: float = 30.0,
    max_retries: int = 3,
    retry_delay: float = 1.0,
)
```

### 2. Updated Dependencies (`agents/dependencies.py`)

**AirbyteDependencies Class:**
- Added `client_id` and `client_secret` fields for OAuth2
- Made `api_key` optional to support OAuth2-only configurations
- Added `from_oauth()` class method for OAuth2 credential creation

**OrchestratorDependencies Class:**
- Added OAuth2 fields for Airbyte configuration
- Reorganized field order to comply with dataclass requirements
- Updated getter methods to pass OAuth2 credentials

### 3. Configuration Updates (`config/settings.py`)

**New Environment Variables:**
```env
# OAuth2 Token Refresh (Recommended)
AIRBYTE_CLIENT_ID=your_airbyte_client_id_here
AIRBYTE_CLIENT_SECRET=your_airbyte_client_secret_here

# Static API Key (Legacy)
AIRBYTE_API_KEY=your_static_airbyte_api_key_here
```

**Validation:**
- Added `validate_airbyte_config()` method to ensure either API key or OAuth2 credentials are provided
- Updated field validators to handle optional API key

### 4. Agent Updates (`agents/airbyte_agent.py`)

**Tool Updates:**
- Updated `get_airbyte_jobs()` to pass OAuth2 credentials
- Updated `get_connection_health()` to support both authentication methods
- Maintained full backward compatibility

### 5. Convenience Function Updates

**Enhanced Functions:**
- `get_airbyte_job_status()`: Now accepts both authentication methods
- `get_airbyte_connection_health()`: Supports OAuth2 and static keys

## Usage Examples

### OAuth2 Configuration (Recommended)

```python
from tools.airbyte_api import AirbyteAPIClient

# Using OAuth2 credentials
client = AirbyteAPIClient(
    client_id="your_client_id",
    client_secret="your_client_secret"
)

# Tokens are automatically refreshed as needed
jobs = await client.get_jobs()
```

### Legacy API Key Configuration

```python
from tools.airbyte_api import AirbyteAPIClient

# Using static API key (legacy)
client = AirbyteAPIClient(
    api_key="your_static_api_key"
)

# Works exactly as before
jobs = await client.get_jobs()
```

### Using Dependencies

```python
from agents.dependencies import AirbyteDependencies

# OAuth2 dependencies
oauth_deps = AirbyteDependencies.from_oauth(
    client_id="your_client_id",
    client_secret="your_client_secret",
    workspace_id="your_workspace_id"
)

# Legacy dependencies
legacy_deps = AirbyteDependencies(
    api_key="your_api_key",
    workspace_id="your_workspace_id"
)
```

## Environment Setup

### Option 1: OAuth2 (Recommended)

1. **Create Airbyte Application:**
   - Log into Airbyte Cloud
   - Go to Settings → Applications
   - Click "Create Application"
   - Note the Client ID and Client Secret

2. **Set Environment Variables:**
   ```env
   AIRBYTE_CLIENT_ID=your_client_id
   AIRBYTE_CLIENT_SECRET=your_client_secret
   AIRBYTE_WORKSPACE_ID=your_workspace_id
   ```

### Option 2: Static API Key (Legacy)

1. **Generate API Key:**
   - Log into Airbyte
   - Go to Settings → Access Management
   - Generate new API key

2. **Set Environment Variables:**
   ```env
   AIRBYTE_API_KEY=your_api_key
   AIRBYTE_WORKSPACE_ID=your_workspace_id
   ```

## Benefits

### OAuth2 Token Refresh

✅ **Automatic Token Management**: No manual token rotation required  
✅ **Enhanced Security**: Tokens have shorter lifespans  
✅ **Better Error Handling**: Automatic retry on token expiration  
✅ **Production Ready**: Designed for long-running services  

### Backward Compatibility

✅ **Zero Breaking Changes**: Existing code continues to work  
✅ **Gradual Migration**: Can migrate from static keys over time  
✅ **Fallback Support**: Multiple authentication methods supported  

## Testing

### Unit Tests

```bash
# Test token refresh functionality
python examples/simple_token_test.py
```

### Integration Tests

```bash
# Test with real API credentials
python examples/test_token_refresh.py
```

### Validation

The implementation includes comprehensive error handling and validation:

- **Credential Validation**: Ensures required credentials are provided
- **Token Expiry Handling**: Automatic refresh before expiration
- **API Error Recovery**: Retry logic for transient failures
- **Fallback Mechanisms**: Graceful degradation on errors

## Migration Guide

### From Static API Keys to OAuth2

1. **Obtain OAuth2 Credentials** from Airbyte Cloud
2. **Update Environment Variables** to include client credentials
3. **Remove or Comment Out** static API key variables
4. **Test the Integration** using the provided test scripts
5. **Deploy with Confidence** - no code changes required

### Gradual Migration

The implementation supports gradual migration:

- Keep both authentication methods configured
- OAuth2 takes precedence when both are present
- Monitor logs for successful token refresh events
- Remove static API keys once OAuth2 is verified

## Monitoring and Observability

### Logging

The implementation includes detailed logging:

```
INFO:tools.airbyte_api:Successfully refreshed Airbyte API token
WARNING:tools.airbyte_api:Authentication failed, attempting token refresh
ERROR:tools.airbyte_api:Token refresh failed: [error details]
```

### Health Checks

Token refresh status is included in:
- API client health checks
- Platform monitoring reports
- Error tracking and alerting

## Security Considerations

### OAuth2 Best Practices

- **Client Secret Protection**: Store in secure environment variables
- **Token Lifecycle**: Tokens automatically expire and refresh
- **Audit Trail**: All authentication events are logged
- **Principle of Least Privilege**: Use minimal required permissions

### Production Deployment

- **Environment Separation**: Different credentials per environment
- **Secret Management**: Use proper secret management systems
- **Monitoring**: Track authentication success/failure rates
- **Rotation**: Regular client credential rotation

## Troubleshooting

### Common Issues

**"Invalid client credentials"**
- Verify CLIENT_ID and CLIENT_SECRET in environment
- Check that Airbyte application is properly configured

**"Token refresh failed"**
- Ensure network connectivity to api.airbyte.com
- Verify client application has proper permissions

**"Either api_key or both client_id and client_secret are required"**
- Set either AIRBYTE_API_KEY or both OAuth2 credentials
- Check environment variable loading

### Debug Mode

Enable debug logging for detailed troubleshooting:

```bash
export LOG_LEVEL=DEBUG
python examples/simple_token_test.py
```

## Conclusion

The OAuth2 token refresh implementation enhances the Airbyte integration with:

- **Improved Security**: Automatic token management
- **Better Reliability**: Retry logic and error handling  
- **Production Readiness**: Designed for long-running services
- **Zero Disruption**: Full backward compatibility maintained

This implementation follows industry best practices for OAuth2 integration while maintaining the simplicity and reliability of the existing system.
