# Data Pipeline Monitoring Test Scripts

This directory contains comprehensive test scripts to validate each platform connection and the overall integration of the data pipeline monitoring framework.

## Overview

The test scripts are designed to:
- ✅ Validate API connections for each platform independently
- ✅ Test data retrieval and status conversion functionality  
- ✅ Verify error handling and authentication
- ✅ Test the complete end-to-end workflow
- ✅ Generate detailed test reports for debugging

## Test Scripts

### Individual Platform Tests

| Script | Platform | Description |
|--------|----------|-------------|
| `test_airbyte_connection.py` | Airbyte | Tests Airbyte API connection, job listing, and status retrieval |
| `test_databricks_connection.py` | Databricks | Tests Databricks Jobs API, run monitoring, and status conversion |
| `test_powerautomate_connection.py` | Power Automate | Tests Microsoft Graph API for Power Automate flows |
| `test_snowflake_task_connection.py` | Snowflake Tasks | Tests Snowflake Task API and execution history |
| `test_snowflake_db_connection.py` | Snowflake DB | Tests database operations and AUDIT_JOB_HUB schema |
| `test_outlook_connection.py` | Outlook/Email | Tests Microsoft Graph API for email notifications |

### Integration Tests

| Script | Description |
|--------|-------------|
| `test_all_platforms_integration.py` | Comprehensive test of all platforms and orchestrator agent |
| `run_all_tests.py` | Test runner that executes all tests with progress tracking |

## Prerequisites

### Environment Configuration

Create a `.env` file in the project root with the following variables:

```bash
# LLM Configuration
LLM_PROVIDER=openai
LLM_API_KEY=sk-your-openai-api-key
LLM_MODEL=gpt-4o-mini

# Airbyte Configuration
AIRBYTE_API_KEY=your_airbyte_api_key
AIRBYTE_WORKSPACE_ID=your_workspace_id
AIRBYTE_BASE_URL=https://api.airbyte.com/v1

# Databricks Configuration  
DATABRICKS_API_TOKEN=your_databricks_token
DATABRICKS_WORKSPACE_URL=https://your-workspace.cloud.databricks.com

# Power Automate Configuration
POWER_AUTOMATE_CLIENT_ID=your_azure_client_id
POWER_AUTOMATE_CLIENT_SECRET=your_azure_client_secret
POWER_AUTOMATE_TENANT_ID=your_azure_tenant_id
POWER_AUTOMATE_ENVIRONMENT_ID=your_environment_id

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your-account.snowflakecomputing.com
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=DEV_POWERAPPS
SNOWFLAKE_SCHEMA=AUDIT_JOB_HUB
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=SYSADMIN

# Outlook Configuration
OUTLOOK_CLIENT_ID=your_outlook_client_id
OUTLOOK_CLIENT_SECRET=your_outlook_client_secret
OUTLOOK_TENANT_ID=your_outlook_tenant_id
OUTLOOK_TEST_RECIPIENT=test@yourcompany.com
```

### API Key Setup Instructions

#### Airbyte
1. Go to Airbyte Cloud Settings → Account → Applications
2. Create a new application to get an API key
3. Set `AIRBYTE_API_KEY` in your `.env` file

#### Databricks
1. Go to Databricks workspace → User Settings → Access Tokens
2. Generate a new token
3. Set `DATABRICKS_API_TOKEN` and `DATABRICKS_WORKSPACE_URL`

#### Power Automate
1. Register an Azure AD application
2. Grant Microsoft Graph API permissions
3. Set client ID, secret, and tenant ID

#### Snowflake
1. Use existing Snowflake account credentials
2. Ensure user has permissions for DEV_POWERAPPS database

#### Outlook
1. Register an Azure AD application (can be same as Power Automate)
2. Grant Microsoft Graph Mail permissions
3. Set client ID, secret, and tenant ID

## Running Tests

### Individual Platform Tests

Test a specific platform:

```bash
# Test Airbyte connection
python test-scripts/test_airbyte_connection.py

# Test Databricks connection  
python test-scripts/test_databricks_connection.py

# Test Power Automate connection
python test-scripts/test_powerautomate_connection.py

# Test Snowflake Task connection
python test-scripts/test_snowflake_task_connection.py

# Test Snowflake Database operations
python test-scripts/test_snowflake_db_connection.py

# Test Outlook email functionality
python test-scripts/test_outlook_connection.py
```

### Comprehensive Integration Test

Run all tests together:

```bash
# Run comprehensive integration test
python test-scripts/test_all_platforms_integration.py

# Or use the test runner for organized output
python test-scripts/run_all_tests.py
```

## Test Output

Each test script provides:

- ✅ **Rich Console Output**: Color-coded status updates and progress
- 📊 **Summary Tables**: Test results organized in readable tables
- 💾 **JSON Reports**: Detailed test results saved to files
- 🔍 **Error Details**: Specific error messages for troubleshooting

### Sample Output

```
🚀 Airbyte API Connection Test Suite

Airbyte Test Configuration
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Setting       ┃ Value                                 ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ API Key       │ ********************...abc4          │
│ Workspace ID  │ 12345678-1234-1234-1234-123456789012 │
│ Base URL      │ https://api.airbyte.com/v1           │
└───────────────┴───────────────────────────────────────┘

🔍 Testing Airbyte API health and authentication...
✅ Airbyte API health check passed

Test Summary
┏━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Test             ┃ Status  ┃ Details                               ┃
┡━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ Health Check     │ ✅ PASS │ API authentication working           │
│ List Connections │ ✅ PASS │ 5 found                              │
│ List Jobs        │ ✅ PASS │ 23 found                             │
│ Status Conversion│ ✅ PASS │ 23 converted                         │
│ Error Handling   │ ✅ PASS │ Exceptions handled correctly         │
└──────────────────┴─────────┴───────────────────────────────────────┘

🎉 Airbyte API integration is working correctly!
```

## Test Results and Reports

### Generated Files

Each test creates timestamped result files:

- `airbyte_test_results_YYYYMMDD_HHMMSS.json`
- `databricks_test_results_YYYYMMDD_HHMMSS.json`
- `powerautomate_test_results_YYYYMMDD_HHMMSS.json`
- `snowflake_task_test_results_YYYYMMDD_HHMMSS.json`
- `snowflake_db_test_results_YYYYMMDD_HHMMSS.json`
- `outlook_test_results_YYYYMMDD_HHMMSS.json`
- `integration_test_report_YYYYMMDD_HHMMSS.json`

### Report Contents

JSON reports include:
- Configuration settings (credentials masked)
- Test execution timings
- Sample data from each platform
- Error messages and stack traces
- Platform-specific metrics

## Troubleshooting

### Common Issues

#### Authentication Errors
- Verify API keys are correct and not expired
- Check permissions for Azure AD applications
- Ensure Snowflake user has required database access

#### Network/Connection Issues
- Check firewall settings for API endpoints
- Verify proxy settings if applicable
- Test basic connectivity to platform APIs

#### Missing Data
- Some platforms may have no recent jobs/runs
- This is normal and tests will show "EMPTY" status
- Focus on connection health rather than data volume

### Debug Mode

Run tests with additional debug output:

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
python test-scripts/test_airbyte_connection.py
```

### Platform-Specific Notes

#### Airbyte
- Requires workspace ID for most operations
- API rate limits may affect large workspaces

#### Databricks  
- Workspace URL must include full domain
- Some job runs may be in archived state

#### Power Automate
- Environment ID is optional (uses default if not specified)
- Flow permissions may limit visibility

#### Snowflake
- Tasks may require SYSADMIN role or higher
- AUDIT_JOB_HUB schema is created if it doesn't exist

#### Outlook
- Email send tests require confirmation
- Draft creation doesn't send actual emails

## Next Steps

After successful platform tests:

1. ✅ **Run Integration Test**: Verify orchestrator agent works
2. ✅ **Test GitHub Actions**: Set up workflow secrets
3. ✅ **Configure Monitoring**: Set up scheduled execution
4. ✅ **Test Notifications**: Verify email alerts work correctly

## Support

For issues with specific platform tests:
1. Check the generated JSON report for detailed error information
2. Verify environment configuration
3. Test API connectivity manually
4. Review platform-specific documentation
