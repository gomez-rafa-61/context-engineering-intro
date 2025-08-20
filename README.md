# Data Pipeline Monitoring Automation Framework

A comprehensive AI-powered monitoring system for data pipelines across multiple platforms using Pydantic AI multi-agent architecture.

## 🏗️ Architecture

This framework uses a multi-agent system to monitor data pipeline health across:
- **Airbyte** - Data connector sync jobs and streams
- **Databricks** - Job runs and cluster health
- **Power Automate** - Flow execution status  
- **Snowflake Tasks** - Task execution history

### Agent Architecture

```
🎯 Orchestrator Agent
├── 📊 Airbyte Agent → Airbyte API Tool
├── ⚙️ Databricks Agent → Databricks API Tool
├── 🔄 Power Automate Agent → Microsoft Graph Tool
├── ❄️ Snowflake Task Agent → Snowflake Task API Tool
├── 💾 Snowflake DB Agent → Database Operations Tool
└── 📧 Email Agent → Outlook API Tool
```

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Virtual environment (recommended)
- API access to monitored platforms
- Snowflake database for audit storage

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd audit-automation-framework
```

2. **Set up virtual environment**
```bash
python -m venv venv_linux
source venv_linux/bin/activate  # Linux/Mac
# or
venv_linux\Scripts\activate     # Windows
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure environment**
```bash
cp .env.example .env
# Edit .env with your API keys and configuration
```

### Configuration

#### Required Environment Variables

**LLM Configuration:**
```env
LLM_PROVIDER=openai
LLM_API_KEY=sk-your_openai_api_key_here
LLM_MODEL=gpt-4o-mini
```

**Platform API Keys:**
```env
# Airbyte (Choose one authentication method)
# Option 1: Static API Key (Legacy)
AIRBYTE_API_KEY=your_airbyte_access_token

# Option 2: OAuth2 Token Refresh (Recommended)
AIRBYTE_CLIENT_ID=your_airbyte_client_id
AIRBYTE_CLIENT_SECRET=your_airbyte_client_secret

# Common Airbyte Settings
AIRBYTE_WORKSPACE_ID=your_workspace_id

# Databricks  
DATABRICKS_API_KEY=your_databricks_token
DATABRICKS_BASE_URL=https://your-workspace.cloud.databricks.com

# Power Automate (Azure AD App)
POWER_AUTOMATE_CLIENT_ID=your_azure_app_client_id
POWER_AUTOMATE_CLIENT_SECRET=your_azure_app_secret
POWER_AUTOMATE_TENANT_ID=your_azure_tenant_id

# Snowflake
SNOWFLAKE_ACCOUNT=CURALEAF-CURAPROD.snowflakecomputing.com
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=DEV_POWERAPPS
SNOWFLAKE_SCHEMA=AUDIT_JOB_HUB

# Email Notifications (Azure AD App)
OUTLOOK_CLIENT_ID=your_outlook_client_id
OUTLOOK_CLIENT_SECRET=your_outlook_secret
OUTLOOK_TENANT_ID=your_tenant_id
```

## 🖥️ Usage

### Interactive CLI

Launch the interactive monitoring interface:

```bash
python cli.py
```

**Available Commands:**
- `monitor all` - Comprehensive monitoring across all platforms
- `monitor airbyte` - Airbyte-specific monitoring
- `health check` - Quick system health assessment
- `help` - Show command help
- `config` - Display current configuration

### Automated Monitoring

Run automated monitoring cycles:

```bash
# Full monitoring cycle
python main.py --mode full

# Health check only
python main.py --mode health

# Custom notifications
python main.py --emails admin@company.com ops@company.com --from-email monitor@company.com

# Save results to file
python main.py --output-file results.json
```

### GitHub Actions Integration

The framework includes automated GitHub Actions workflows:

**Scheduled Monitoring:**
- Runs every 15 minutes
- Full platform monitoring
- Automatic notifications on issues
- Results stored as artifacts

**Manual Execution:**
- Trigger via GitHub Actions UI
- Customizable email recipients
- Configurable monitoring mode

## 🔧 API Setup Instructions

### Airbyte Setup

**Option 1: OAuth2 Token Refresh (Recommended)**
1. Log into your Airbyte Cloud account
2. Go to Settings → Applications
3. Click "Create Application"
4. Note down the Client ID and Client Secret
5. Set `AIRBYTE_CLIENT_ID` and `AIRBYTE_CLIENT_SECRET`

**Option 2: Static API Key (Legacy)**
1. Log into your Airbyte instance
2. Go to Settings → Account → Applications  
3. Click "Create Access Token"
4. Copy token and set as `AIRBYTE_API_KEY`

**Note:** OAuth2 is recommended as it provides automatic token refresh and better security.

### Databricks Setup
1. Log into your Databricks workspace
2. User Settings → Access Tokens
3. Generate New Token
4. Copy token and set as `DATABRICKS_API_KEY`

### Power Automate Setup
1. Azure Portal → App registrations
2. New registration
3. API permissions: Microsoft Graph
   - `Flow.Read.All`
   - `Directory.Read.All`
4. Generate client secret

### Snowflake Setup
1. Create database: `DEV_POWERAPPS`
2. Create schema: `AUDIT_JOB_HUB`
3. Grant permissions for INSERT/UPDATE operations
4. Tables are auto-created on first run

### Outlook Setup
1. Azure Portal → App registrations
2. API permissions: Microsoft Graph
   - `Mail.Send`
   - `User.Read.All`
3. Generate client secret

## 📊 Data Storage

### Snowflake Schema

The framework automatically creates these tables in `DEV_POWERAPPS.AUDIT_JOB_HUB`:

**JOB_STATUS_RECORDS**
- Individual job execution records
- Platform-specific metadata
- Error messages and durations

**MONITORING_SESSIONS**
- Complete monitoring cycle results
- Overall health assessments
- Platform summaries

**PLATFORM_HEALTH_SUMMARIES**
- Per-platform health aggregations
- Success/failure rates
- Issue tracking

## 🔔 Notifications

### Email Templates

The system includes three notification templates:

1. **Critical Alert** - Multiple failures, system-wide issues
2. **Warning Alert** - Platform-specific failures
3. **Info Summary** - Regular status reports

### Notification Logic

- **CRITICAL**: >10 failed jobs or multiple platform failures
- **HIGH**: >5 failed jobs or single platform major issues  
- **NORMAL**: Minor issues, regular reports
- **LOW**: Informational updates only

## 🧪 Testing

### Unit Tests
```bash
# Run all tests
pytest tests/ -v

# Coverage report
pytest tests/ --cov=agents --cov=tools --cov-report=html
```

### Integration Tests
```bash
# Test with actual APIs (requires valid .env)
python main.py --mode health

# Test CLI interface
python cli.py

# Test Airbyte token refresh functionality
python examples/test_token_refresh.py
```

### Agent Testing with TestModel
```python
from pydantic_ai.models.test import TestModel
from agents.airbyte_agent import airbyte_agent

# Fast agent validation without API calls
test_model = TestModel()
with airbyte_agent.override(model=test_model):
    result = await airbyte_agent.run("Test monitoring")
```

## 🔍 Validation Commands

Before deployment, run these validation checks:

```bash
# Code quality
ruff check . --fix
mypy .

# Unit tests
pytest tests/ -v

# Manual system test
python main.py --mode health --output-file health.json
```

## 🏢 Project Structure

```
audit-automation-framework/
├── agents/                    # Pydantic AI agents
│   ├── orchestrator_agent.py # Main coordinator
│   ├── airbyte_agent.py      # Airbyte monitoring
│   ├── email_agent.py        # Notifications
│   └── dependencies.py       # Shared dependencies
├── tools/                     # Platform API integrations
│   ├── airbyte_api.py        # Airbyte client
│   ├── databricks_api.py     # Databricks client
│   ├── snowflake_db_api.py   # Database operations
│   └── outlook_api.py        # Email client
├── models/                    # Data models
│   ├── job_status.py         # Core job models
│   ├── platform_models.py    # Platform-specific models
│   └── notification_models.py # Email models
├── config/
│   └── settings.py           # Configuration management
├── .github/workflows/        # GitHub Actions
├── cli.py                    # Interactive interface
├── main.py                   # Automated execution
└── requirements.txt          # Dependencies
```

## 🔒 Security Best Practices

- Store all API keys in environment variables
- Use Azure AD app registrations with least-privilege permissions
- Enable MFA on all service accounts
- Rotate API keys regularly
- Monitor access logs for unusual activity
- Use Snowflake key-pair authentication in production

## 📈 Monitoring & Observability

### Logs
- Application logs: `monitoring.log`
- Structured logging with timestamps
- Error tracking and debugging information

### Metrics
- Job success/failure rates
- Platform availability percentages
- Response times and performance metrics
- Historical trend analysis

### Alerts
- Real-time failure notifications
- Escalation based on severity
- Integration with existing alerting systems

## 🛠️ Troubleshooting

### Common Issues

**Authentication Errors:**
- Verify API keys in .env file
- Check Azure AD app permissions
- Confirm service account access

**Connection Timeouts:**
- Increase timeout settings
- Check network connectivity
- Verify API endpoint URLs

**Missing Dependencies:**
- Run `pip install -r requirements.txt`
- Check Python version compatibility
- Verify virtual environment activation

### Debug Mode
```bash
# Enable debug logging
export DEBUG=true
export LOG_LEVEL=DEBUG
python main.py --mode health
```

## 📝 Contributing

1. Follow PEP8 coding standards
2. Add unit tests for new features
3. Update documentation
4. Use conventional commit messages
5. Test with all supported platforms

## 📜 License

This project is licensed under the MIT License. See LICENSE file for details.

---

## 🆘 Support

For support and questions:
- Review the troubleshooting guide
- Check application logs
- Verify API configurations
- Contact the data engineering team

**Monitoring Dashboard:** Access real-time status via the CLI interface  
**Documentation:** Complete API setup guides in `.env.example`  
**Status:** Production-ready multi-agent monitoring system

## 🔑 **No API Key Needed for Ollama + Mistral 7B!**

**Ollama is completely free and runs locally** - you don't need to get an API key from anywhere. Here's why:

### ✅ **Local Deployment = No API Keys Required**

- **Ollama runs on your own computer/server**
- **Mistral 7B is an open-source model** 
- **No external service to authenticate with**
- **No registration or signup required**

### 🚀 **Complete Setup Process**

```bash
# 1. Install Ollama (completely free)
curl -fsSL https://ollama.ai/install.sh | sh

# 2. Download Mistral 7B model (free, no account needed)
ollama pull mistral:7b

# 3. Start the local server
ollama serve
```

### 📝 **Your .env Configuration**

In your `.env` file, use:
```bash
LLM_PROVIDER=openai
LLM_API_KEY=dummy_key_not_needed    # Any text works - it's ignored
LLM_MODEL=mistral:7b
LLM_BASE_URL=http://localhost:11434/v1
```

The `LLM_API_KEY` can be **any text** because:
- Ollama runs locally on your machine
- No authentication is required for local access
- The framework expects an API key field, so we provide a dummy value

### 🌐 **Official Ollama Resources**

- **Download Ollama**: https://ollama.ai/download
- **Model Library**: https://ollama.ai/library
- **Mistral 7B Model**: https://ollama.ai/library/mistral
- **Documentation**: https://github.com/ollama/ollama

### 🔍 **How It Works**

1. **Ollama** = Local server that runs LLMs on your machine
2. **Mistral 7B** = Free, open-source language model  
3. **OpenAI-compatible API** = Your existing code works without changes
4. **No external dependencies** = Completely offline operation

### ✅ **Verification Steps**

After setup, verify it works:

```bash
# Test Ollama is running
curl http://localhost:11434/v1/models

# Test the model responds
curl http://localhost:11434/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "mistral:7b",
    "messages": [{"role": "user", "content": "Hello!"}]
  }'

# Test your framework
python cli.py
```

### 💡 **Key Benefits**

- ✅ **$0 cost** - No subscription fees
- ✅ **No API limits** - Unlimited requests  
- ✅ **Complete privacy** - Data never leaves your server
- ✅ **No registration** - No accounts or signups needed
- ✅ **Works offline** - No internet required after download

### 🆚 **Comparison with OpenAI**

| Aspect | OpenAI API | Ollama + Mistral 7B |
|--------|------------|---------------------|
| **API Key** | Required (from OpenAI) | Not needed (dummy text) |
| **Cost** | $0.50-$10/month | $0 (just electricity) |
| **Signup** | Required | Not required |
| **Privacy** | Data sent to OpenAI | Data stays local |
| **Internet** | Required | Only for initial download |

So to directly answer your question: **There is no URL to get an API key** because Ollama + Mistral 7B doesn't use API keys - it's completely free local software!

Just install Ollama, download the model, and use any dummy text as the API key in your configuration.