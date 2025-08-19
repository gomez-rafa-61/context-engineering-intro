name: "Data Pipeline Monitoring Automation Framework"
description: |

## Purpose
Build a comprehensive Pydantic AI multi-agent automation framework to monitor data pipeline job status across multiple platforms (Airbyte, Databricks, Power Automate, Snowflake Task), register status in Snowflake database, and generate email notifications to support teams.

## Core Principles
1. **Context is King**: Include ALL necessary documentation, examples, and caveats
2. **Validation Loops**: Provide executable tests/lints the AI can run and fix
3. **Information Dense**: Use keywords and patterns from the codebase
4. **Progressive Success**: Start simple, validate, then enhance
5. **Global rules**: Be sure to follow all rules in CLAUDE.md

---

## Goal
Create a production-ready multi-agent automation framework that monitors data pipeline health across multiple platforms, automatically registers job status in Snowflake, and generates intelligent email notifications. The system should be schedulable via GitHub Actions and provide comprehensive monitoring for data engineering teams.

## Why
- **Business value**: Proactive monitoring prevents data pipeline failures from going unnoticed
- **Integration**: Centralized monitoring across heterogeneous data stack platforms
- **Problems solved**: Eliminates manual monitoring, reduces MTTR, provides audit trail for compliance

## What
A comprehensive automation framework with:
- **Multi-platform monitoring**: Airbyte, Databricks, Power Automate, Snowflake Task
- **Intelligent status evaluation**: AI-powered health assessment of pipeline jobs
- **Centralized storage**: All job status stored in Snowflake AUDIT_JOB_HUB schema
- **Smart notifications**: Context-aware email alerts to support teams
- **GitHub Actions integration**: Scheduled execution and CI/CD ready

## Example
In the `examples/` folder, is information on how to connect to AirByte and refresh API token for each API call 

- `examples/airbyte_status_checker.py` - use this as a to access AirByte API Token
- `examples/.env` - use to for setting AirByte environment variables

### Success Criteria
- [ ] Successfully monitor job status from all 4 data platforms
- [ ] Register job status data in Snowflake DEV_POWERAPPS.AUDIT_JOB_HUB schema
- [ ] Generate intelligent email notifications based on job health
- [ ] Orchestrator agent coordinates all monitoring activities
- [ ] GitHub Actions workflow executes monitoring on schedule
- [ ] All agents handle errors gracefully with retry mechanisms
- [ ] CLI interface provides manual execution and debugging capabilities

## All Needed Context

### Documentation & References
```yaml
# MUST READ - Include these in your context window
- url: https://ai.pydantic.dev/agents/
  why: Core agent creation patterns, dependency injection, system prompts
  
- url: https://ai.pydantic.dev/multi-agent-applications/
  why: Multi-agent system patterns, agent-as-tool delegation, usage tracking
  
- url: https://reference.airbyte.com/reference/getting-started
  why: Airbyte API authentication (access tokens), job monitoring endpoints (/listjobs, /getjob)
  critical: Access tokens created in Settings -> Account -> Applications
  
- url: https://docs.snowflake.com/en/developer-guide/sql-api/index
  why: Snowflake SQL API for INSERT/UPDATE operations, OAuth/Key Pair authentication
  
- file: use-cases/pydantic-ai/examples/main_agent_reference/research_agent.py
  why: Multi-agent delegation pattern, agent-as-tool with ctx.usage tracking
  
- file: use-cases/pydantic-ai/examples/main_agent_reference/settings.py
  why: Configuration management with pydantic-settings, API key validation
  
- file: use-cases/pydantic-ai/examples/main_agent_reference/tools.py
  why: HTTP client patterns with httpx, error handling, rate limiting
  
- file: use-cases/pydantic-ai/examples/main_agent_reference/cli.py
  why: Rich console interface, streaming, tool visibility, conversation history
  
- file: use-cases/pydantic-ai/examples/main_agent_reference/models.py
  why: Pydantic model patterns for data validation and structured outputs
```

### Current Codebase tree
```bash
C:\python-work\claude-project\audit-automation-framework\
├── CLAUDE.md                    # Global rules and conventions
├── INITIAL.md                   # Feature requirements  
├── README.md                    # Project documentation
├── PRPs/
│   ├── templates/
│   │   └── prp_base.md         # PRP template
│   └── EXAMPLE_multi_agent_prp.md
├── examples/                    # Code examples directory
└── use-cases/
    ├── pydantic-ai/
    │   ├── examples/
    │   │   └── main_agent_reference/  # EXCELLENT patterns to follow
    │   │       ├── research_agent.py  # Multi-agent delegation
    │   │       ├── tools.py           # HTTP tools with error handling
    │   │       ├── models.py          # Pydantic models
    │   │       ├── settings.py        # Configuration management
    │   │       └── cli.py             # Rich console interface
    │   └── CLAUDE.md              # PydanticAI specific rules
    └── mcp-server/               # MCP patterns (may be useful)
```

### Desired Codebase tree with files to be added
```bash
audit-automation-framework/
├── agents/
│   ├── __init__.py              # Package init
│   ├── orchestrator_agent.py   # Main coordinator agent
│   ├── airbyte_agent.py        # Airbyte monitoring agent
│   ├── databricks_agent.py     # Databricks monitoring agent  
│   ├── powerautomate_agent.py  # Power Automate monitoring agent
│   ├── snowflake_task_agent.py # Snowflake Task monitoring agent
│   ├── snowflake_db_agent.py   # Snowflake database operations agent
│   ├── email_agent.py          # Email notification agent
│   └── dependencies.py         # Shared dependencies and context
├── tools/
│   ├── __init__.py             # Package init
│   ├── airbyte_api.py          # Airbyte API integration
│   ├── databricks_api.py       # Databricks API integration
│   ├── powerautomate_api.py    # Power Automate API integration
│   ├── snowflake_task_api.py   # Snowflake Task API integration
│   ├── snowflake_db_api.py     # Snowflake database operations
│   └── outlook_api.py          # Outlook/Email API integration
├── models/
│   ├── __init__.py             # Package init
│   ├── job_status.py           # Job status data models
│   ├── platform_models.py      # Platform-specific models
│   └── notification_models.py  # Email notification models
├── config/
│   ├── __init__.py             # Package init
│   └── settings.py             # Environment and config management
├── tests/
│   ├── __init__.py             # Package init
│   ├── test_orchestrator.py    # Orchestrator agent tests
│   ├── test_platform_agents.py # Platform agent tests
│   ├── test_tools/             # Tool-specific tests
│   │   ├── test_airbyte_api.py
│   │   ├── test_databricks_api.py
│   │   ├── test_powerautomate_api.py
│   │   ├── test_snowflake_apis.py
│   │   └── test_outlook_api.py
│   └── test_cli.py             # CLI tests
├── .github/
│   └── workflows/
│       └── pipeline-monitoring.yml  # GitHub Actions workflow
├── cli.py                      # CLI interface for manual execution
├── main.py                     # Main entry point for automation
├── .env.example                # Environment variables template
├── requirements.txt            # Updated dependencies
├── README.md                   # Comprehensive setup documentation
└── venv_linux/                 # Virtual environment (existing)
```

### Known Gotchas & Library Quirks
```python
# CRITICAL: Pydantic AI requires async throughout - no sync functions in async context
# CRITICAL: Airbyte API requires access tokens created in UI: Settings -> Account -> Applications  
# CRITICAL: Snowflake SQL API supports OAuth/Key Pair - recommend Key Pair for automation
# CRITICAL: Databricks API has rate limits - implement exponential backoff
# CRITICAL: Power Automate API requires specific Azure AD app permissions
# CRITICAL: Agent-as-tool pattern requires passing ctx.usage for token tracking
# CRITICAL: Always use absolute imports for cleaner code
# CRITICAL: Store sensitive credentials in .env, never commit them
# CRITICAL: GitHub Actions needs secrets for all API keys and credentials
# CRITICAL: Use python_dotenv and load_dotenv() consistently like main_agent_reference
# CRITICAL: Rich console patterns from cli.py for proper formatting and tool visibility
# CRITICAL: HTTP clients need timeout and retry logic - follow tools.py patterns
```

## Implementation Blueprint

### Data models and structure

```python
# models/job_status.py - Core job status models
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

class JobStatus(str, Enum):
    RUNNING = "running"
    SUCCESS = "success" 
    FAILED = "failed"
    CANCELLED = "cancelled"
    PENDING = "pending"
    UNKNOWN = "unknown"

class PlatformType(str, Enum):
    AIRBYTE = "airbyte"
    DATABRICKS = "databricks"
    POWER_AUTOMATE = "power_automate"
    SNOWFLAKE_TASK = "snowflake_task"

class JobStatusRecord(BaseModel):
    """Core job status record for Snowflake storage."""
    job_id: str = Field(..., description="Platform-specific job identifier")
    platform: PlatformType = Field(..., description="Data platform type")
    job_name: str = Field(..., description="Human-readable job name")
    status: JobStatus = Field(..., description="Current job status")
    last_run_time: Optional[datetime] = Field(None, description="Last execution time")
    duration_seconds: Optional[int] = Field(None, description="Job duration in seconds")
    error_message: Optional[str] = Field(None, description="Error details if failed")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Platform-specific metadata")
    checked_at: datetime = Field(default_factory=datetime.utcnow, description="Status check timestamp")
    
class HealthAssessment(BaseModel):
    """AI-generated health assessment of job status."""
    overall_health: str = Field(..., description="Overall health assessment")
    risk_level: str = Field(..., description="Risk level: LOW, MEDIUM, HIGH, CRITICAL")
    recommendations: List[str] = Field(default_factory=list, description="Recommended actions")
    requires_notification: bool = Field(False, description="Whether to send notification")
    notification_priority: str = Field("normal", description="Priority: low, normal, high, urgent")
```

### List of tasks to be completed to fulfill the PRP in the order they should be completed

```yaml
Task 1: Setup Configuration and Environment
CREATE config/settings.py:
  - PATTERN: Follow use-cases/pydantic-ai/examples/main_agent_reference/settings.py exactly
  - Use pydantic-settings with BaseSettings
  - Add fields for all platform API keys (Airbyte, Databricks, Power Automate, Snowflake)
  - Add Snowflake connection details (instance, database, schema)
  - Use field_validator for API key validation
  - Include LLM configuration following existing pattern

CREATE .env.example:
  - Include all required environment variables with descriptions
  - Follow pattern from main_agent_reference examples
  - Add comments explaining where to get each API key

Task 2: Implement Core Data Models
CREATE models/job_status.py:
  - PATTERN: Follow use-cases/pydantic-ai/examples/main_agent_reference/models.py structure
  - Implement JobStatusRecord, HealthAssessment, PlatformType enums
  - Add proper Pydantic validation and field descriptions
  - Include example configurations for all models

CREATE models/platform_models.py:
  - Create platform-specific models for API responses
  - Airbyte connection/stream models
  - Databricks job run models  
  - Power Automate flow run models
  - Snowflake task history models

Task 3: Implement Platform API Tools
CREATE tools/airbyte_api.py:
  - PATTERN: Follow use-cases/pydantic-ai/examples/main_agent_reference/tools.py HTTP patterns
  - Use httpx.AsyncClient with timeout and retry logic
  - Implement get_connections(), get_job_status(), list_jobs() functions
  - Handle rate limiting and authentication errors
  - Return structured JobStatusRecord models

CREATE tools/databricks_api.py:
  - Similar pattern to airbyte_api.py
  - Implement Databricks Jobs API calls
  - Handle authentication with tokens
  - Parse job run status and metadata

CREATE tools/powerautomate_api.py:
  - Microsoft Graph API integration
  - Handle Azure AD authentication
  - Flow run status monitoring

CREATE tools/snowflake_task_api.py:
  - Snowflake Task history monitoring
  - SQL API integration for task status

CREATE tools/snowflake_db_api.py:
  - Database operations for storing job status
  - INSERT/UPDATE operations for AUDIT_JOB_HUB schema
  - Connection management and error handling

CREATE tools/outlook_api.py:
  - Email draft creation functionality
  - Microsoft Graph API for Outlook
  - Template-based email generation

Task 4: Create Platform Monitoring Agents
CREATE agents/airbyte_agent.py:
  - PATTERN: Follow use-cases/pydantic-ai/examples/main_agent_reference/research_agent.py structure
  - Use Agent with deps_type pattern
  - Register airbyte_api tools with @agent.tool
  - Return structured job status data
  - Include health assessment logic

CREATE agents/databricks_agent.py:
  - Similar pattern to airbyte_agent.py
  - Databricks-specific monitoring logic
  - Job run status evaluation

CREATE agents/powerautomate_agent.py:
  - Power Automate flow monitoring
  - Azure integration patterns

CREATE agents/snowflake_task_agent.py:
  - Snowflake Task monitoring
  - Task execution history analysis

CREATE agents/snowflake_db_agent.py:
  - Database operations agent
  - Status storage and retrieval
  - Register tools for INSERT/UPDATE operations

CREATE agents/email_agent.py:
  - PATTERN: Follow existing email agent pattern from main_agent_reference
  - Email draft generation based on job status
  - Template-based notification creation
  - Priority-based messaging

Task 5: Create Main Orchestrator Agent
CREATE agents/orchestrator_agent.py:
  - PATTERN: Multi-agent delegation from use-cases/pydantic-ai/examples/main_agent_reference/research_agent.py
  - Use agent-as-tool pattern for all platform agents
  - Coordinate monitoring workflow across all platforms
  - Aggregate health assessments
  - Trigger notifications based on assessment results
  - Pass ctx.usage between agent calls for token tracking

CREATE agents/dependencies.py:
  - Shared dependency classes for all agents
  - Configuration injection
  - Session management

Task 6: Implement CLI Interface
CREATE cli.py:
  - PATTERN: Follow use-cases/pydantic-ai/examples/main_agent_reference/cli.py exactly
  - Rich console with streaming and tool visibility
  - Support manual monitoring execution
  - Display job status summaries
  - Interactive debugging capabilities

CREATE main.py:
  - Entry point for automated execution
  - Orchestrate full monitoring workflow
  - Generate status reports
  - Handle GitHub Actions integration

Task 7: Add GitHub Actions Workflow
CREATE .github/workflows/pipeline-monitoring.yml:
  - Scheduled execution (e.g., every 15 minutes)
  - Environment secrets for API keys
  - Python setup and dependency installation
  - Execute main.py monitoring workflow
  - Artifact upload for status reports

Task 8: Add Comprehensive Tests
CREATE tests/:
  - PATTERN: Follow existing test patterns from examples
  - Mock external API calls
  - Test agent interactions and tool responses
  - Test happy path, edge cases, errors
  - Ensure 80%+ coverage
  - Use TestModel/FunctionModel for agent testing

Task 9: Create Documentation
UPDATE README.md:
  - Comprehensive setup instructions
  - API key configuration guide
  - GitHub Actions setup
  - Architecture overview with agent diagram
  - Troubleshooting guide
```

### Per task pseudocode

```python
# Task 3: Airbyte API Tool Example
async def get_airbyte_jobs(api_key: str, workspace_id: str) -> List[JobStatusRecord]:
    """Get job status from Airbyte API."""
    # PATTERN: Use httpx like main_agent_reference/tools.py
    async with httpx.AsyncClient() as client:
        headers = {"Authorization": f"Bearer {api_key}"}
        
        # GOTCHA: Airbyte API requires workspace context
        response = await client.get(
            f"https://api.airbyte.com/v1/jobs",
            headers=headers,
            params={"workspaceId": workspace_id},
            timeout=30.0  # CRITICAL: Set timeout to avoid hanging
        )
        
        # PATTERN: Structured error handling like tools.py
        if response.status_code == 401:
            raise AirbyteAPIError("Invalid API key")
        if response.status_code == 429:
            raise AirbyteAPIError("Rate limit exceeded")
        if response.status_code != 200:
            raise AirbyteAPIError(f"API returned {response.status_code}")
        
        # Parse and validate with Pydantic
        data = response.json()
        job_records = []
        for job in data.get("data", []):
            record = JobStatusRecord(
                job_id=job["jobId"],
                platform=PlatformType.AIRBYTE,
                job_name=job.get("configName", "Unknown"),
                status=map_airbyte_status(job["status"]),
                last_run_time=parse_datetime(job.get("createdAt")),
                metadata={"connection_id": job.get("configId")}
            )
            job_records.append(record)
        
        return job_records

# Task 5: Orchestrator Agent with Multi-Agent Delegation
@orchestrator_agent.tool
async def monitor_all_platforms(
    ctx: RunContext[OrchestratorDependencies]
) -> Dict[str, Any]:
    """Monitor all data platforms and assess overall health."""
    platform_results = {}
    
    # CRITICAL: Pass usage for token tracking like research_agent.py
    # Monitor Airbyte
    airbyte_result = await airbyte_agent.run(
        "Check all Airbyte connections and streams status",
        deps=AirbyteDependencies(api_key=ctx.deps.airbyte_api_key),
        usage=ctx.usage  # PATTERN from multi-agent docs
    )
    platform_results["airbyte"] = airbyte_result.data
    
    # Monitor Databricks
    databricks_result = await databricks_agent.run(
        "Check all Databricks job runs status",
        deps=DatabricksDependencies(api_key=ctx.deps.databricks_api_key),
        usage=ctx.usage
    )
    platform_results["databricks"] = databricks_result.data
    
    # Continue for other platforms...
    
    # Aggregate health assessment
    overall_health = assess_overall_health(platform_results)
    
    # Store results in Snowflake
    await snowflake_db_agent.run(
        f"Store job status records: {platform_results}",
        deps=SnowflakeDependencies(connection_params=ctx.deps.snowflake_config),
        usage=ctx.usage
    )
    
    # Send notifications if needed
    if overall_health.requires_notification:
        await email_agent.run(
            f"Create notification email for health status: {overall_health}",
            deps=EmailDependencies(outlook_credentials=ctx.deps.outlook_config),
            usage=ctx.usage
        )
    
    return {
        "platform_results": platform_results,
        "overall_health": overall_health,
        "timestamp": datetime.utcnow().isoformat()
    }
```

### Integration Points
```yaml
ENVIRONMENT:
  - add to: .env
  - vars: |
      # LLM Configuration
      LLM_PROVIDER=openai
      LLM_API_KEY=sk-...
      LLM_MODEL=gpt-4
      
      # Platform API Keys
      AIRBYTE_API_KEY=your_token_here
      DATABRICKS_API_KEY=your_token_here
      POWER_AUTOMATE_CLIENT_ID=your_client_id
      POWER_AUTOMATE_CLIENT_SECRET=your_secret
      
      # Snowflake Configuration
      SNOWFLAKE_ACCOUNT=CURALEAF-CURAPROD.snowflakecomputing.com
      SNOWFLAKE_USER=your_user
      SNOWFLAKE_PASSWORD=your_password
      SNOWFLAKE_DATABASE=DEV_POWERAPPS
      SNOWFLAKE_SCHEMA=AUDIT_JOB_HUB
      
      # Email Configuration
      OUTLOOK_CLIENT_ID=your_outlook_client_id
      OUTLOOK_CLIENT_SECRET=your_outlook_secret

GITHUB_ACTIONS:
  - secrets: All API keys and credentials as repository secrets
  - schedule: "*/15 * * * *"  # Every 15 minutes
  - workflow: Execute main.py for full monitoring cycle
  
DEPENDENCIES:
  - Update requirements.txt with:
    - pydantic-ai[all]
    - httpx
    - rich
    - python-dotenv
    - pydantic-settings
    - snowflake-sqlalchemy
    - azure-identity
    - msal
```

## Validation Loop

### Level 1: Syntax & Style
```bash
# Run these FIRST - fix any errors before proceeding
ruff check . --fix              # Auto-fix style issues
mypy .                          # Type checking

# Expected: No errors. If errors, READ and fix.
```

### Level 2: Unit Tests
```python
# test_orchestrator_agent.py
async def test_orchestrator_monitors_all_platforms():
    """Test orchestrator coordinates all platform monitoring"""
    # Use TestModel for fast validation
    test_model = TestModel()
    
    # Override orchestrator agent with test model
    with orchestrator_agent.override(model=test_model):
        deps = OrchestratorDependencies(
            airbyte_api_key="test_key",
            databricks_api_key="test_key",
            # ... other test dependencies
        )
        
        result = await orchestrator_agent.run(
            "Monitor all data platforms",
            deps=deps
        )
        
        assert result.data is not None
        assert "platform_results" in result.data
        assert "overall_health" in result.data

# test_airbyte_agent.py
async def test_airbyte_monitoring_success():
    """Test Airbyte agent retrieves job status correctly"""
    # Mock HTTP responses
    with patch('httpx.AsyncClient.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{
                "jobId": "123",
                "status": "succeeded", 
                "configName": "Test Connection",
                "createdAt": "2024-01-01T00:00:00Z"
            }]
        }
        mock_get.return_value = mock_response
        
        agent = create_airbyte_agent()
        result = await agent.run("Check Airbyte job status")
        
        assert result.data
        assert len(result.data) > 0
        assert result.data[0]["platform"] == "airbyte"

async def test_airbyte_api_error_handling():
    """Test Airbyte API error handling"""
    with patch('httpx.AsyncClient.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response
        
        agent = create_airbyte_agent()
        result = await agent.run("Check Airbyte job status")
        
        # Should handle error gracefully
        assert "error" in result.data or "failed" in str(result.data).lower()
```

```bash
# Run tests iteratively until passing:
uv run pytest tests/ -v --cov=agents --cov=tools --cov-report=term-missing

# If failing: Debug specific test, fix code, re-run
```

### Level 3: Integration Test
```bash
# Test CLI interaction
python cli.py

# Expected interaction:
# 🤖 Data Pipeline Monitor
# You: monitor all platforms
# 🔹 Calling tool: monitor_all_platforms
# ✅ Tool result: Successfully monitored 4 platforms
# 🔹 Calling tool: store_status_records  
# ✅ Tool result: Stored 23 job records in Snowflake
# 🔹 Calling tool: assess_health
# ✅ Tool result: Overall health: GOOD, 2 warnings detected
# Assistant: Monitoring complete. All platforms healthy with 2 minor warnings.

# Test GitHub Actions locally
act -j pipeline-monitoring

# Expected: Workflow runs successfully, status report generated
```

## Final Validation Checklist
- [ ] All platform agents retrieve job status: `pytest tests/test_platform_agents.py -v`
- [ ] Orchestrator coordinates all agents: `pytest tests/test_orchestrator.py -v`
- [ ] Snowflake database operations work: `pytest tests/test_snowflake_db.py -v`
- [ ] Email notifications generate correctly: `pytest tests/test_email_agent.py -v`
- [ ] No linting errors: `ruff check .`
- [ ] No type errors: `mypy .`
- [ ] CLI provides proper tool visibility: `python cli.py`
- [ ] GitHub Actions workflow executes: Manual workflow test
- [ ] All API integrations handle errors gracefully
- [ ] Environment configuration validated
- [ ] Documentation includes clear setup instructions

---

## Anti-Patterns to Avoid
- ❌ Don't hardcode API keys - use environment variables with settings.py pattern
- ❌ Don't use sync functions in async agent context
- ❌ Don't skip agent-as-tool usage tracking (ctx.usage)
- ❌ Don't ignore rate limits for external APIs
- ❌ Don't forget to use TestModel for agent validation
- ❌ Don't commit credentials or .env files
- ❌ Don't skip error handling in HTTP clients
- ❌ Don't create agents without proper dependency injection
- ❌ Don't forget retry mechanisms for external API calls
- ❌ Don't skip GitHub Actions secrets configuration

## Confidence Score: 9/10

High confidence due to:
- Excellent existing patterns in use-cases/pydantic-ai examples to follow
- Well-documented external APIs (Airbyte, Snowflake, Pydantic AI)
- Established multi-agent patterns and validation approaches
- Clear implementation path with executable validation gates
- Comprehensive error handling and testing strategies

Minor uncertainty on:
- Databricks and Power Automate API specifics (need additional research during implementation)
- Exact Snowflake schema requirements (may need clarification during development)
- GitHub Actions integration specifics (straightforward but needs testing)