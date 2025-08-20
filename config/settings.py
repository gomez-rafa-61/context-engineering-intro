"""
Configuration management using pydantic-settings for data pipeline monitoring.
"""

import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field, field_validator, ConfigDict
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Settings(BaseSettings):
    """Application settings with environment variable support."""
    
    model_config = ConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )
    
    # LLM Configuration
    llm_provider: str = Field(default="openai")
    llm_api_key: str = Field(...)
    llm_model: str = Field(default="gpt-4o-mini")
    llm_base_url: Optional[str] = Field(default="https://api.openai.com/v1")
    
    # Airbyte Configuration
    airbyte_api_key: Optional[str] = Field(None, description="Static API key (legacy)")
    airbyte_client_id: Optional[str] = Field(None, description="OAuth2 Client ID")
    airbyte_client_secret: Optional[str] = Field(None, description="OAuth2 Client Secret")
    airbyte_base_url: str = Field(
        default="https://api.airbyte.com/v1"
    )
    airbyte_workspace_id: Optional[str] = Field(None)
    
    # Databricks Configuration
    databricks_api_key: str = Field(...)
    databricks_base_url: str = Field(...)  # e.g., https://your-workspace.cloud.databricks.com
    databricks_workspace_id: Optional[str] = Field(None)
    
    # Power Automate Configuration
    power_automate_client_id: str = Field(...)
    power_automate_client_secret: str = Field(...)
    power_automate_tenant_id: str = Field(...)
    power_automate_base_url: str = Field(
        default="https://graph.microsoft.com/v1.0"
    )
    
    # Snowflake Configuration
    snowflake_account: str = Field(...)  # e.g., CURALEAF-CURAPROD.snowflakecomputing.com
    snowflake_user: str = Field(...)
    snowflake_password: str = Field(...)
    snowflake_database: str = Field(default="DEV_POWERAPPS")
    snowflake_schema: str = Field(default="AUDIT_JOB_HUB")
    snowflake_warehouse: str = Field(default="COMPUTE_WH")
    snowflake_role: Optional[str] = Field(None)
    
    # Email Configuration (Outlook/Microsoft Graph)
    outlook_client_id: str = Field(...)
    outlook_client_secret: str = Field(...)
    outlook_tenant_id: str = Field(...)
    
    # Application Configuration
    app_env: str = Field(default="development")
    log_level: str = Field(default="INFO")
    debug: bool = Field(default=False)
    
    # Monitoring Configuration
    monitoring_interval_minutes: int = Field(default=15)
    health_check_timeout_seconds: int = Field(default=30)
    max_retries: int = Field(default=3)
    retry_delay_seconds: int = Field(default=5)
    
    @field_validator("llm_api_key", "databricks_api_key")
    @classmethod
    def validate_required_api_keys(cls, v):
        """Ensure required API keys are not empty."""
        if not v or v.strip() == "":
            raise ValueError("API key cannot be empty")
        return v
    
    def validate_airbyte_config(self) -> None:
        """Validate that either API key or OAuth2 credentials are provided for Airbyte."""
        if not self.airbyte_api_key and not (self.airbyte_client_id and self.airbyte_client_secret):
            raise ValueError("Either airbyte_api_key or both airbyte_client_id and airbyte_client_secret must be provided")
        return self
    
    @field_validator("power_automate_client_id", "power_automate_client_secret", "power_automate_tenant_id")
    @classmethod
    def validate_power_automate_config(cls, v):
        """Ensure Power Automate configuration is not empty."""
        if not v or v.strip() == "":
            raise ValueError("Power Automate configuration cannot be empty")
        return v
    
    @field_validator("snowflake_account", "snowflake_user", "snowflake_password")
    @classmethod
    def validate_snowflake_config(cls, v):
        """Ensure Snowflake configuration is not empty."""
        if not v or v.strip() == "":
            raise ValueError("Snowflake configuration cannot be empty")
        return v
    
    @field_validator("outlook_client_id", "outlook_client_secret", "outlook_tenant_id")
    @classmethod
    def validate_outlook_config(cls, v):
        """Ensure Outlook configuration is not empty."""
        if not v or v.strip() == "":
            raise ValueError("Outlook configuration cannot be empty")
        return v
    
    @field_validator("monitoring_interval_minutes")
    @classmethod
    def validate_monitoring_interval(cls, v):
        """Ensure monitoring interval is reasonable."""
        if v < 1 or v > 1440:  # 1 minute to 24 hours
            raise ValueError("Monitoring interval must be between 1 and 1440 minutes")
        return v


# Global settings instance
try:
    settings = Settings()
except Exception:
    # For testing, create settings with dummy values
    import os
    os.environ.setdefault("LLM_API_KEY", "test_key")
    os.environ.setdefault("AIRBYTE_API_KEY", "test_key")
    os.environ.setdefault("AIRBYTE_CLIENT_ID", "test_client_id")
    os.environ.setdefault("AIRBYTE_CLIENT_SECRET", "test_client_secret")
    os.environ.setdefault("DATABRICKS_API_KEY", "test_key")
    os.environ.setdefault("DATABRICKS_BASE_URL", "https://test.databricks.com")
    os.environ.setdefault("POWER_AUTOMATE_CLIENT_ID", "test_client_id")
    os.environ.setdefault("POWER_AUTOMATE_CLIENT_SECRET", "test_client_secret")
    os.environ.setdefault("POWER_AUTOMATE_TENANT_ID", "test_tenant_id")
    os.environ.setdefault("SNOWFLAKE_ACCOUNT", "test-account.snowflakecomputing.com")
    os.environ.setdefault("SNOWFLAKE_USER", "test_user")
    os.environ.setdefault("SNOWFLAKE_PASSWORD", "test_password")
    os.environ.setdefault("OUTLOOK_CLIENT_ID", "test_client_id")
    os.environ.setdefault("OUTLOOK_CLIENT_SECRET", "test_client_secret")
    os.environ.setdefault("OUTLOOK_TENANT_ID", "test_tenant_id")
    settings = Settings()