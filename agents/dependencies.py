"""
Shared dependency classes for all monitoring agents.
"""

from dataclasses import dataclass
from typing import Optional
from config.settings import settings


@dataclass
class AirbyteDependencies:
    """Dependencies for Airbyte monitoring agent."""
    api_key: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    base_url: str = "https://api.airbyte.com/v1"
    workspace_id: Optional[str] = None
    session_id: Optional[str] = None
    
    @classmethod
    def from_settings(cls, session_id: Optional[str] = None) -> "AirbyteDependencies":
        """Create dependencies from global settings."""
        return cls(
            api_key=settings.airbyte_api_key,
            client_id=getattr(settings, 'airbyte_client_id', None),
            client_secret=getattr(settings, 'airbyte_client_secret', None),
            base_url=settings.airbyte_base_url,
            workspace_id=settings.airbyte_workspace_id,
            session_id=session_id,
        )
    
    @classmethod
    def from_oauth(
        cls, 
        client_id: str, 
        client_secret: str, 
        workspace_id: Optional[str] = None,
        base_url: str = "https://api.airbyte.com/v1",
        session_id: Optional[str] = None
    ) -> "AirbyteDependencies":
        """Create dependencies for OAuth2 token refresh."""
        return cls(
            api_key=None,
            client_id=client_id,
            client_secret=client_secret,
            base_url=base_url,
            workspace_id=workspace_id,
            session_id=session_id,
        )


@dataclass
class DatabricksDependencies:
    """Dependencies for Databricks monitoring agent."""
    api_key: str
    base_url: str
    workspace_id: Optional[str] = None
    session_id: Optional[str] = None
    
    @classmethod
    def from_settings(cls, session_id: Optional[str] = None) -> "DatabricksDependencies":
        """Create dependencies from global settings."""
        return cls(
            api_key=settings.databricks_api_key,
            base_url=settings.databricks_base_url,
            workspace_id=settings.databricks_workspace_id,
            session_id=session_id,
        )


@dataclass
class PowerAutomateDependencies:
    """Dependencies for Power Automate monitoring agent."""
    client_id: str
    client_secret: str
    tenant_id: str
    base_url: str = "https://graph.microsoft.com/v1.0"
    session_id: Optional[str] = None
    
    @classmethod
    def from_settings(cls, session_id: Optional[str] = None) -> "PowerAutomateDependencies":
        """Create dependencies from global settings."""
        return cls(
            client_id=settings.power_automate_client_id,
            client_secret=settings.power_automate_client_secret,
            tenant_id=settings.power_automate_tenant_id,
            base_url=settings.power_automate_base_url,
            session_id=session_id,
        )


@dataclass
class SnowflakeTaskDependencies:
    """Dependencies for Snowflake Task monitoring agent."""
    account: str
    user: str
    password: str
    database: str = "DEV_POWERAPPS"
    schema: str = "AUDIT_JOB_HUB"
    warehouse: str = "COMPUTE_WH"
    role: Optional[str] = None
    session_id: Optional[str] = None
    
    @classmethod
    def from_settings(cls, session_id: Optional[str] = None) -> "SnowflakeTaskDependencies":
        """Create dependencies from global settings."""
        return cls(
            account=settings.snowflake_account,
            user=settings.snowflake_user,
            password=settings.snowflake_password,
            database=settings.snowflake_database,
            schema=settings.snowflake_schema,
            warehouse=settings.snowflake_warehouse,
            role=settings.snowflake_role,
            session_id=session_id,
        )


@dataclass
class SnowflakeDBDependencies:
    """Dependencies for Snowflake Database operations agent."""
    account: str
    user: str
    password: str
    database: str = "DEV_POWERAPPS"
    schema: str = "AUDIT_JOB_HUB"
    warehouse: str = "COMPUTE_WH"
    role: Optional[str] = None
    session_id: Optional[str] = None
    
    @classmethod
    def from_settings(cls, session_id: Optional[str] = None) -> "SnowflakeDBDependencies":
        """Create dependencies from global settings."""
        return cls(
            account=settings.snowflake_account,
            user=settings.snowflake_user,
            password=settings.snowflake_password,
            database=settings.snowflake_database,
            schema=settings.snowflake_schema,
            warehouse=settings.snowflake_warehouse,
            role=settings.snowflake_role,
            session_id=session_id,
        )


@dataclass
class EmailDependencies:
    """Dependencies for email notification agent."""
    client_id: str
    client_secret: str
    tenant_id: str
    from_email: Optional[str] = None
    session_id: Optional[str] = None
    
    @classmethod
    def from_settings(cls, from_email: Optional[str] = None, session_id: Optional[str] = None) -> "EmailDependencies":
        """Create dependencies from global settings."""
        return cls(
            client_id=settings.outlook_client_id,
            client_secret=settings.outlook_client_secret,
            tenant_id=settings.outlook_tenant_id,
            from_email=from_email,
            session_id=session_id,
        )


@dataclass
class OrchestratorDependencies:
    """Dependencies for the main orchestrator agent."""
    # Required Platform API configurations
    databricks_api_key: str
    databricks_base_url: str
    power_automate_client_id: str
    power_automate_client_secret: str
    power_automate_tenant_id: str
    snowflake_account: str
    snowflake_user: str
    snowflake_password: str
    snowflake_database: str
    snowflake_schema: str
    snowflake_warehouse: str
    outlook_client_id: str
    outlook_client_secret: str
    outlook_tenant_id: str
    
    # Optional configurations with defaults
    airbyte_api_key: Optional[str] = None
    airbyte_client_id: Optional[str] = None
    airbyte_client_secret: Optional[str] = None
    airbyte_base_url: str = "https://api.airbyte.com/v1"
    airbyte_workspace_id: Optional[str] = None
    databricks_workspace_id: Optional[str] = None
    snowflake_role: Optional[str] = None
    from_email: Optional[str] = None
    session_id: Optional[str] = None
    monitoring_id: Optional[str] = None
    
    @classmethod
    def from_settings(
        cls, 
        session_id: Optional[str] = None, 
        monitoring_id: Optional[str] = None,
        from_email: Optional[str] = None,
    ) -> "OrchestratorDependencies":
        """Create orchestrator dependencies from global settings."""
        return cls(
            # Airbyte
            airbyte_api_key=settings.airbyte_api_key,
            airbyte_client_id=getattr(settings, 'airbyte_client_id', None),
            airbyte_client_secret=getattr(settings, 'airbyte_client_secret', None),
            airbyte_base_url=settings.airbyte_base_url,
            airbyte_workspace_id=settings.airbyte_workspace_id,
            
            # Databricks
            databricks_api_key=settings.databricks_api_key,
            databricks_base_url=settings.databricks_base_url,
            databricks_workspace_id=settings.databricks_workspace_id,
            
            # Power Automate
            power_automate_client_id=settings.power_automate_client_id,
            power_automate_client_secret=settings.power_automate_client_secret,
            power_automate_tenant_id=settings.power_automate_tenant_id,
            
            # Snowflake
            snowflake_account=settings.snowflake_account,
            snowflake_user=settings.snowflake_user,
            snowflake_password=settings.snowflake_password,
            snowflake_database=settings.snowflake_database,
            snowflake_schema=settings.snowflake_schema,
            snowflake_warehouse=settings.snowflake_warehouse,
            snowflake_role=settings.snowflake_role,
            
            # Email
            outlook_client_id=settings.outlook_client_id,
            outlook_client_secret=settings.outlook_client_secret,
            outlook_tenant_id=settings.outlook_tenant_id,
            from_email=from_email,
            
            # Session
            session_id=session_id,
            monitoring_id=monitoring_id,
        )
    
    def get_airbyte_deps(self) -> AirbyteDependencies:
        """Get Airbyte dependencies."""
        return AirbyteDependencies(
            api_key=self.airbyte_api_key,
            client_id=self.airbyte_client_id,
            client_secret=self.airbyte_client_secret,
            base_url=self.airbyte_base_url,
            workspace_id=self.airbyte_workspace_id,
            session_id=self.session_id,
        )
    
    def get_databricks_deps(self) -> DatabricksDependencies:
        """Get Databricks dependencies."""
        return DatabricksDependencies(
            api_key=self.databricks_api_key,
            base_url=self.databricks_base_url,
            workspace_id=self.databricks_workspace_id,
            session_id=self.session_id,
        )
    
    def get_powerautomate_deps(self) -> PowerAutomateDependencies:
        """Get Power Automate dependencies."""
        return PowerAutomateDependencies(
            client_id=self.power_automate_client_id,
            client_secret=self.power_automate_client_secret,
            tenant_id=self.power_automate_tenant_id,
            session_id=self.session_id,
        )
    
    def get_snowflake_task_deps(self) -> SnowflakeTaskDependencies:
        """Get Snowflake Task dependencies."""
        return SnowflakeTaskDependencies(
            account=self.snowflake_account,
            user=self.snowflake_user,
            password=self.snowflake_password,
            database=self.snowflake_database,
            schema=self.snowflake_schema,
            warehouse=self.snowflake_warehouse,
            role=self.snowflake_role,
            session_id=self.session_id,
        )
    
    def get_snowflake_db_deps(self) -> SnowflakeDBDependencies:
        """Get Snowflake Database dependencies."""
        return SnowflakeDBDependencies(
            account=self.snowflake_account,
            user=self.snowflake_user,
            password=self.snowflake_password,
            database=self.snowflake_database,
            schema=self.snowflake_schema,
            warehouse=self.snowflake_warehouse,
            role=self.snowflake_role,
            session_id=self.session_id,
        )
    
    def get_email_deps(self) -> EmailDependencies:
        """Get Email dependencies."""
        return EmailDependencies(
            client_id=self.outlook_client_id,
            client_secret=self.outlook_client_secret,
            tenant_id=self.outlook_tenant_id,
            from_email=self.from_email,
            session_id=self.session_id,
        )