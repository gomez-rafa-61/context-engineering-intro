"""
Snowflake Task API integration tools for task monitoring.
"""

import asyncio
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import snowflake.connector

from models.job_status import JobStatusRecord, PlatformType
from models.platform_models import (
    SnowflakeTaskHistory,
    SnowflakeTaskInfo,
    map_snowflake_task_status,
)

logger = logging.getLogger(__name__)


class SnowflakeTaskAPIError(Exception):
    """Custom exception for Snowflake Task API errors."""
    pass


class SnowflakeTaskAPIClient:
    """Snowflake Task API client with connection pooling and error handling."""
    
    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        database: str = "DEV_POWERAPPS",
        schema: str = "AUDIT_JOB_HUB",
        warehouse: str = "COMPUTE_WH",
        role: Optional[str] = None,
    ):
        """
        Initialize Snowflake Task API client.
        
        Args:
            account: Snowflake account identifier
            user: Snowflake username
            password: Snowflake password
            database: Database name
            schema: Schema name
            warehouse: Warehouse name
            role: Optional role name
        """
        self.account = account
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.role = role
        self.connection = None
    
    async def _get_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Get or create Snowflake connection."""
        if self.connection is None or self.connection.is_closed():
            try:
                connection_params = {
                    "account": self.account,
                    "user": self.user,
                    "password": self.password,
                    "database": self.database,
                    "schema": self.schema,
                    "warehouse": self.warehouse,
                }
                
                if self.role:
                    connection_params["role"] = self.role
                
                # Run connection in thread pool since it's not async
                loop = asyncio.get_event_loop()
                self.connection = await loop.run_in_executor(
                    None, lambda: snowflake.connector.connect(**connection_params)
                )
                
                logger.info("Connected to Snowflake successfully")
                
            except Exception as e:
                logger.error(f"Failed to connect to Snowflake: {e}")
                raise SnowflakeTaskAPIError(f"Connection failed: {str(e)}")
        
        return self.connection
    
    async def _execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute SQL query and return results."""
        connection = await self._get_connection()
        
        try:
            loop = asyncio.get_event_loop()
            
            def _run_query():
                cursor = connection.cursor()
                try:
                    cursor.execute(query, params or {})
                    
                    # Get column names
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    
                    # Fetch all results
                    rows = cursor.fetchall()
                    
                    # Convert to list of dictionaries
                    results = []
                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        results.append(row_dict)
                    
                    return results
                    
                finally:
                    cursor.close()
            
            results = await loop.run_in_executor(None, _run_query)
            logger.debug(f"Query executed successfully, returned {len(results)} rows")
            return results
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise SnowflakeTaskAPIError(f"Query failed: {str(e)}")
    
    async def get_task_history(
        self,
        task_name: Optional[str] = None,
        limit: int = 100,
        hours_back: int = 24,
    ) -> List[SnowflakeTaskHistory]:
        """
        Get task execution history.
        
        Args:
            task_name: Optional specific task name
            limit: Maximum number of records
            hours_back: How many hours back to look
            
        Returns:
            List of SnowflakeTaskHistory objects
        """
        # Base query for task history
        query = """
        SELECT 
            NAME,
            DATABASE_NAME,
            SCHEMA_NAME,
            STATE,
            SCHEDULED_TIME,
            STARTED_TIME,
            COMPLETED_TIME,
            ROOT_TASK_ID,
            GRAPH_RUN_ID,
            RUN_ID,
            ERROR_CODE,
            ERROR_MESSAGE
        FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
        WHERE SCHEDULED_TIME >= DATEADD(hour, -%(hours_back)s, CURRENT_TIMESTAMP())
        """
        
        params = {"hours_back": hours_back}
        
        if task_name:
            query += " AND NAME = %(task_name)s"
            params["task_name"] = task_name
        
        query += " ORDER BY SCHEDULED_TIME DESC LIMIT %(limit)s"
        params["limit"] = limit
        
        try:
            results = await self._execute_query(query, params)
            return [SnowflakeTaskHistory(**row) for row in results]
            
        except Exception as e:
            logger.error(f"Failed to get task history: {e}")
            raise SnowflakeTaskAPIError(f"Failed to get task history: {str(e)}")
    
    async def get_tasks(self) -> List[SnowflakeTaskInfo]:
        """Get list of all tasks in the current database/schema."""
        query = """
        SELECT 
            NAME,
            DATABASE_NAME,
            SCHEMA_NAME,
            OWNER,
            COMMENT,
            WAREHOUSE,
            SCHEDULE,
            STATE,
            DEFINITION,
            CONDITION
        FROM INFORMATION_SCHEMA.TASKS
        WHERE DATABASE_NAME = CURRENT_DATABASE()
          AND SCHEMA_NAME = CURRENT_SCHEMA()
        ORDER BY NAME
        """
        
        try:
            results = await self._execute_query(query)
            return [SnowflakeTaskInfo(**row) for row in results]
            
        except Exception as e:
            logger.error(f"Failed to get tasks: {e}")
            raise SnowflakeTaskAPIError(f"Failed to get tasks: {str(e)}")
    
    async def close(self):
        """Close the Snowflake connection."""
        if self.connection and not self.connection.is_closed():
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self.connection.close)
                logger.info("Snowflake connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")


# Convenience functions for use in agents
async def get_snowflake_task_status(
    account: str,
    user: str,
    password: str,
    database: str = "DEV_POWERAPPS",
    schema: str = "AUDIT_JOB_HUB",
    warehouse: str = "COMPUTE_WH",
    role: Optional[str] = None,
    hours_back: int = 24,
) -> List[JobStatusRecord]:
    """Get task status records from Snowflake."""
    client = SnowflakeTaskAPIClient(
        account=account,
        user=user,
        password=password,
        database=database,
        schema=schema,
        warehouse=warehouse,
        role=role,
    )
    
    try:
        # Get task history
        task_history = await client.get_task_history(hours_back=hours_back)
        
        job_records = []
        for task in task_history:
            # Parse timestamps
            last_run_time = None
            if task.started_time:
                if isinstance(task.started_time, datetime):
                    last_run_time = task.started_time.replace(tzinfo=timezone.utc)
                elif isinstance(task.started_time, str):
                    try:
                        last_run_time = datetime.fromisoformat(task.started_time)
                        if last_run_time.tzinfo is None:
                            last_run_time = last_run_time.replace(tzinfo=timezone.utc)
                    except ValueError:
                        logger.warning(f"Failed to parse start time for task {task.name}")
            
            # Calculate duration
            duration_seconds = None
            if task.started_time and task.completed_time:
                try:
                    if isinstance(task.started_time, datetime) and isinstance(task.completed_time, datetime):
                        duration = task.completed_time - task.started_time
                        duration_seconds = int(duration.total_seconds())
                except Exception:
                    logger.warning(f"Failed to calculate duration for task {task.name}")
            
            # Create record
            record = JobStatusRecord(
                job_id=f"snowflake_task_{task.name}_{task.run_id or 'unknown'}",
                platform=PlatformType.SNOWFLAKE_TASK,
                job_name=f"{task.database_name}.{task.schema_name}.{task.name}",
                status=map_snowflake_task_status(task.state),
                last_run_time=last_run_time,
                duration_seconds=duration_seconds,
                error_message=task.error_message,
                metadata={
                    "database_name": task.database_name,
                    "schema_name": task.schema_name,
                    "root_task_id": task.root_task_id,
                    "graph_run_id": task.graph_run_id,
                    "run_id": task.run_id,
                    "error_code": task.error_code,
                    "scheduled_time": str(task.scheduled_time) if task.scheduled_time else None,
                },
                checked_at=datetime.now(timezone.utc),
            )
            job_records.append(record)
        
        logger.info(f"Successfully retrieved {len(job_records)} Snowflake task records")
        return job_records
        
    except Exception as e:
        logger.error(f"Failed to get Snowflake task status: {e}")
        raise SnowflakeTaskAPIError(f"Failed to get task status: {str(e)}")
    
    finally:
        await client.close()