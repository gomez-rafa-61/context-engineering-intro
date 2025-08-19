"""
Snowflake Database API integration tools for storing job status records.
"""

import asyncio
import logging
from typing import List, Optional, Dict, Any
import snowflake.connector
import json

from models.job_status import JobStatusRecord, PlatformHealthSummary, MonitoringResult

logger = logging.getLogger(__name__)


class SnowflakeDBAPIError(Exception):
    """Custom exception for Snowflake Database API errors."""
    pass


class SnowflakeDBAPIClient:
    """Snowflake Database API client for storing monitoring data."""
    
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
        Initialize Snowflake Database API client.
        
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
                raise SnowflakeDBAPIError(f"Connection failed: {str(e)}")
        
        return self.connection
    
    async def _execute_query(
        self, 
        query: str, 
        params: Optional[List[Any]] = None,
        fetch_results: bool = False,
    ) -> Optional[List[Dict[str, Any]]]:
        """Execute SQL query."""
        connection = await self._get_connection()
        
        try:
            loop = asyncio.get_event_loop()
            
            def _run_query():
                cursor = connection.cursor()
                try:
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    
                    if fetch_results:
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
                    else:
                        return None
                        
                finally:
                    cursor.close()
            
            results = await loop.run_in_executor(None, _run_query)
            if fetch_results:
                logger.debug(f"Query executed successfully, returned {len(results or [])} rows")
            else:
                logger.debug("Query executed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise SnowflakeDBAPIError(f"Query failed: {str(e)}")
    
    async def create_tables_if_not_exist(self):
        """Create the necessary tables for storing monitoring data."""
        # Job status records table
        job_status_table = f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.JOB_STATUS_RECORDS (
            RECORD_ID VARCHAR(255) PRIMARY KEY,
            JOB_ID VARCHAR(255) NOT NULL,
            PLATFORM VARCHAR(50) NOT NULL,
            JOB_NAME VARCHAR(500) NOT NULL,
            STATUS VARCHAR(50) NOT NULL,
            LAST_RUN_TIME TIMESTAMP_NTZ,
            DURATION_SECONDS INTEGER,
            ERROR_MESSAGE TEXT,
            METADATA VARIANT,
            CHECKED_AT TIMESTAMP_NTZ NOT NULL,
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            INDEX(JOB_ID),
            INDEX(PLATFORM),
            INDEX(STATUS),
            INDEX(CHECKED_AT)
        )
        """
        
        # Monitoring sessions table
        monitoring_sessions_table = f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.MONITORING_SESSIONS (
            MONITORING_ID VARCHAR(255) PRIMARY KEY,
            STARTED_AT TIMESTAMP_NTZ NOT NULL,
            COMPLETED_AT TIMESTAMP_NTZ,
            TOTAL_JOBS_MONITORED INTEGER DEFAULT 0,
            FAILED_JOBS_COUNT INTEGER DEFAULT 0,
            SUCCESS_JOBS_COUNT INTEGER DEFAULT 0,
            OVERALL_HEALTH_ASSESSMENT VARIANT,
            PLATFORM_SUMMARIES VARIANT,
            ERRORS VARIANT,
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        
        # Platform health summaries table
        platform_health_table = f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.PLATFORM_HEALTH_SUMMARIES (
            SUMMARY_ID VARCHAR(255) PRIMARY KEY,
            MONITORING_ID VARCHAR(255) NOT NULL,
            PLATFORM VARCHAR(50) NOT NULL,
            TOTAL_JOBS INTEGER DEFAULT 0,
            SUCCESSFUL_JOBS INTEGER DEFAULT 0,
            FAILED_JOBS INTEGER DEFAULT 0,
            RUNNING_JOBS INTEGER DEFAULT 0,
            PLATFORM_STATUS VARCHAR(500),
            LAST_CHECK TIMESTAMP_NTZ NOT NULL,
            ISSUES VARIANT,
            SUCCESS_RATE FLOAT,
            FAILURE_RATE FLOAT,
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            FOREIGN KEY (MONITORING_ID) REFERENCES {self.schema}.MONITORING_SESSIONS(MONITORING_ID)
        )
        """
        
        try:
            await self._execute_query(job_status_table)
            await self._execute_query(monitoring_sessions_table)
            await self._execute_query(platform_health_table)
            logger.info("Database tables created/verified successfully")
            
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise SnowflakeDBAPIError(f"Table creation failed: {str(e)}")
    
    async def insert_job_status_records(self, records: List[JobStatusRecord]) -> int:
        """Insert job status records into the database."""
        if not records:
            return 0
        
        await self.create_tables_if_not_exist()
        
        # Prepare batch insert query
        insert_query = f"""
        INSERT INTO {self.schema}.JOB_STATUS_RECORDS (
            RECORD_ID, JOB_ID, PLATFORM, JOB_NAME, STATUS, 
            LAST_RUN_TIME, DURATION_SECONDS, ERROR_MESSAGE, 
            METADATA, CHECKED_AT
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            connection = await self._get_connection()
            cursor = connection.cursor()
            
            # Prepare batch data
            batch_data = []
            for record in records:
                record_id = f"{record.platform}_{record.job_id}_{int(record.checked_at.timestamp())}"
                
                # Convert metadata to JSON string
                metadata_json = json.dumps(record.metadata) if record.metadata else None
                
                row_data = [
                    record_id,
                    record.job_id,
                    record.platform.value,
                    record.job_name,
                    record.status.value,
                    record.last_run_time,
                    record.duration_seconds,
                    record.error_message,
                    metadata_json,
                    record.checked_at,
                ]
                batch_data.append(row_data)
            
            # Execute batch insert
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, cursor.executemany, insert_query, batch_data)
            cursor.close()
            
            logger.info(f"Successfully inserted {len(records)} job status records")
            return len(records)
            
        except Exception as e:
            logger.error(f"Failed to insert job status records: {e}")
            raise SnowflakeDBAPIError(f"Insert failed: {str(e)}")
    
    async def insert_monitoring_session(self, monitoring_result: MonitoringResult) -> str:
        """Insert a monitoring session record."""
        await self.create_tables_if_not_exist()
        
        insert_query = f"""
        INSERT INTO {self.schema}.MONITORING_SESSIONS (
            MONITORING_ID, STARTED_AT, COMPLETED_AT, TOTAL_JOBS_MONITORED,
            FAILED_JOBS_COUNT, SUCCESS_JOBS_COUNT, OVERALL_HEALTH_ASSESSMENT,
            PLATFORM_SUMMARIES, ERRORS
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            # Calculate success count
            success_count = len([r for r in monitoring_result.job_records if r.status.value == "success"])
            failed_count = len([r for r in monitoring_result.job_records if r.status.value == "failed"])
            
            # Prepare data
            data = [
                monitoring_result.monitoring_id,
                monitoring_result.started_at,
                monitoring_result.completed_at,
                monitoring_result.total_jobs_monitored,
                failed_count,
                success_count,
                json.dumps(monitoring_result.overall_assessment.dict()) if monitoring_result.overall_assessment else None,
                json.dumps([s.dict() for s in monitoring_result.platform_summaries]),
                json.dumps(monitoring_result.errors),
            ]
            
            await self._execute_query(insert_query, data)
            
            # Insert platform summaries
            if monitoring_result.platform_summaries:
                await self._insert_platform_summaries(
                    monitoring_result.monitoring_id,
                    monitoring_result.platform_summaries
                )
            
            logger.info(f"Successfully inserted monitoring session: {monitoring_result.monitoring_id}")
            return monitoring_result.monitoring_id
            
        except Exception as e:
            logger.error(f"Failed to insert monitoring session: {e}")
            raise SnowflakeDBAPIError(f"Insert failed: {str(e)}")
    
    async def _insert_platform_summaries(
        self, 
        monitoring_id: str, 
        summaries: List[PlatformHealthSummary]
    ):
        """Insert platform health summaries."""
        insert_query = f"""
        INSERT INTO {self.schema}.PLATFORM_HEALTH_SUMMARIES (
            SUMMARY_ID, MONITORING_ID, PLATFORM, TOTAL_JOBS,
            SUCCESSFUL_JOBS, FAILED_JOBS, RUNNING_JOBS, PLATFORM_STATUS,
            LAST_CHECK, ISSUES, SUCCESS_RATE, FAILURE_RATE
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        batch_data = []
        for summary in summaries:
            summary_id = f"{monitoring_id}_{summary.platform.value}"
            
            row_data = [
                summary_id,
                monitoring_id,
                summary.platform.value,
                summary.total_jobs,
                summary.successful_jobs,
                summary.failed_jobs,
                summary.running_jobs,
                summary.platform_status,
                summary.last_check,
                json.dumps(summary.issues),
                summary.success_rate,
                summary.failure_rate,
            ]
            batch_data.append(row_data)
        
        connection = await self._get_connection()
        cursor = connection.cursor()
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, cursor.executemany, insert_query, batch_data)
        cursor.close()
    
    async def get_recent_job_status(
        self,
        platform: Optional[str] = None,
        hours_back: int = 24,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get recent job status records."""
        query = f"""
        SELECT *
        FROM {self.schema}.JOB_STATUS_RECORDS
        WHERE CHECKED_AT >= DATEADD(hour, -?, CURRENT_TIMESTAMP())
        """
        
        params = [hours_back]
        
        if platform:
            query += " AND PLATFORM = ?"
            params.append(platform)
        
        query += " ORDER BY CHECKED_AT DESC LIMIT ?"
        params.append(limit)
        
        try:
            results = await self._execute_query(query, params, fetch_results=True)
            return results or []
            
        except Exception as e:
            logger.error(f"Failed to get recent job status: {e}")
            raise SnowflakeDBAPIError(f"Query failed: {str(e)}")
    
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
async def store_job_status_records(
    records: List[JobStatusRecord],
    account: str,
    user: str,
    password: str,
    database: str = "DEV_POWERAPPS",
    schema: str = "AUDIT_JOB_HUB",
    warehouse: str = "COMPUTE_WH",
    role: Optional[str] = None,
) -> int:
    """Store job status records in Snowflake database."""
    client = SnowflakeDBAPIClient(
        account=account,
        user=user,
        password=password,
        database=database,
        schema=schema,
        warehouse=warehouse,
        role=role,
    )
    
    try:
        return await client.insert_job_status_records(records)
    finally:
        await client.close()


async def store_monitoring_result(
    monitoring_result: MonitoringResult,
    account: str,
    user: str,
    password: str,
    database: str = "DEV_POWERAPPS",
    schema: str = "AUDIT_JOB_HUB",
    warehouse: str = "COMPUTE_WH",
    role: Optional[str] = None,
) -> str:
    """Store complete monitoring result in Snowflake database."""
    client = SnowflakeDBAPIClient(
        account=account,
        user=user,
        password=password,
        database=database,
        schema=schema,
        warehouse=warehouse,
        role=role,
    )
    
    try:
        return await client.insert_monitoring_session(monitoring_result)
    finally:
        await client.close()