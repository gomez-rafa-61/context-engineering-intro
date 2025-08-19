## FEATURE:

Build an automation framework that can monitor and register the status of existing data pipeline job runs. The framework should be structured to monitor data pipelines process jobs from multiple data stack platforms (AirByte, Databricks, Power Automate, Snowflake Task). The automation process will read the job status from the data stack platform and register the status into a Snowflake database and draft an email notifications to be send to the support team.

- Monitor Airbyte connections and its streams associated
- Store code-base in Github repo
- Use Github workflow actions for execution of job monitor automation process.
- Use Pydantic AI agent and data framework to orchestra the following 
    - evaluation health of each job, 
    - draft email for notification, 
    - register status in Snowflake database 
- Outlook for the email draft agent, Snowflake API agent for registering status, Airbyte API for job status agent

## EXAMPLES:
Use the following example for connecting to AirByte


## DOCUMENTATION:

Airbyte API documentation: https://reference.airbyte.com/reference/getting-started
Pydantic AI documentation: https://ai.pydantic.dev/
Snowflake API documentation: https://docs.snowflake.com/en/developer-guide/sql-api/index
    Snowflake Instance - CURALEAF-CURAPROD.snowflakecomputing.com
    Snowflake Database - DEV_POWERAPPS
    Snowflake Schema - AUDIT_JOB_HUB

## OTHER CONSIDERATIONS:

- Include a .env.example, README with instructions for setup including how to configure Gmail and Brave.
- Include the project structure in the README.
- Virtual environment has already been set up with the necessary dependencies.
- Use python_dotenv and load_env() for environment variables
