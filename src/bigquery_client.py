import logging
import time
from typing import Set
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import Client
from google.cloud.exceptions import NotFound, GoogleCloudError

logger = logging.getLogger(__name__)

DATASET_NAME = "dataform_assguard"
TABLE_NAME = "assertion_data"
SYNTHESIS_VIEW_SUFFIX = "_synthesis_by_assertion"
RECAP_VIEW_SUFFIX = "_recap_by_day_view"

def get_bigquery_client(credentials, project_id: str) -> Client:
    try:
        c = bigquery.Client(credentials=credentials, project=project_id)
        logger.info("BigQuery client initialized successfully.")
        return c
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        raise

def ensure_table_exists(client: Client, table_fqdn: str, schema: list, location: str) -> None:
    try:
        client.get_table(table_fqdn)
        logger.info(f"BigQuery table {table_fqdn} exists.")
    except NotFound:
        logger.warning(f"BigQuery table {table_fqdn} not found. Creating table.")
        t = bigquery.Table(table_fqdn, schema=schema)
        try:
            client.create_table(t)
            logger.info(f"BigQuery table {table_fqdn} created successfully.")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Failed to create BigQuery table {table_fqdn}: {e}")
            raise

def get_processed_invocations(client: Client, table_fqdn: str) -> Set[str]:
    schema = [
        bigquery.SchemaField("Start_Time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("End_Time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("Invocation_Name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Action_Name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Database", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Schema", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("State", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Failure_Reason", "STRING", mode="NULLABLE"),
    ]
    r = client.dataset(DATASET_NAME)
    try:
        d = client.get_dataset(r)
        loc = d.location
    except NotFound:
        logger.error(f"Dataset {DATASET_NAME} does not exist in project {client.project}.")
        raise
    ensure_table_exists(client, table_fqdn, schema, loc)
    q = f"SELECT DISTINCT Invocation_Name FROM `{table_fqdn}`"
    try:
        logger.debug(f"Executing query: {q}")
        job = client.query(q)
        res = job.result()
        s = {row.Invocation_Name for row in res if row.Invocation_Name}
        logger.info(f"Retrieved {len(s)} processed invocation names.")
        return s
    except GoogleCloudError as e:
        logger.error(f"Error fetching processed Invocation_Name: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching processed Invocation_Name: {e}")
        raise

def load_to_bigquery(df: pd.DataFrame, client: Client, table_fqdn: str) -> None:
    try:
        r = client.dataset(DATASET_NAME)
        d = client.get_dataset(r)
        loc = d.location
        logger.info(f"Dataset location: {loc}")
        schema = [
            bigquery.SchemaField("Start_Time", "TIMESTAMP"),
            bigquery.SchemaField("End_Time", "TIMESTAMP"),
            bigquery.SchemaField("Invocation_Name", "STRING"),
            bigquery.SchemaField("Action_Name", "STRING"),
            bigquery.SchemaField("Database", "STRING"),
            bigquery.SchemaField("Schema", "STRING"),
            bigquery.SchemaField("State", "STRING"),
            bigquery.SchemaField("Failure_Reason", "STRING"),
        ]
        ensure_table_exists(client, table_fqdn, schema, loc)
        cfg = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=schema,
            source_format=bigquery.SourceFormat.PARQUET,
        )
        job = client.load_table_from_dataframe(df, table_fqdn, job_config=cfg)
        job.result()
        logger.info(f"Data successfully loaded into BigQuery table: {table_fqdn}")
    except GoogleCloudError as e:
        logger.error(f"Failed to load data to BigQuery: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading data to BigQuery: {e}")
        raise

def get_view_query_synthesis(table_fqdn: str, synthesis_view_fqdn: str) -> str:
    return f"""
    CREATE OR REPLACE VIEW `{synthesis_view_fqdn}` AS
    WITH processed_data AS (
        SELECT
            Action_Name,
            State,
            TIMESTAMP_DIFF(End_Time, Start_Time, SECOND) AS duration_seconds
        FROM `{table_fqdn}`
    )
    SELECT
        Action_Name,
        COUNT(*) AS total_executions,
        COUNTIF(State = 'SUCCEEDED') AS passed_executions,
        COUNTIF(State = 'FAILED') AS failed_executions,
        ROUND((COUNTIF(State = 'FAILED') / COUNT(*)) * 100, 2) AS failure_percentage,
        ROUND(AVG(duration_seconds), 2) AS avg_duration_seconds,
        ROUND(MIN(duration_seconds), 2) AS min_duration_seconds,
        ROUND(MAX(duration_seconds), 2) AS max_duration_seconds
    FROM
        processed_data
    GROUP BY
        Action_Name
    ORDER BY
        failure_percentage DESC,
        total_executions DESC
    """

def get_view_query_recap(table_fqdn: str, recap_view_fqdn: str) -> str:
    return f"""
    CREATE OR REPLACE VIEW `{recap_view_fqdn}` AS
    WITH processed_data AS (
        SELECT
            DATE(Start_Time) AS assertion_date,
            State,
            TIMESTAMP_DIFF(End_Time, Start_Time, SECOND) AS duration_seconds
        FROM `{table_fqdn}`
    )
    SELECT
        assertion_date,
        COUNT(*) AS total_assertions,
        COUNTIF(State = 'SUCCEEDED') AS passed_assertions,
        COUNTIF(State = 'FAILED') AS failed_assertions,
        ROUND((COUNTIF(State = 'FAILED') / COUNT(*)) * 100, 2) AS failure_percentage,
        ROUND(AVG(duration_seconds), 2) AS avg_duration_seconds,
        ROUND(MIN(duration_seconds), 2) AS min_duration_seconds,
        ROUND(MAX(duration_seconds), 2) AS max_duration_seconds
    FROM
        processed_data
    GROUP BY
        assertion_date
    ORDER BY
        assertion_date DESC
    """

def create_or_replace_view(client: Client, ddl_query: str, location: str) -> None:
    try:
        job = client.query(ddl_query, location=location)
        job.result()
        logger.info("View created or replaced successfully.")
    except GoogleCloudError as e:
        logger.error(f"Failed to create or replace view: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while creating/replacing view: {e}")
        raise

def create_all_views(client: Client, project_id: str, location: str) -> None:
    table_fqdn = f"{project_id}.{DATASET_NAME}.{TABLE_NAME}"
    s_name = f"{table_fqdn}{SYNTHESIS_VIEW_SUFFIX}"
    r_name = f"{table_fqdn}{RECAP_VIEW_SUFFIX}"
    s_query = get_view_query_synthesis(table_fqdn, s_name)
    r_query = get_view_query_recap(table_fqdn, r_name)
    logger.info(f"Creating or replacing view: {s_name}")
    create_or_replace_view(client, s_query, location)
    logger.info(f"Creating or replacing view: {r_name}")
    create_or_replace_view(client, r_query, location)
