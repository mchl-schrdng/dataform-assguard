import os
import logging
import pandas as pd

from authentication import authenticate
from dataform_api import list_workflow_invocations, query_invocation_actions
from bigquery_client import (
    get_bigquery_client,
    get_processed_invocations,
    load_to_bigquery,
    create_all_views
)
from utils import setup_logging


def main() -> None:

    setup_logging()
    logger = logging.getLogger(__name__)

    # Fetch required environment variables
    service_account_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    project_id = os.getenv("PROJECT_ID")
    location = os.getenv("LOCATION")
    repository_id = os.getenv("REPOSITORY_ID")

    # Validate environment variables
    required_env_vars = ["GCP_SERVICE_ACCOUNT_JSON", "PROJECT_ID", "LOCATION", "REPOSITORY_ID"]
    missing_env_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_env_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_env_vars)}")
        return

    logger.info("All required environment variables are set.")

    # Authenticate with GCP
    try:
        token, credentials = authenticate(service_account_json)
        logger.info("Authentication successful.")
    except Exception:
        logger.error("Exiting due to authentication failure.")
        return

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Initialize BigQuery client
    try:
        logger.info("Initializing BigQuery client.")
        bq_client = get_bigquery_client(credentials, project_id)
    except Exception as exc:
        logger.error(f"Failed to initialize BigQuery client: {exc}")
        return

    # Fully qualified table name
    table_fqdn = f"{project_id}.dataform_assguard.assertion_data"

    # Fetch previously processed invocation names
    try:
        processed_invocations = get_processed_invocations(bq_client, table_fqdn)
    except Exception as exc:
        logger.error(f"Exiting due to failure in fetching processed Invocation_Name: {exc}")
        return

    # Fetch all workflow invocations
    try:
        workflow_invocations = list_workflow_invocations(project_id, location, repository_id, headers)
        if not workflow_invocations:
            logger.info("No workflow invocations found. Exiting script.")
            return
    except Exception as exc:
        logger.error(f"Failed to list workflow invocations: {exc}")
        return

    # Collect assertion data for new invocations
    assertion_data = []
    for invocation in workflow_invocations:
        invocation_name = invocation.get("name", "N/A")

        # Skip if this invocation has already been processed
        if invocation_name in processed_invocations:
            logger.info(f"Invocation {invocation_name} already processed. Skipping.")
            continue

        logger.info(f"Processing Invocation: {invocation_name}")
        try:
            actions = query_invocation_actions(invocation_name, headers)
            if not actions:
                logger.warning(f"No actions found for invocation: {invocation_name}")
                continue
        except Exception as exc:
            logger.error(f"Failed to query actions for invocation {invocation_name}: {exc}")
            continue

        # Gather only assertion-related actions
        for action in actions:
            action_name = action.get("target", {}).get("name", "")
            if "assertion" not in action_name.lower():
                continue

            start_time = action.get("invocationTiming", {}).get("startTime")
            end_time = action.get("invocationTiming", {}).get("endTime")
            target_info = action.get("target", {})
            database = target_info.get("database", "N/A")
            schema = target_info.get("schema", "N/A")
            state = action.get("state", "UNKNOWN")
            failure_reason = action.get("failureReason", "N/A") if state == "FAILED" else "N/A"

            logger.info(f"  - Found assertion action: {action_name} with state: {state}")

            assertion_data.append({
                "Start_Time": start_time,
                "End_Time": end_time,
                "Invocation_Name": invocation_name,
                "Action_Name": action_name,
                "Database": database,
                "Schema": schema,
                "State": state,
                "Failure_Reason": failure_reason
            })

    # Exit early if we have no new assertion data
    df = pd.DataFrame(assertion_data)
    if df.empty:
        logger.info("No new assertion data found in this run. Exiting script.")
        return

    # Convert timestamps to proper datetime
    if "Start_Time" in df.columns:
        logger.info("Converting column Start_Time to datetime.")
        df["Start_Time"] = (
            pd.to_datetime(df["Start_Time"], errors="coerce")
              .dt.tz_localize(None)
              .dt.round("us")
        )

    if "End_Time" in df.columns:
        logger.info("Converting column End_Time to datetime.")
        df["End_Time"] = (
            pd.to_datetime(df["End_Time"], errors="coerce")
              .dt.tz_localize(None)
              .dt.round("us")
        )

    # Sort data chronologically
    logger.info("Sorting DataFrame by Start_Time in descending order.")
    df.sort_values(by="Start_Time", ascending=False, inplace=True, na_position="last")
    df.reset_index(drop=True, inplace=True)

    # Load data into BigQuery
    try:
        load_to_bigquery(df, bq_client, table_fqdn)
    except Exception as exc:
        logger.error(f"Exiting due to failure in loading data to BigQuery: {exc}")
        return

    # Create or replace views
    try:
        logger.info("Creating or replacing BigQuery views for analysis.")
        create_all_views(bq_client, project_id=project_id, location=location)
    except Exception as exc:
        logger.error(f"Failed to create or replace BigQuery views: {exc}")
        return

    logger.info("Script execution completed successfully.")


if __name__ == "__main__":
    main()