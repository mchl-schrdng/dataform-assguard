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
    s = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    p = os.getenv("PROJECT_ID")
    l = os.getenv("LOCATION")
    r = os.getenv("REPOSITORY_ID")
    logger = logging.getLogger(__name__)
    v = ["GCP_SERVICE_ACCOUNT_JSON", "PROJECT_ID", "LOCATION", "REPOSITORY_ID"]
    m = [x for x in v if not os.getenv(x)]
    if m:
        logger.error(f"Missing required environment variables: {', '.join(m)}")
        return
    logger.info("All required environment variables are set.")
    try:
        t, c = authenticate(s)
        logger.info("Authentication successful.")
    except Exception:
        logger.error("Exiting due to authentication failure.")
        return
    h = {"Authorization": f"Bearer {t}", "Content-Type": "application/json"}
    try:
        logger.info("Initializing BigQuery client.")
        cl = get_bigquery_client(c, p)
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        return
    fq = f"{p}.dataform_assguard.assertion_data"
    try:
        pi = get_processed_invocations(cl, fq)
    except Exception as e:
        logger.error(f"Exiting due to failure in fetching processed Invocation_Name: {e}")
        return
    try:
        w = list_workflow_invocations(p, l, r, h)
        if not w:
            logger.info("No workflow invocations found. Exiting script.")
            return
    except Exception as e:
        logger.error(f"Failed to list workflow invocations: {e}")
        return
    d = []
    for inv in w:
        n = inv.get("name", "N/A")
        if n in pi:
            continue
        logger.info(f"Processing Invocation: {n}")
        try:
            a = query_invocation_actions(n, h)
            if not a:
                logger.warning(f"No actions found for invocation: {n}")
                continue
        except Exception as e:
            logger.error(f"Failed to query actions for invocation {n}: {e}")
            continue
        for x in a:
            an = x.get("target", {}).get("name", "")
            if "assertion" in an.lower():
                s0 = x.get("invocationTiming", {}).get("startTime")
                e0 = x.get("invocationTiming", {}).get("endTime")
                t0 = x.get("target", {})
                db = t0.get("database", "N/A")
                sch = t0.get("schema", "N/A")
                st = x.get("state", "UNKNOWN")
                fr = x.get("failureReason", "N/A") if st == "FAILED" else "N/A"
                logger.info(f"  - Found assertion action: {an} with state: {st}")
                d.append({
                    "Start_Time": s0,
                    "End_Time": e0,
                    "Invocation_Name": n,
                    "Action_Name": an,
                    "Database": db,
                    "Schema": sch,
                    "State": st,
                    "Failure_Reason": fr
                })
    f = pd.DataFrame(d)
    if f.empty:
        logger.info("No new assertion data found in this run. Exiting script.")
        return
    if "Start_Time" in f.columns:
        logger.info("Converting column Start_Time to datetime.")
        f["Start_Time"] = pd.to_datetime(f["Start_Time"], errors="coerce").dt.tz_localize(None).dt.round("us")
    if "End_Time" in f.columns:
        logger.info("Converting column End_Time to datetime.")
        f["End_Time"] = pd.to_datetime(f["End_Time"], errors="coerce").dt.tz_localize(None).dt.round("us")
    logger.info("Sorting DataFrame by Start_Time in descending order.")
    f.sort_values(by="Start_Time", ascending=False, inplace=True, na_position="last")
    f.reset_index(drop=True, inplace=True)
    try:
        load_to_bigquery(f, cl, fq)
    except Exception as e:
        logger.error(f"Exiting due to failure in loading data to BigQuery: {e}")
        return
    try:
        logger.info("Creating or replacing BigQuery views for analysis.")
        create_all_views(cl, project_id=p, location=l)
    except Exception as e:
        logger.error(f"Failed to create or replace BigQuery views: {e}")
        return
    logger.info("Script execution completed successfully.")

if __name__ == "__main__":
    main()
