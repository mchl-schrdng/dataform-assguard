import logging
import requests
from typing import List

logger = logging.getLogger(__name__)

def list_workflow_invocations(project_id: str, location: str, repository_id: str, headers: dict) -> List[dict]:
    p = f"projects/{project_id}/locations/{location}/repositories/{repository_id}"
    u = f"https://dataform.googleapis.com/v1beta1/{p}/workflowInvocations"
    try:
        logger.info(f"Listing workflow invocations from {u}.")
        r = requests.get(u, headers=headers)
        r.raise_for_status()
        i = r.json().get("workflowInvocations", [])
        logger.info(f"Retrieved {len(i)} workflow invocations.")
        return i
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error listing WorkflowInvocations: {e}")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception listing WorkflowInvocations: {e}")
        return []

def query_invocation_actions(invocation_name: str, headers: dict) -> List[dict]:
    u = f"https://dataform.googleapis.com/v1beta1/{invocation_name}:query"
    try:
        logger.info(f"Querying actions for invocation: {invocation_name}.")
        r = requests.get(u, headers=headers)
        r.raise_for_status()
        a = r.json().get("workflowInvocationActions", [])
        logger.info(f"Retrieved {len(a)} actions for invocation {invocation_name}.")
        return a
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error querying actions for {invocation_name}: {e}")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception querying actions for {invocation_name}: {e}")
        return []