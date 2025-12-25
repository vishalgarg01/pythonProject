import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURATION ---
# BASE_URL = "https://glue-a-crm-crm-staging-new.cc.capillarytech.com/api"
BASE_URL = "https://ushccrm.connectplus.capillarytech.com//api"

# TODO: Replace with your actual authentication headers
# For example: HEADERS = {"Authorization": "Bearer YOUR_ACCESS_TOKEN"}
HEADERS = {
    "X-CAP-SOURCE": "extension",
    "Authorization": "Basic YXV0b21hdGlvbl91c2VyOks1MjFyITMzYQ=="
}


def get_workspaces():
    """Fetches all workspaces."""
    try:
        response = requests.get(f"{BASE_URL}/workspaces", headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching workspaces: {e}")
        return []


def get_workspace_dataflows(workspace_id):
    """Fetches all dataflows for a given workspace."""
    try:
        url = f"{BASE_URL}/workspaces/{workspace_id}/dataflows"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching dataflows for workspace {workspace_id}: {e}")
        return []


def stop_dataflow(workspace_id, dataflow_id):
    """Stops a specific dataflow."""
    try:
        url = f"{BASE_URL}/workspaces/{workspace_id}/dataflows/{dataflow_id}/state/Stopped"
        # response = requests.put(url, headers=HEADERS)
        # response.raise_for_status()
        logging.info(f"Successfully stopped dataflow {dataflow_id} in workspace {workspace_id}")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Error stopping dataflow {dataflow_id} in workspace {workspace_id}: {e}")
        return False


def delete_dataflow(workspace_id, dataflow_id):
    """Deletes a specific dataflow."""
    try:
        url = f"{BASE_URL}/workspaces/{workspace_id}/dataflows/{dataflow_id}/hard-delete"
        response = requests.delete(url, headers=HEADERS)
        response.raise_for_status()
        logging.info(f"Successfully deleted dataflow {dataflow_id} in workspace {workspace_id}")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Error deleting dataflow {dataflow_id} in workspace {workspace_id}: {e}")
        return False


def main():
    """Main function to orchestrate the process."""
    workspaces = get_workspaces()
    if not workspaces:
        logging.info("No workspaces found or error in fetching them.")
        return

    for workspace in workspaces:
        workspace_id = workspace.get('id')  # Assuming the workspace object has an 'id'
        if not workspace_id:
            logging.warning(f"Could not find ID for workspace: {workspace}")
            continue

        logging.info(f"Processing workspace: {workspace.get('name', workspace_id)}")
        dataflows = get_workspace_dataflows(workspace_id)

        for dataflow in dataflows:
            dataflow_id = dataflow.get('uuid')  # Assuming the dataflow object has an 'id'
            if not dataflow_id:
                logging.warning(f"Could not find ID for dataflow: {dataflow}")
                continue

            logging.info(f"  Stopping dataflow: {dataflow.get('name', dataflow_id)}")
            if stop_dataflow(workspace_id, dataflow_id):
                logging.info(f"  Deleting dataflow: {dataflow.get('name', dataflow_id)}")
                delete_dataflow(workspace_id, dataflow_id)


if __name__ == "__main__":
    main()

