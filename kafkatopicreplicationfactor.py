import requests
from requests.auth import HTTPBasicAuth

# NiFi API base URL
NIFI_API_URL = 'https://nifiui.incrm.cctools.capillarytech.com/nifi-api'
username = 'tools-auser'
password = 'e5zrttpWs0'
auth = HTTPBasicAuth(username, password)

# Set up a session to reuse connections
session = requests.Session()
session.auth = auth
session.headers.update({'Content-Type': 'application/json'})

# Function to get process groups
def get_process_groups(base_url, session):
    try:
        # Endpoint for root process group
        root_pg_url = f"{base_url}/flow/process-groups/root"

        # Get the root process group
        response = session.get(root_pg_url, verify=False)  # Add verify=False if using self-signed SSL
        response.raise_for_status()
        data = response.json()

        # Extract process groups under the root
        process_groups = data["processGroupFlow"]["flow"]["processGroups"]

        # Print process group details
        for pg in process_groups:
            pg_id = pg["id"]
            pg_name = pg["component"]["name"]
            print(f"Process Group ID: {pg_id}, Name: {pg_name}")
    except requests.exceptions.RequestException as e:
        print(f"Error while fetching process groups: {e}")

# Call the function
get_process_groups(NIFI_API_URL, session)
