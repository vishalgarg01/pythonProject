import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import Timeout, RequestException

# Configuration
nifi_url = 'https://nifiui.incrm.cctools.capillarytech.com/nifi-api'
username = 'tools-auser'
password = 'e5zrttpWs0'
group_id = '564cffc4-0173-1000-14d4-23a93d64c5f5'
auth = HTTPBasicAuth(username, password)

# Set up a session to reuse connections
session = requests.Session()
session.auth = auth
session.headers.update({'Content-Type': 'application/json'})


def list_controller_services():
    url = f'{nifi_url}/flow/process-groups/{group_id}/controller-services?version=0&clientId=eeba5497-0190-1000-d0c4-38566028fab5&disconnectedNodeAcknowledged=false'
    try:
        response = session.get(url, timeout=70)  # Adjust timeout if necessary
        response.raise_for_status()  # Check if the request was successful
        return response.json()  # Return the JSON data if successful
    except Timeout:
        print(f"Timeout occurred while fetching controller services from {url}")
    except RequestException as e:
        print(f"Request failed: {e}")
        if hasattr(e.response, 'text'):
            print("Response body:", e.response.text)
    except Exception as e:
        print(f"An error occurred: {e}")
    return {}


def delete_controller_service(service_id="3d312821-818c-3087-99c5-363de3ac2976"):
    url = f'{nifi_url}/controller-services/{service_id}?version=0&clientId=eeba5497-0190-1000-d0c4-38566028fab5&disconnectedNodeAcknowledged=false'
    try:
        response = session.delete(url, timeout=70)  # Adjust timeout if necessary
        response.raise_for_status()  # Check if the delete request was successful
        return response.json()  # Return the JSON data if successful
    except Timeout:
        print(f"Timeout occurred while deleting controller service {service_id}")
    except RequestException as e:
        print(f"Request failed: {e}")
    except Exception as e:
        print(f"An error occurred while deleting {service_id}: {e}")
    return None


print('starting')
# delete_controller_service()
print('starting1')

services = list_controller_services()

if services:
    print('fetched')
    print(f'Number of services: {len(services.get("controllerServices", []))}')

    for service in services.get('controllerServices', []):
        status = service['component']['state']
        service_id = service['id']
        print(f'service name: {service_id}')
        print(f'service status: {status}')

        if status == 'DISABLED':
            print(f'Deleting disabled controller service: {service_id}')
            delete_controller_service(service_id)

print('done')
