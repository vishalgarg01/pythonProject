import requests
from requests.auth import HTTPBasicAuth

# Configuration
nifi_url = 'https://nifiui.eucrm.cctools.capillarytech.com/nifi-api'
username = 'tools-auser'
password = 'ovgwAHmXe1'
group_id = '713e6e60-0193-1000-ffff-ffff8500d846'
auth = HTTPBasicAuth(username, password)


def list_controller_services():
    url = f'{nifi_url}/flow/process-groups/{group_id}/controller-services?version=0&clientId=eeba5497-0190-1000-d0c4-38566028fab5&disconnectedNodeAcknowledged=false'
    response = requests.get(url, auth=auth, timeout=3000)
    response.raise_for_status()
    return response.json()


def delete_controller_service(service_id, revision_version):
    try:
        url = f'{nifi_url}/controller-services/{service_id}?version=0&clientId=eeba5497-0190-1000-d0c4-38566028fab5&disconnectedNodeAcknowledged=false'
        headers = {'Content-Type': 'application/json'}
        # data = {
        #     'revision': { 'version': 0,},
        #     'disconnectedNodeAcknowledged': 'false'
        # }
        response = requests.delete(url, headers=headers,  auth=auth)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"error for {service_id}")

print('starting')
services = list_controller_services()
print('fetched')
print(len(services['controllerServices']))
for service in services['controllerServices']:
    status = service['component']['state']
    service_id = service['id']
    revision_version = service['revision']['version']
    print(f'service name: {service_id}')
    print(f'service status: {status}')

    if status == 'DISABLED':
        print(f'Deleting disabled controller service: {service_id}')
        delete_controller_service(service_id, revision_version)


