import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# url = "https://apitester.capillary.in/apitest_app/redisRequest/getKeyByPattern?key=Event_Notification_Crm_Staging_New_Sanity*event_notification"
url = "https://apitester.capillary.in/apitest_app/redisRequest/getKeyByPattern?key=Connect_Plus_Ushc_Crm*connect_plus"

headers = {
    "Authorization": "Basic Q2FwaWxsYXJ5OklVTmhjR2xzYkdGeWVVQTROekl5",
    "Cookie": "csrftoken=TYWqGwlniVB5w9drA7apzbYSuzYor4b4"
}
# 👇 Add verify=False
response = requests.get(url, headers=headers, verify=False)
if response.status_code == 200 and response.json().get('status') == 'success':
    keys = response.json().get('keys', [])
    for key in keys:
        delete_url = "https://apitester.capillary.in/apitest_app/redisRequest/deleteKey"
        params = {'key': key}
        delete_headers = headers.copy()
        delete_headers['Content-Type'] = 'application/json'
        delete_response = requests.get(delete_url, headers=delete_headers, params=params, verify=False)
        print(f"Deletion status for key '{key}': {delete_response.status_code}")
else:
    print("Failed to retrieve keys or no keys found.")