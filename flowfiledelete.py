import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import Timeout, RequestException
from lxml import etree  # To parse XML

# Configuration
nifi_url = 'https://nifiui.eucrm.cctools.capillarytech.com/nifi-api'
username = 'tools-auser'
password = 'e5zrttpWs0'
group_id = '564cffc4-0173-1000-14d4-23a93d64c5f5'
# auth = HTTPBasicAuth(username, password)

# Set up a session to reuse connections
session = requests.Session()
# session.auth = auth
session.headers.update({'Content-Type': 'application/json',
                        "Cookie": "wfx_unq=EKlRcQj1qmUvH39U; _BEAMER_USER_ID_vnosnxuz35404=65b5763d-3d76-4523-bc21-916bd10a63a3; _BEAMER_FIRST_VISIT_vnosnxuz35404=2025-01-09T06:31:18.428Z; _BEAMER_DATE_vnosnxuz35404=2024-03-17T15:10:30.000Z; _ga_KL2T5V26LV=GS1.2.1746461066.2.1.1746461523.0.0.0; _ga_DMDM65MHS0=GS2.2.s1747934220$o3$g1$t1747934430$j0$l0$h0; _hjSessionUser_5012341=eyJpZCI6IjkyMDk3MWYyLTU2NWMtNTczZS1hMDFkLWZkNTc2YzkxNTUxMCIsImNyZWF0ZWQiOjE3NTIxNjYwMDQ0NDMsImV4aXN0aW5nIjpmYWxzZX0=; _ga_YN8Y1WW2VC=GS2.1.s1752166002$o1$g1$t1752166005$j57$l0$h0; _ga_EKC3LFCMST=GS2.2.s1754298705$o3$g1$t1754298705$j60$l0$h0; fs_uid=#AQWY4#78027e9d-f5a0-469a-99fa-685785e0c614:2d8b1718-97a7-461a-ab58-c13befb3f85e:1754298706042::1#1c4ad302#/1777997108; _ga_8ZVPN9KQ6D=GS2.1.s1754904482$o20$g1$t1754904693$j6$l0$h0; _ga=GA1.2.1183059349.1736404268; _ga_Z68ZS274TW=GS2.2.s1755807737$o37$g1$t1755807803$j60$l0$h0; _gid=GA1.2.175924596.1756188402; session=eyJjc3JmX3Rva2VuIjoiZDYyNDhmYTVlYmM1YjIxNDM4OWZhYmIxOTM0MDJkZjNlNmVjZjAxMCJ9.aK1diQ._ExZ51V_ypXXHhLl0nNvG2cplaI; tools_session=gAAAAABorWJQ1-cbECsEVcU943EJyvOhj0RrLGyZvIZR5lXk2Gwc7qWoiKaOv7Xlf0XCnk14QrWQjzOwGBhDz994r2lX8HRJrOQowKn4h6LJzKbnFoNR3YI=_84a5b794ccf569f29410d5468cbdfb4ee89ccaeee8c19983a4bee5567cc2b4cc"})


def delete_controller_service(service_id):
    url = f'{nifi_url}/controller-services/{service_id}?version=0&clientId=eeba5497-0190-1000-d0c4-38566028fab5&disconnectedNodeAcknowledged=false'
    try:
        response = session.delete(url, timeout=20)  # Adjust timeout if necessary
        response.raise_for_status()  # Check if the delete request was successful
        return response.json()  # Return the JSON data if successful
    except Timeout:
        print(f"Timeout occurred while deleting controller service {service_id}")
    except RequestException as e:
        print(f"Request failed: {e}")
    except Exception as e:
        print(f"An error occurred while deleting {service_id}: {e}")
    return None


def parse_and_delete_from_xml(xml_file):
    try:
        # Parse the flow.xml file
        tree = etree.parse(xml_file)
        root = tree.getroot()

        # Loop through all controllerService elements
        for controller_service in root.xpath("//controllerService"):
            enabled = controller_service.find("enabled")
            service_id = controller_service.find("id").text
            if enabled is not None and enabled.text.lower() == 'false':
                print(f"Disabling controller service: {service_id}")
                delete_controller_service(service_id)
    except etree.XMLSyntaxError as e:
        print(f"XML Parsing Error: {e}")
    except Exception as e:
        print(f"An error occurred while parsing the XML: {e}")


# Usage
print('starting')
# XML path to the flow.xml file
xml_file_path = '/Users/vishalgarg/Downloads/euflow.xml.gz'

# Parse the XML and delete disabled services
parse_and_delete_from_xml(xml_file_path)

print('done')
