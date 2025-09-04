import http.client
import json
import base64
import time

url = 'nifiui.incrm.cctools.capillarytech.com'
workspaceIds = ['139df77d-0187-1000-ffff-ffffc91f4809']
glueWorkspaceId = 137
glueUrl = 'incrm.connectplus.capillarytech.com'

conn = http.client.HTTPSConnection(url, timeout=10000)
connglue = http.client.HTTPSConnection(glueUrl)
payload = ''
username = 'tools-auser'
password = 'e5zrttpWs0'
auth = base64.b64encode(f"{username}:{password}".encode()).decode()
headers = {
    'Authorization': f'Basic {auth}'
}
print("workspaceId, dataflow_Id,dataflow_name,invalid_count,disabled_count,stoppedCount")

glueHedaers = {
    "Authorization": "Basic YXV0b21hdGlvbl91c2VyOks1MjFyITMzYQ==",
    'Content-Type': 'application/json'
}


def stopInflow(id):
    try:
        payload = '{"action" : "STOP_INFLOW" }'
        connglue.request("PUT", f"/api/workspaces/{glueWorkspaceId}/dataflows/{id}/run-status", payload, glueHedaers)
        res = connglue.getresponse()
        if (res.status != 200):
            print(f'put response status is invalid :  , {str(res.status)} for id {id}')
        else:
            print(f'put happen for id : {id}')
        connglue.close()
    except Exception as e:
        print(f'faced error for 1 {workspaceId} and  {e}')


def stopAllExceptInflow(id):
    try:
        payload = '{"action" : "STOP_ALL_EXCEPT_INFLOW" }'
        connglue.request("PUT",
                         f"/api/workspaces/{glueWorkspaceId}/dataflows/{id}/run-status",
                         payload, glueHedaers)
        res = connglue.getresponse()
        if (res.status != 200):
            print(f'put response status is invalid :  , {str(res.status)} for id {id} ')
        else:
            print(f'put happen for id : {id}')
        connglue.close()
    except Exception as e:
        print(f'faced error for 2  {workspaceId} and  {e}')


def deleteDataflow(id):
    try:
        payload = ''

        connglue.request("DELETE",
                         f"/api/workspaces/{glueWorkspaceId}/dataflows/{id}/hard-delete?forceEmptyConnectionQueues=true",
                         payload, glueHedaers)
        res = connglue.getresponse()
        data = res.read()
        if (res.status != 200):
            print(f'response status is invalid :  , {str(res.status)} for id {id}')
        else:
            print(f'delete dataflow for id {id}')
        connglue.close()
    except Exception as e:
        print(f'faced error for 3  {workspaceId} and  {e}')
    pass


for workspaceId in workspaceIds:
    try:
        conn.request("GET", "/nifi-api/flow/process-groups/" + workspaceId, payload, headers)
        res = conn.getresponse()
        data = res.read()
        if (res.status != 200):
            print(f'response status is invalid :  , {str(res.status)},  {res.reason}')
        responsebody = json.loads(data.decode("utf-8"))
        dataflowgroup = responsebody["processGroupFlow"]["flow"]["processGroups"]
        connections = responsebody["processGroupFlow"]["flow"]["connections"]
    except:
        print(f'faced error for {workspaceId}')

    dataflows = []
    stoppedDataFlows = []
    print(f'total dataflow are {len(dataflowgroup)}')
    for flow in dataflowgroup:
        dataflow = {}
        dataflow["id"] = flow["component"]["id"]
        dataflow["name"] = flow["component"]["name"]
        dataflow["invalidCount"] = flow["component"]["invalidCount"]
        dataflow["disabledCount"] = flow["component"]["disabledCount"]
        dataflow["stoppedCount"] = flow["component"]["stoppedCount"]
        dataflow["runningCount"] = flow["component"]["runningCount"]

        # dataflow["orgId"] = flow["component"].get("variables")
        if dataflow["stoppedCount"] <= 3 and dataflow["disabledCount"] <= 3  and dataflow["invalidCount"] == 0 and \
                dataflow["runningCount"] > 0 :
            dataflows.append(dataflow)
        else:
            stoppedDataFlows.append(dataflow)
            stopInflow(dataflow["id"])
            stopAllExceptInflow(dataflow["id"])
            deleteDataflow(dataflow["id"])

    for df in dataflows:
        print(f'{workspaceId},{df["id"]},{df["name"]},{df["invalidCount"]},{df["disabledCount"]},{df["stoppedCount"]}')

    print("stopped data flows are")
    for df in stoppedDataFlows:
        print(f'{workspaceId},{df["id"]},{df["name"]},{df["invalidCount"]},{df["disabledCount"]},{df["stoppedCount"]}')

