#!/usr/bin/env python3
"""
Script to fetch workspaces and dataflows from API and check for specific hostname.
Writes matching results to CSV file.
"""

import requests
import csv
import sys
from typing import List, Dict, Any, Optional, Tuple

# API Configuration
BASE_URL = "https://ushccrm.connectplus.capillarytech.com/api"
AUTH_HEADER = "Basic YXV0b21hdGlvbl91c2VyOks1MjFyITMzYQ=="
TARGET_HOSTNAME = "data.capillarydata.com"


def get_workspaces() -> List[Dict[str, Any]]:
    """
    Fetch all workspaces from the API.

    Returns:
        List of workspace dictionaries
    """
    url = f"{BASE_URL}/workspaces"
    headers = {
        "Authorization": AUTH_HEADER,
        "Content-Type": "application/json"
    }

    try:
        print(f"Fetching workspaces from {url}...")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        workspaces = response.json()
        print(f"Found {len(workspaces)} workspaces")
        return workspaces
    except requests.exceptions.RequestException as e:
        print(f"Error fetching workspaces: {e}")
        sys.exit(1)


def get_dataflows(workspace_id: int) -> List[Dict[str, Any]]:
    """
    Fetch all dataflows for a workspace.

    Args:
        workspace_id: The workspace ID

    Returns:
        List of dataflow dictionaries
    """
    url = f"{BASE_URL}/workspaces/{workspace_id}/dataflows"
    headers = {
        "Authorization": AUTH_HEADER,
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        dataflows = response.json()
        return dataflows
    except requests.exceptions.RequestException as e:
        print(f"Error fetching dataflows for workspace {workspace_id}: {e}")
        return []


def get_dataflow_details(workspace_id: int, dataflow_uuid: str) -> Optional[Dict[str, Any]]:
    """
    Fetch detailed information for a specific dataflow.

    Args:
        workspace_id: The workspace ID
        dataflow_uuid: The dataflow UUID

    Returns:
        Dataflow details dictionary or None if error
    """
    url = f"{BASE_URL}/workspaces/{workspace_id}/dataflows/{dataflow_uuid}"
    headers = {
        "Authorization": AUTH_HEADER,
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching dataflow details for {dataflow_uuid}: {e}")
        return None


def check_hostname_in_blocks(dataflow_details: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Check if any block in the dataflow has a field with name="hostname"
    and value matching the target hostname. Also extracts username from the same block.

    Args:
        dataflow_details: The dataflow details dictionary

    Returns:
        Tuple of (True if hostname matches, username value or None)
    """
    if "blocks" not in dataflow_details:
        return False, None

    for block in dataflow_details["blocks"]:
        if "fields" not in block:
            continue

        hostname_found = False
        username_value = None

        # First, check if hostname matches
        for field in block["fields"]:
            if field.get("name") == "hostname" and field.get("value") == TARGET_HOSTNAME:
                hostname_found = True
                break

        # If hostname matches, find username in the same block
        if hostname_found:
            for field in block["fields"]:
                if field.get("name") == "username":
                    username_value = field.get("value", "")
                    break
            return True, username_value

    return False, None


def process_all_workspaces() -> List[Dict[str, Any]]:
    """
    Process all workspaces and dataflows to find matches.

    Returns:
        List of matching records
    """
    matches = []

    # Get all workspaces
    workspaces = get_workspaces()

    # Process each workspace
    for workspace in workspaces:
        workspace_id = workspace.get("id")
        workspace_name = workspace.get("name", "")
        workspace_enabled = workspace.get("enabled", False)
        workspace_uuid = workspace.get("uuid", "")

        # Extract organisation information (take first organisation if multiple exist)
        organisations = workspace.get("organisations", [])
        org_id = ""
        org_name = ""
        if organisations and len(organisations) > 0:
            org_id = organisations[0].get("id", "")
            org_name = organisations[0].get("name", "")

        print(f"\nProcessing workspace: {workspace_name} (ID: {workspace_id})")
        if org_name:
            print(f"  Organisation: {org_name} (ID: {org_id})")

        # Get dataflows for this workspace
        dataflows = get_dataflows(workspace_id)
        print(f"  Found {len(dataflows)} dataflows")

        # Process each dataflow
        for dataflow in dataflows:
            dataflow_name = dataflow.get("name", "")
            dataflow_uuid = dataflow.get("uuid", "")
            dataflow_status = dataflow.get("status", {})
            status_state = dataflow_status.get("state", "") if isinstance(dataflow_status, dict) else str(
                dataflow_status)

            # Generate dataflow URL
            dataflow_url = f"https://ushccrm.connectplus.capillarytech.com/connect/ui/summary/{dataflow_uuid}"

            print(f"    Checking dataflow: {dataflow_name} (UUID: {dataflow_uuid})")

            # Get detailed dataflow information
            dataflow_details = get_dataflow_details(workspace_id, dataflow_uuid)

            if dataflow_details:
                hostname_matches, username = check_hostname_in_blocks(dataflow_details)
                if hostname_matches:
                    print(f"      ✓ Match found! Username: {username}")
                    matches.append({
                        "workspaceId": workspace_id,
                        "workspaceName": workspace_name,
                        "workspaceEnabled": workspace_enabled,
                        "orgId": org_id,
                        "orgName": org_name,
                        "dataflowName": dataflow_name,
                        "dataflowUUID": dataflow_uuid,
                        "dataflowStatus": status_state,
                        "dataflowUrl": dataflow_url,
                        "username": username or ""
                    })

    return matches


def write_to_csv(matches: List[Dict[str, Any]], output_file: str):
    """
    Write matching records to a CSV file.

    Args:
        matches: List of matching records
        output_file: Path to output CSV file
    """
    if not matches:
        print("\nNo matches found. CSV file not created.")
        return

    fieldnames = [
        "workspaceId",
        "workspaceName",
        "workspaceEnabled",
        "orgId",
        "orgName",
        "dataflowName",
        "dataflowUUID",
        "dataflowStatus",
        "dataflowUrl",
        "username"
    ]

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(matches)

    print(f"\n✓ Wrote {len(matches)} matches to {output_file}")


def main():
    """Main function."""
    print("=" * 60)
    print("Dataflow Hostname Checker")
    print("=" * 60)
    print(f"Target hostname: {TARGET_HOSTNAME}")
    print()

    # Process all workspaces and dataflows
    matches = process_all_workspaces()

    # Write results to CSV
    output_file = "dataflow_matches.csv"
    write_to_csv(matches, output_file)

    # Print summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total matches found: {len(matches)}")
    if matches:
        print(f"Results saved to: {output_file}")


if __name__ == "__main__":
    main()

