#!/usr/bin/env python3
"""
Script to fetch workspaces and dataflows from API and summarise counts.
Writes per-workspace totals to a CSV file.
"""

import requests
import csv
import sys
from typing import List, Dict, Any

# API Configuration
BASE_URL = "https://uscrm.connectplus.capillarytech.com/api"
AUTH_HEADER = "Basic YXV0b21hdGlvbl91c2VyOks1MjFyITMzYQ=="


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


def process_all_workspaces() -> List[Dict[str, Any]]:
    """
    Process all workspaces and return dataflow counts per workspace.

    Returns:
        List of summary records
    """
    summaries = []

    # Get all workspaces
    workspaces = get_workspaces()

    # Process each workspace
    for workspace in workspaces:
        workspace_id = workspace.get("id")
        workspace_name = workspace.get("name", "")
        workspace_enabled = bool(workspace.get("enabled", True))

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
        if not workspace_enabled:
            print("  Skipping disabled workspace.")
            continue

        # Get dataflows for this workspace
        dataflows = get_dataflows(workspace_id)
        total_dataflows = len(dataflows)
        live_dataflows = 0

        for dataflow in dataflows:
            status = dataflow.get("status", {})
            if isinstance(status, dict):
                state = status.get("state", "")
            else:
                state = status

            if str(state).lower() == "live":
                live_dataflows += 1

        print(f"  Total dataflows: {total_dataflows}")
        print(f"  Live dataflows: {live_dataflows}")

        summaries.append({
            "orgId": org_id,
            "orgName": org_name,
            "workspaceName": workspace_name,
            "workspaceId": workspace_id,
            "totalDataflows": total_dataflows,
            "liveDataflows": live_dataflows
        })

    return summaries


def write_to_csv(records: List[Dict[str, Any]], output_file: str):
    """
    Write summary records to a CSV file.

    Args:
        records: List of summary records
        output_file: Path to output CSV file
    """
    if not records:
        print("\nNo records found. CSV file not created.")
        return

    fieldnames = [
        "orgId",
        "orgName",
        "workspaceName",
        "workspaceId",
        "totalDataflows",
        "liveDataflows"
    ]

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"\n✓ Wrote {len(records)} records to {output_file}")


def main():
    """Main function."""
    print("=" * 60)
    print("Dataflow Count Summary")
    print("=" * 60)
    print()

    # Process all workspaces and dataflows
    summaries = process_all_workspaces()

    # Write results to CSV
    output_file = "workspace_dataflow_counts.csv"
    write_to_csv(summaries, output_file)

    # Print summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total workspaces summarised: {len(summaries)}")
    if summaries:
        print(f"Results saved to: {output_file}")


if __name__ == "__main__":
    main()

