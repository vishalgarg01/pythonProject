#!/usr/bin/env python3
"""
Fetch enabled workspaces and their live dataflows, then emit the ordered
block types for each dataflow to a JSONL file for downstream analysis.
"""

import json
import sys
from typing import Any, Dict, List, Optional

import requests

# API configuration
BASE_URL = "https://uscrm.connectplus.capillarytech.com/api"
AUTH_HEADER = "Basic YXV0b21hdGlvbl91c2VyOks1MjFyITMzYQ=="


def get_workspaces() -> List[Dict[str, Any]]:
    """Fetch all workspaces from the API."""
    url = f"{BASE_URL}/workspaces"
    headers = {
        "Authorization": AUTH_HEADER,
        "Content-Type": "application/json",
    }

    try:
        print(f"Fetching workspaces from {url}...")
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        workspaces = resp.json()
        print(f"Found {len(workspaces)} workspaces")
        return workspaces
    except requests.exceptions.RequestException as exc:
        print(f"Error fetching workspaces: {exc}")
        sys.exit(1)


def get_dataflows(workspace_id: int) -> List[Dict[str, Any]]:
    """Fetch all dataflows for a workspace."""
    url = f"{BASE_URL}/workspaces/{workspace_id}/dataflows"
    headers = {
        "Authorization": AUTH_HEADER,
        "Content-Type": "application/json",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as exc:
        print(f"Error fetching dataflows for workspace {workspace_id}: {exc}")
        return []


def get_dataflow_details(workspace_id: int, dataflow_uuid: str) -> Optional[Dict[str, Any]]:
    """Fetch detailed information for a specific dataflow."""
    url = f"{BASE_URL}/workspaces/{workspace_id}/dataflows/{dataflow_uuid}"
    headers = {
        "Authorization": AUTH_HEADER,
        "Content-Type": "application/json",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as exc:
        print(f"Error fetching dataflow details for {dataflow_uuid}: {exc}")
        return None


def extract_block_sequence(dataflow_details: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract ordered block metadata from dataflow details.

    Returns a list of dicts containing the block order (if present) and type.
    """
    blocks = dataflow_details.get("blocks", [])
    sequence: List[Dict[str, Any]] = []

    for block in blocks:
        sequence.append({
            "order": block.get("order"),
            "type": block.get("type"),
        })

    # Sort by order when available; keep stable ordering otherwise.
    sequence.sort(key=lambda b: (float("inf") if b["order"] is None else b["order"]))
    return sequence


def process() -> List[Dict[str, Any]]:
    """Process enabled workspaces and live dataflows, returning block sequences."""
    output_records: List[Dict[str, Any]] = []
    workspaces = get_workspaces()

    for workspace in workspaces:
        workspace_enabled = bool(workspace.get("enabled", True))
        if not workspace_enabled:
            continue

        workspace_id = workspace.get("id")
        workspace_name = workspace.get("name", "")
        organisations = workspace.get("organisations", [])
        org_id = organisations[0].get("id", "") if organisations else ""
        org_name = organisations[0].get("name", "") if organisations else ""

        dataflows = get_dataflows(workspace_id)
        print(f"\nWorkspace '{workspace_name}' (ID: {workspace_id}) - {len(dataflows)} dataflows")

        for dataflow in dataflows:
            dataflow_uuid = dataflow.get("uuid", "")
            dataflow_name = dataflow.get("name", "")
            dataflow_status = dataflow.get("status", {})
            state = dataflow_status.get("state", "") if isinstance(dataflow_status, dict) else str(dataflow_status)

            # Only keep dataflows whose state is Live (case-insensitive).
            if str(state).lower() != "live":
                continue

            details = get_dataflow_details(workspace_id, dataflow_uuid)
            if not details:
                continue

            block_sequence = extract_block_sequence(details)
            dataflow_url = f"https://uscrm.connectplus.capillarytech.com/connect/ui/summary/{dataflow_uuid}"

            # Log a quick human-readable sequence.
            readable = " -> ".join(
                f"{b.get('order') if b.get('order') is not None else '?'}:{b.get('type')}" for b in block_sequence
            )
            print(f"  Live dataflow: {dataflow_name} ({dataflow_uuid})")
            print(f"    Blocks: {readable}")

            output_records.append({
                "workspaceId": workspace_id,
                "workspaceName": workspace_name,
                "workspaceEnabled": workspace_enabled,
                "orgId": org_id,
                "orgName": org_name,
                "dataflowName": dataflow_name,
                "dataflowUUID": dataflow_uuid,
                "dataflowStatus": state,
                "dataflowUrl": dataflow_url,
                "blocks": block_sequence,
            })

    return output_records


def write_jsonl(records: List[Dict[str, Any]], output_path: str) -> None:
    """Write records to a JSONL file for easy consumption by analysis tools/LLMs."""
    if not records:
        print("No records to write.")
        return

    with open(output_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False))
            fh.write("\n")

    print(f"\n✓ Wrote {len(records)} records to {output_path}")


def main() -> None:
    print("=" * 60)
    print("Dataflow Block Sequence Exporter")
    print("=" * 60)
    records = process()
    write_jsonl(records, "us_dataflow_block_sequences.jsonl")
    print("\nDone.")


if __name__ == "__main__":
    main()

