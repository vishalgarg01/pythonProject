#!/usr/bin/env python3
"""
Report how many times each API endpoint is called across a set of dataflows.

Steps:
1) Fetch all workspaces and find the enabled one for the hardcoded orgId.
2) For each hardcoded dataflowId, fetch the full dataflow detail (blocks).
3) Find the last block (highest blockOrder).
4) In that block, look for a field named "API EndPoint".
5) Count how many dataflows reference each unique endpoint value.
"""

import os
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional

import requests

# ── Hardcoded configuration ────────────────────────────────────────────────
ORG_ID = 2000039  # <-- replace with your actual orgId (int or str)

DATAFLOW_IDS = (
    "7c41553f-0196-1000-ffff-ffffacde0b23"
)  # comma-separated UUIDs
# ──────────────────────────────────────────────────────────────────────────

BASE_URL = "https://uscrm.connectplus.capillarytech.com/api"
AUTH_HEADER = os.getenv("CONNECTPLUS_AUTH_HEADER", "Basic YXV0b21hdGlvbl91c2VyOks1MjFyITMzYQ==")


def _headers() -> Dict[str, str]:
    return {
        "Authorization": AUTH_HEADER,
        "Content-Type": "application/json",
    }


def get_workspaces() -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/workspaces"
    try:
        print(f"Fetching workspaces from {url}...")
        resp = requests.get(url, headers=_headers(), timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        print(f"Error fetching workspaces: {exc}")
        sys.exit(1)


def find_workspace_for_org(org_id: Any) -> Optional[Dict[str, Any]]:
    """Return the first enabled workspace that belongs to org_id."""
    workspaces = get_workspaces()
    str_org_id = str(org_id)
    for ws in workspaces:
        if not ws.get("enabled", True):
            continue
        organisations = ws.get("organisations") or []
        for org in organisations:
            if str(org.get("id", "")) == str_org_id:
                print(f"Found enabled workspace '{ws.get('name')}' (id={ws.get('id')}) for orgId={org_id}")
                return ws
    return None


def fetch_dataflow_blocks(workspace_id: int, dataflow_uuid: str) -> List[Dict[str, Any]]:
    """Fetch the full dataflow detail and return its blocks list."""
    url = f"{BASE_URL}/workspaces/{workspace_id}/dataflows/{dataflow_uuid}"
    try:
        resp = requests.get(url, headers=_headers(), timeout=30)
        resp.raise_for_status()
        dataflow = resp.json()
        blocks = dataflow.get("blocks") or []
        return blocks if isinstance(blocks, list) else []
    except requests.RequestException as exc:
        print(f"  Error fetching dataflow {dataflow_uuid}: {exc}")
        return []


def get_last_block(blocks: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return the block with the highest blockOrder value."""
    if not blocks:
        return None
    return max(blocks, key=lambda b: b.get("blockOrder", b.get("order", 0)))


def extract_api_endpoint(block: Dict[str, Any]) -> Optional[str]:
    """
    Look for a field named 'API EndPoint' inside a block.

    Blocks may store their configuration in different shapes:
      - block["fields"] as a list of {"name": ..., "value": ...} dicts
      - block["config"] as a flat dict
      - block["properties"] as a flat dict
    All three are checked.
    """
    # Shape 1: fields list
    fields = block.get("fields") or []
    if isinstance(fields, list):
        for field in fields:
            if isinstance(field, dict) and field.get("displayName") == "API EndPoint":
                return field.get("value")

    # Shape 2: config / properties flat dict
    for key in ("config", "properties"):
        mapping = block.get(key) or {}
        if isinstance(mapping, dict) and "API EndPoint" in mapping:
            return mapping["API EndPoint"]

    # Shape 3: top-level key directly on the block
    if "API EndPoint" in block:
        return block["API EndPoint"]

    return None


def main():
    print("=" * 70)
    print("Dataflow API Endpoint Usage Report")
    print("=" * 70)

    # Parse the comma-separated dataflow IDs
    dataflow_ids = [d.strip() for d in DATAFLOW_IDS.split(",") if d.strip()]
    print(f"OrgId          : {ORG_ID}")
    print(f"Dataflow count : {len(dataflow_ids)}")
    print()

    # Step 1 – find the workspace for our org
    workspace = find_workspace_for_org(ORG_ID)
    if not workspace:
        print(f"No enabled workspace found for orgId={ORG_ID}. Exiting.")
        sys.exit(1)

    workspace_id = workspace["id"]

    # Step 2 – process each dataflow
    endpoint_counts: Dict[str, int] = defaultdict(int)
    no_endpoint: List[str] = []

    for dataflow_uuid in dataflow_ids:
        print(f"Processing dataflow: {dataflow_uuid}")
        blocks = fetch_dataflow_blocks(workspace_id, dataflow_uuid)

        if not blocks:
            print(f"  No blocks found – skipping.")
            no_endpoint.append(dataflow_uuid)
            continue

        last_block = get_last_block(blocks)
        if last_block is None:
            print(f"  Could not determine last block – skipping.")
            no_endpoint.append(dataflow_uuid)
            continue

        block_order = last_block.get("blockOrder", last_block.get("order", "?"))
        print(f"  Last block blockOrder={block_order}, type={last_block.get('type', last_block.get('name', '?'))}")

        endpoint = extract_api_endpoint(last_block)
        if endpoint:
            endpoint_counts[endpoint] += 1
            print(f"  API EndPoint: {endpoint}")
        else:
            print(f"  'API EndPoint' field not found in last block.")
            no_endpoint.append(dataflow_uuid)

    # Step 3 – print the report
    print()
    print("=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"Unique API endpoints called : {len(endpoint_counts)}")
    print()

    if endpoint_counts:
        # Sort by count descending
        sorted_endpoints = sorted(endpoint_counts.items(), key=lambda x: x[1], reverse=True)
        col_w = max(len(ep) for ep, _ in sorted_endpoints)
        print(f"{'API Endpoint':<{col_w}}  Count")
        print("-" * (col_w + 8))
        for endpoint, count in sorted_endpoints:
            print(f"{endpoint:<{col_w}}  {count}")
    else:
        print("No API endpoints found.")

    if no_endpoint:
        print()
        print(f"Dataflows with no 'API EndPoint' field ({len(no_endpoint)}):")
        for df_id in no_endpoint:
            print(f"  - {df_id}")


if __name__ == "__main__":
    main()
