#!/usr/bin/env python3
"""
Summarize unique block types per cluster across enabled workspaces and valid dataflows.

Workflow:
1) Fetch enabled workspaces from the workspaces API.
2) For each enabled workspace, fetch its dataflows.
3) Keep only dataflows where both invalidCount and disabledCount are zero.
4) Fetch each qualifying dataflow's details and collect blockTypeId values.
5) Aggregate unique blockTypeId values per cluster (from the workspace organisations list).

Auth: Uses the provided Basic auth header.
Base URL: https://uscrm.connectplus.capillarytech.com/api
"""

from __future__ import annotations

import sys
from typing import Any, Dict, Iterable, List, Optional, Set

import requests

BASE_URL = "https://eucrm.connectplus.capillarytech.com/api"
AUTH_HEADER = "Basic YXV0b21hdGlvbl91c2VyOks1MjFyITMzYQ=="


def fetch_json(url: str) -> Any:
    """GET JSON with shared headers."""
    headers = {
        "Authorization": AUTH_HEADER,
        "Content-Type": "application/json",
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_enabled_workspaces() -> List[Dict[str, Any]]:
    """Return only enabled workspaces."""
    url = f"{BASE_URL}/workspaces"
    workspaces = fetch_json(url)
    return [ws for ws in workspaces if ws.get("enabled") is True]


def get_valid_dataflows(workspace_id: int) -> List[Dict[str, Any]]:
    """Return dataflows with invalidCount == 0 and disabledCount == 0."""
    url = f"{BASE_URL}/workspaces/{workspace_id}/dataflows"
    dataflows = fetch_json(url)
    valid: List[Dict[str, Any]] = []
    for df in dataflows:
        status = df.get("status", {}) or {}
        if not isinstance(status, dict):
            continue
        if status.get("invalidCount") == 0 and status.get("disabledCount") == 0:
            valid.append(df)
    return valid


def get_dataflow_details(workspace_id: int, dataflow_uuid: str) -> Optional[Dict[str, Any]]:
    """Fetch a dataflow's details."""
    url = f"{BASE_URL}/workspaces/{workspace_id}/dataflows/{dataflow_uuid}"
    try:
        return fetch_json(url)
    except requests.exceptions.RequestException as exc:  # pragma: no cover - network
        print(f"[warn] failed to fetch dataflow {dataflow_uuid} in workspace {workspace_id}: {exc}")
        return None


def clusters_for_workspace(workspace: Dict[str, Any]) -> Set[str]:
    """Extract cluster values from workspace organisations (fallback to 'unknown')."""
    orgs = workspace.get("organisations") or []
    clusters = {org.get("cluster", "unknown") for org in orgs if isinstance(org, dict)}
    return clusters or {"unknown"}


def block_ids_from_dataflow(details: Dict[str, Any]) -> Set[int]:
    """Collect blockTypeId values from a dataflow detail payload."""
    blocks = details.get("blocks") or []
    ids: Set[int] = set()
    for block in blocks:
        block_id = block.get("blockTypeId")
        if isinstance(block_id, int):
            ids.add(block_id)
    return ids


def summarize() -> Dict[str, List[int]]:
    """Aggregate unique blockTypeId values per cluster."""
    summary: Dict[str, Set[int]] = {}

    enabled_workspaces = get_enabled_workspaces()
    print(f"Found {len(enabled_workspaces)} enabled workspaces")

    for ws in enabled_workspaces:
        ws_id = ws.get("id")
        ws_name = ws.get("name", "")
        if ws_id is None:
            continue

        clusters = clusters_for_workspace(ws)
        dataflows = get_valid_dataflows(ws_id)
        print(f"- Workspace {ws_name} (ID: {ws_id}) | clusters={','.join(clusters)} | valid dataflows={len(dataflows)}")

        for df in dataflows:
            df_uuid = df.get("uuid")
            if not df_uuid:
                continue
            details = get_dataflow_details(ws_id, df_uuid)
            if not details:
                continue

            block_ids = block_ids_from_dataflow(details)
            for cluster in clusters:
                summary.setdefault(cluster, set()).update(block_ids)

    # Convert sets to sorted lists for presentation/serialization
    return {cluster: sorted(ids) for cluster, ids in summary.items()}


def main() -> None:
    try:
        cluster_blocks = summarize()
    except requests.exceptions.RequestException as exc:  # pragma: no cover - network
        print(f"[error] request failed: {exc}")
        sys.exit(1)

    if not cluster_blocks:
        print("No data found (no enabled workspaces or no valid dataflows).")
        return

    print("\nCluster-level unique blockTypeId usage:")
    for cluster, ids in cluster_blocks.items():
        print(f"  - cluster={cluster}: {len(ids)} unique block(s) -> {ids}")


if __name__ == "__main__":
    main()

