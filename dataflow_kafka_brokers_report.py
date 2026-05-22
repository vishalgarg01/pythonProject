#!/usr/bin/env python3
"""
Collect unique Kafka brokers used across all enabled workspaces and their dataflows.

Steps:
1) Fetch all workspaces and keep only the enabled ones.
2) For each enabled workspace, fetch its dataflows.
3) For each dataflow, fetch its detail and inspect blocks.
4) If a block's type contains 'kafka' (case-insensitive), find the field
   whose name == 'brokers' and capture its value.
5) Aggregate unique broker values across all workspaces/dataflows and print
   both a per-occurrence breakdown and the global unique list.
"""

from __future__ import annotations

import csv
import os
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

BASE_URL = "https://ushccrm.connectplus.capillarytech.com/api"
AUTH_HEADER = os.getenv("CONNECTPLUS_AUTH_HEADER", "Basic YXV0b21hdGlvbl91c2VyOks1MjFyITMzYQ==")

OUTPUT_CSV = "dataflow_kafka_brokers_report.csv"


def _headers() -> Dict[str, str]:
    return {
        "Authorization": AUTH_HEADER,
        "Content-Type": "application/json",
    }


def fetch_json(url: str) -> Any:
    resp = requests.get(url, headers=_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_enabled_workspaces() -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/workspaces"
    print(f"Fetching workspaces from {url}...")
    try:
        workspaces = fetch_json(url)
    except requests.RequestException as exc:
        print(f"Error fetching workspaces: {exc}")
        sys.exit(1)
    return [ws for ws in workspaces if ws.get("enabled") is True]


def get_dataflows(workspace_id: int) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/workspaces/{workspace_id}/dataflows"
    try:
        return fetch_json(url)
    except requests.RequestException as exc:
        print(f"  Error fetching dataflows for workspace {workspace_id}: {exc}")
        return []


def get_dataflow_detail(workspace_id: int, dataflow_uuid: str) -> Optional[Dict[str, Any]]:
    url = f"{BASE_URL}/workspaces/{workspace_id}/dataflows/{dataflow_uuid}"
    try:
        return fetch_json(url)
    except requests.RequestException as exc:
        print(f"  Error fetching dataflow {dataflow_uuid}: {exc}")
        return None


def is_kafka_block(block: Dict[str, Any]) -> bool:
    """Return True if any identifying attribute of the block mentions 'kafka'."""
    for key in ("type", "name", "blockType", "blockTypeName"):
        val = block.get(key)
        if isinstance(val, str) and "kafka" in val.lower():
            return True
    return False


def extract_brokers_value(block: Dict[str, Any]) -> Optional[str]:
    """
    Find the field named 'brokers' (case-insensitive) inside the block and return its value.
    Looks in block['fields'] (list of {name, value, displayName, ...}), then
    falls back to block['config'] / block['properties'] / top-level keys.
    """
    fields = block.get("fields") or []
    if isinstance(fields, list):
        for field in fields:
            if not isinstance(field, dict):
                continue
            name = field.get("name") or field.get("displayName") or ""
            if isinstance(name, str) and name.strip().lower() == "brokers":
                value = field.get("value")
                if value is not None and value != "":
                    return str(value)

    for key in ("config", "properties"):
        mapping = block.get(key) or {}
        if isinstance(mapping, dict):
            for k, v in mapping.items():
                if isinstance(k, str) and k.strip().lower() == "brokers" and v not in (None, ""):
                    return str(v)

    if "brokers" in block and block["brokers"] not in (None, ""):
        return str(block["brokers"])

    return None


def process() -> Tuple[List[Dict[str, Any]], Set[str]]:
    """Return (per-occurrence rows, set of unique brokers)."""
    rows: List[Dict[str, Any]] = []
    unique_brokers: Set[str] = set()

    workspaces = get_enabled_workspaces()
    print(f"Found {len(workspaces)} enabled workspaces\n")

    for ws in workspaces:
        ws_id = ws.get("id")
        ws_name = ws.get("name", "")
        if ws_id is None:
            continue

        organisations = ws.get("organisations") or []
        org_id = organisations[0].get("id", "") if organisations else ""
        org_name = organisations[0].get("name", "") if organisations else ""

        dataflows = get_dataflows(ws_id)
        print(f"- Workspace: {ws_name} (ID: {ws_id}) | dataflows={len(dataflows)}")

        for df in dataflows:
            df_uuid = df.get("uuid") or df.get("dataflowUuid") or df.get("id")
            df_name = df.get("name", "")
            if not df_uuid:
                continue

            detail = get_dataflow_detail(ws_id, df_uuid)
            if not detail:
                continue

            blocks = detail.get("blocks") or []
            if not isinstance(blocks, list):
                continue

            for block in blocks:
                if not isinstance(block, dict):
                    continue
                if not is_kafka_block(block):
                    continue

                brokers = extract_brokers_value(block)
                block_type = block.get("type") or block.get("name") or ""
                if not brokers:
                    print(f"    [kafka block, no brokers] {df_name} / {block_type}")
                    continue

                print(f"    [kafka] {df_name} / {block_type} -> {brokers}")
                unique_brokers.add(brokers)
                rows.append({
                    "workspaceId": ws_id,
                    "workspaceName": ws_name,
                    "orgId": org_id,
                    "orgName": org_name,
                    "dataflowId": df_uuid,
                    "dataflowName": df_name,
                    "blockType": block_type,
                    "brokers": brokers,
                })

    return rows, unique_brokers


def write_csv(rows: List[Dict[str, Any]], output_file: str) -> None:
    fieldnames = [
        "workspaceId",
        "workspaceName",
        "orgId",
        "orgName",
        "dataflowId",
        "dataflowName",
        "blockType",
        "brokers",
    ]
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n✓ Wrote {len(rows)} rows to {output_file}")


def main() -> None:
    print("=" * 70)
    print("Dataflow Kafka Brokers Report")
    print("=" * 70)

    rows, unique_brokers = process()

    write_csv(rows, OUTPUT_CSV)

    print("\n" + "=" * 70)
    print(f"Unique Kafka broker values across all workspaces/dataflows: {len(unique_brokers)}")
    print("=" * 70)
    for broker in sorted(unique_brokers):
        print(f"  - {broker}")


if __name__ == "__main__":
    main()
