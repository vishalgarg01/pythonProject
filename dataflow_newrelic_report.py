#!/usr/bin/env python3
"""
Generate a CSV report of live dataflows with New Relic stats.

Steps:
1) Fetch enabled workspaces.
2) For each workspace, fetch live dataflows.
3) For each live dataflow, run New Relic NRQL queries to gather:
   - Total files processed (unique lineageIds) in the last 50 days.
   - Total size processed in MB in the last 50 days.
   - Last processed time (latest lineageStartTime) and size for that lineage.
4) Write results to CSV.
"""

import csv
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests

# ConnectPlus API configuration
BASE_URL = "https://eucrm.connectplus.capillarytech.com/api"
# Optionally override via env vars if needed
AUTH_HEADER = os.getenv("CONNECTPLUS_AUTH_HEADER", "Basic YXV0b21hdGlvbl91c2VyOks1MjFyITMzYQ==")

# New Relic configuration
NR_ACCOUNT_ID = os.getenv("NR_ACCOUNT_ID", "67421")
NR_QUERY_KEY = os.getenv("NR_QUERY_KEY", "NRIQ-1kzjhqj-9OCR2Rmw_SAAcOsOROuvXU99")  # required; no default to avoid hardcoding secrets
NR_BASE_URL = "https://insights-api.newrelic.com/v1/accounts"

# Dataflow environment (used in NRQL filters)
ENVIRONMENT = os.getenv("DATAFLOW_ENV", "eucrm")


def _require_nr_key():
    """Exit early if the NR query key is not provided."""
    if not NR_QUERY_KEY:
        print("Error: NR_QUERY_KEY environment variable is not set.")
        sys.exit(1)


def escape_nrql_literal(value: Any) -> str:
    """Safely escape single quotes for NRQL string literals."""
    if value is None:
        return ""
    return str(value).replace("'", "\\'")


def run_nrql(query: str) -> Dict[str, Any]:
    """Execute an NRQL query and return the parsed JSON response."""
    _require_nr_key()
    encoded_query = quote(query, safe="")
    url = f"{NR_BASE_URL}/{NR_ACCOUNT_ID}/query?nrql={encoded_query}"
    headers = {
        "Accept": "application/json",
        "X-Query-Key": NR_QUERY_KEY,
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        print(f"Error running NRQL: {exc}\nQuery: {query}")
        return {}


def extract_first_numeric(results: List[Dict[str, Any]]) -> Optional[float]:
    """Return the first numeric value found in a NRQL results list."""
    for result in results or []:
        if isinstance(result, dict):
            for val in result.values():
                if isinstance(val, (int, float)):
                    return float(val)
    return None


def extract_latest_facet(response: Dict[str, Any]) -> Tuple[Optional[str], Optional[float]]:
    """
    From a facet NRQL response, return (facet_name, latest_value).
    Expected shape:
        { "facets": [ { "name": "<facet>", "results": [ { "latest...": <value> } ] } ] }
    """
    facets = response.get("facets") or []
    if not facets:
        return None, None

    facet = facets[0]
    lineage_id = facet.get("name")
    latest_value = None
    results = facet.get("results") or []
    if results and isinstance(results[0], dict):
        for val in results[0].values():
            if isinstance(val, (int, float)):
                latest_value = float(val)
                break
    return lineage_id, latest_value


def get_workspaces() -> List[Dict[str, Any]]:
    """Fetch all workspaces."""
    url = f"{BASE_URL}/workspaces"
    headers = {
        "Authorization": AUTH_HEADER,
        "Content-Type": "application/json",
    }
    try:
        print(f"Fetching workspaces from {url}...")
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        print(f"Error fetching workspaces: {exc}")
        sys.exit(1)


def get_dataflows(workspace_id: int) -> List[Dict[str, Any]]:
    """Fetch dataflows for a workspace."""
    url = f"{BASE_URL}/workspaces/{workspace_id}/dataflows"
    headers = {
        "Authorization": AUTH_HEADER,
        "Content-Type": "application/json",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        print(f"Error fetching dataflows for workspace {workspace_id}: {exc}")
        return []


def get_total_files_processed(dataflow_uuid: str) -> Optional[float]:
    """Return unique lineageId count for the dataflow over the last 50 days."""
    nrql = (
        "SELECT uniqueCount(lineageId) FROM ErrorFileGeneration "
        f"WHERE dataflowUuid = '{escape_nrql_literal(dataflow_uuid)}' "
        "SINCE 50 days ago LIMIT MAX"
    )
    print(f"running newrelic query {nrql}")

    response = run_nrql(nrql)
    return extract_first_numeric(response.get("results", []))


def get_total_size_mb(dataflow_name: str) -> Optional[float]:
    """Return total processed size in MB for the dataflow over the last 50 days."""
    nrql = (
        "FROM ConnectPlusEvents "
        "SELECT sum(size) / 1024 / 1024 AS 'fileSize (in MB)' "
        "WHERE processorType = 'FetchSFTP' "
        "AND provenanceEventType = 'ROUTE' "
        f"AND environment = '{escape_nrql_literal(ENVIRONMENT)}' "
        f"AND dataflow = '{escape_nrql_literal(dataflow_name)}' "
        "SINCE 50 days ago LIMIT MAX"
    )
    print(f"running newrelic query {nrql}")
    response = run_nrql(nrql)
    return extract_first_numeric(response.get("results", []))


def get_last_processed_info(dataflow_uuid: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (lineageId, ISO timestamp) for the most recent file processed."""
    nrql = (
        "SELECT latest(lineageStartTime) "
        "FROM ErrorFileGeneration "
        "WHERE appName = 'eucrm-glue' "
        f"AND dataflowUuid = '{escape_nrql_literal(dataflow_uuid)}' "
        "FACET lineageId SINCE 50 days ago LIMIT 1 "
    )
    print(f"running newrelic query {nrql}")

    response = run_nrql(nrql)
    lineage_id, latest_value = extract_latest_facet(response)
    if latest_value is None:
        return None, None

    # lineageStartTime is likely epoch millis; convert cautiously
    if latest_value > 10_000_000_000:  # assume millis
        dt = datetime.fromtimestamp(latest_value / 1000, tz=timezone.utc)
    else:
        dt = datetime.fromtimestamp(latest_value, tz=timezone.utc)

    return lineage_id, dt.isoformat()


def get_last_file_size_mb(lineage_id: str) -> Optional[float]:
    """Return size in MB for the last processed lineage."""
    nrql = (
        "FROM ConnectPlusEvents "
        "SELECT sum(size) / 1024 / 1024 AS 'fileSize (in MB)' "
        "WHERE processorType = 'FetchSFTP' "
        "AND provenanceEventType = 'ROUTE' "
        f"AND environment = '{escape_nrql_literal(ENVIRONMENT)}' "
        f"AND (`x-cap-lineage-id` = '{escape_nrql_literal(lineage_id)}' "
        f"OR `previous_x-cap-lineage-id` = '{escape_nrql_literal(lineage_id)}') "
        "LIMIT MAX SINCE 50 days ago"
    )
    print(f"running newrelic query {nrql}")
    response = run_nrql(nrql)
    return extract_first_numeric(response.get("results", []))


def fetch_dataflow_blocks(workspace_id: int, dataflow_uuid: str, fallback: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """
    Fetch the detailed dataflow (to get blocks). If the detail call fails,
    return the provided fallback list or an empty list.
    """
    url = f"{BASE_URL}/workspaces/{workspace_id}/dataflows/{dataflow_uuid}"
    headers = {
        "Authorization": AUTH_HEADER,
        "Content-Type": "application/json",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        dataflow = resp.json()
        blocks = dataflow.get("blocks") or []
        return blocks if isinstance(blocks, list) else []
    except requests.RequestException:
        return fallback or []


def build_block_chain(blocks: List[Dict[str, Any]]) -> str:
    """
    Build an ordered string of block types, e.g., 'sftp_push -> jsonTransform -> oauthApi'.
    Orders by the 'order' field and joins the 'type' values (falling back to 'name').
    """
    if not blocks:
        return ""
    sorted_blocks = sorted(blocks, key=lambda b: b.get("order", 0))
    types: List[str] = []
    for block in sorted_blocks:
        block_type = block.get("type") or block.get("name") or ""
        if block_type:
            types.append(str(block_type))
    return " -> ".join(types)


def process_dataflows() -> List[Dict[str, Any]]:
    """Process enabled workspaces and live dataflows, returning report rows."""
    records: List[Dict[str, Any]] = []
    workspaces = get_workspaces()

    for workspace in workspaces:
        workspace_id = workspace.get("id")
        workspace_enabled = bool(workspace.get("enabled", True))
        if not workspace_enabled:
            continue

        workspace_name = workspace.get("name", "")
        organisations = workspace.get("organisations") or []
        org_id = organisations[0].get("id", "") if organisations else ""
        org_name = organisations[0].get("name", "") if organisations else ""

        dataflows = get_dataflows(workspace_id)
        print(f"\nWorkspace: {workspace_name} (ID: {workspace_id}) | Dataflows: {len(dataflows)}")

        for dataflow in dataflows:
            status = dataflow.get("status", {})
            state = status.get("state") if isinstance(status, dict) else status
            if str(state).lower() != "live":
                continue

            dataflow_name = dataflow.get("name", "")
            dataflow_uuid = (
                dataflow.get("uuid")
                or dataflow.get("dataflowUuid")
                or dataflow.get("id")
                or ""
            )

            if not dataflow_uuid:
                print(f"Skipping dataflow without UUID: {dataflow_name}")
                continue

            print(f"  Processing live dataflow: {dataflow_name} (UUID: {dataflow_uuid})")

            total_files = get_total_files_processed(dataflow_uuid) or 0
            total_size_mb = get_total_size_mb(dataflow_name) or 0.0
            lineage_id, last_processed_on = get_last_processed_info(dataflow_uuid)
            last_file_size_mb = get_last_file_size_mb(lineage_id) if lineage_id else None

            # Blocks: use embedded blocks if present; else fetch detail
            embedded_blocks = dataflow.get("blocks") if isinstance(dataflow, dict) else None
            blocks = fetch_dataflow_blocks(workspace_id, dataflow_uuid, fallback=embedded_blocks or [])
            block_chain = build_block_chain(blocks)

            records.append(
                {
                    "orgId": org_id,
                    "orgName": org_name,
                    "dataflowName": dataflow_name,
                    "lastProcessedOn": last_processed_on or "",
                    "lastFileSizeMB": round(last_file_size_mb, 2) if last_file_size_mb is not None else "",
                    "totalFilesProcessed": int(total_files) if total_files is not None else 0,
                    "totalSizeProcessedMB": round(total_size_mb, 2) if total_size_mb is not None else 0.0,
                    "dataflowId": dataflow_uuid,
                    "blockTypesOrdered": block_chain,
                }
            )

    return records


def write_csv(records: List[Dict[str, Any]], output_file: str) -> None:
    """Write report rows to CSV."""
    if not records:
        print("No records to write.")
        return

    fieldnames = [
        "orgId",
        "orgName",
        "dataflowName",
        "lastProcessedOn",
        "lastFileSizeMB",
        "totalFilesProcessed",
        "totalSizeProcessedMB",
        "dataflowId",
        "blockTypesOrdered",
    ]
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"\n✓ Wrote {len(records)} records to {output_file}")


def main():
    print("=" * 70)
    print("Live Dataflow New Relic Report")
    print("=" * 70)
    _require_nr_key()

    records = process_dataflows()
    output_file = "eucrm_dataflow_newrelic_report.csv"
    write_csv(records, output_file)


if __name__ == "__main__":
    main()
