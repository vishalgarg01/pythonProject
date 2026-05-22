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

import csv
import os
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional

import requests

# ── Hardcoded configuration ────────────────────────────────────────────────
ORG_ID = 2000034  # <-- replace with your actual orgId (int or str)

DATAFLOW_IDS = (
"48a2ee78-019c-1000-ffff-ffff8648e26b,736e225e-0198-1000-ffff-ffffda6ae4f1,79d4a279-0198-1000-0000-00007a4fd457,2eb514a1-e845-373c-92a5-093b676d150c,2eb514a1-e845-373c-b3f6-5e92fac0d706,825565ba-0198-1000-ffff-ffffeddd9a46,59b1d26e-32b2-3bf5-b550-495dcadf00f7,59b1d26e-32b2-3bf5-a7f4-e3c49c036d04,59b1d26e-32b2-3bf5-b033-9e1631d81ffa,0742eeb5-0198-1000-0000-0000518f5828,f8f0cf2e-55e5-36e8-9c5f-e0bbd2cefe16,252d86d8-69bf-3043-b2fa-e4309fedf2af,f8f0cf2e-55e5-36e8-8f8d-c492a09db0a4,07670f8b-0198-1000-0000-000038e095c7,074b1d49-0198-1000-0000-000075b6369a,076c5f63-0198-1000-ffff-ffffbad36479,0739a359-0198-1000-ffff-ffff9d4eada3,f8f0cf2e-55e5-36e8-a0b8-0c780363886d,c8746cea-6ec9-355e-a214-98bb89fc491d,72899b27-fbf5-39ab-a5df-be25d34a50d4,ec99eeac-0199-1000-0000-00007183d1c2,f8f0cf2e-55e5-36e8-b83a-7d910e6da429,59b1d26e-32b2-3bf5-abd3-b4f621654d49,59b1d26e-32b2-3bf5-90ba-c8716b8fb5bc,59b1d26e-32b2-3bf5-8978-274d104ad042,59b1d26e-32b2-3bf5-b4ec-986fa6616fb0,59b1d26e-32b2-3bf5-b151-4d23c72b6e78,59b1d26e-32b2-3bf5-ae13-7d67f78e3c34,59b1d26e-32b2-3bf5-ab48-a9ebdc113983,59b1d26e-32b2-3bf5-b6e8-505ee6dec1f4,59b1d26e-32b2-3bf5-b839-67f33a166de8,59b1d26e-32b2-3bf5-b1ad-de8dbb3bd632,59b1d26e-32b2-3bf5-88ed-7161fa1718bf,59b1d26e-32b2-3bf5-a2d2-0477214419af,59b1d26e-32b2-3bf5-9a5b-6e6874b6d153,59b1d26e-32b2-3bf5-b22b-dd593b4ba5a6,59b1d26e-32b2-3bf5-a0d1-6d88e61174e9,59b1d26e-32b2-3bf5-b0cb-2805d9c31daa,074f8a7d-0198-1000-0000-00005432b689,59b1d26e-32b2-3bf5-b257-caa7f8303e9e,59b1d26e-32b2-3bf5-82c3-4c350fc7154d,59b1d26e-32b2-3bf5-aca2-859d61a8c6c8,277dbdca-019b-1000-0000-00006fecdb7e,59b1d26e-32b2-3bf5-b2b5-8218c5294bfd,59b1d26e-32b2-3bf5-8771-5be88caa6be9,59b1d26e-32b2-3bf5-b28d-2cf626be1fe6,808f260f-0199-1000-0000-000012c69d8b,59b1d26e-32b2-3bf5-acdc-901633986b27,59b1d26e-32b2-3bf5-b8b7-76c83fa9523a,59b1d26e-32b2-3bf5-9be9-49b712658415,f8f0cf2e-55e5-36e8-be36-877a260eeae0,188e61a7-0197-1000-0000-00007b909000,1d254a23-0197-1000-ffff-fffffc66ae7d,b0ff614b-de53-3fb9-b31b-24d4828bf1ee,9468dc11-0199-1000-0000-0000336bb045,13763dcf-0197-1000-0000-0000475b5917,bd4f9da7-ac16-39b0-ae1e-19126a00b3f6,bd4f9da7-ac16-39b0-8bf2-7bdff8ca17ba,bd4f9da7-ac16-39b0-b89d-97a4690cd1a7,bd4f9da7-ac16-39b0-9868-c6c9da617945,bd4f9da7-ac16-39b0-9cd3-5aa79b120ef1,bd4f9da7-ac16-39b0-8e69-917255ab9b53,bd4f9da7-ac16-39b0-9f56-c647a43b3863,bd4f9da7-ac16-39b0-a730-e44f9cae2804,c8746cea-6ec9-355e-a8cd-083eab6d41a4,bd4f9da7-ac16-39b0-b642-ac3e431ddd34,fe46ccc5-0196-1000-0000-00003a7d43a2,3b4e9ffe-1a8d-34c6-b921-05c55475c0b3,6d7a8a7d-0196-1000-0000-00003fdec1c7,7b7671ca-0196-1000-ffff-ffff940bbea9,1d50f4f7-3d9a-3e9c-901b-8988c01ae366,7abda17d-0196-1000-ffff-ffffe926e258,3b4e9ffe-1a8d-34c6-9ab0-053004219752,6d45cb35-0196-1000-0000-00000a90e83d,6f267255-5750-3e98-af22-6c96cd7e1b22,3f8351fa-acdb-3184-a72a-b1fa17830139,bd4f9da7-ac16-39b0-9fe9-f56a5697c708,4def202e-68cd-367f-9f2c-f3df7b317588,ffb00a5b-35d8-3b7f-a6f5-f489c1f31168,e105cd30-d1a9-34eb-8fe2-cc62a3a11c26,bd294f78-bad8-35d1-af1a-25d3f8573ee7,f39b370c-08b1-33c9-bf21-38a38f6160c1,bd294f78-bad8-35d1-9259-b01ae85ec0db,bd294f78-bad8-35d1-a173-29eb95a70b8d,bd294f78-bad8-35d1-bfb9-4f173c485c44,bd294f78-bad8-35d1-b404-9671f3ade6ea,bd294f78-bad8-35d1-8866-b871e7d17a0e,bd294f78-bad8-35d1-b38e-23c62380ef75,bd294f78-bad8-35d1-b94a-bd2f4937e00f,bd294f78-bad8-35d1-8697-daa56097f8a7,bd294f78-bad8-35d1-b9c8-3fe1852cc2f0,0e789d9d-23ee-325a-ab4a-119a4326ed61,bd294f78-bad8-35d1-99b2-9a1258f1271e,bd294f78-bad8-35d1-b5d7-865674594dab,bd294f78-bad8-35d1-a8f4-62547a0bd517,bd294f78-bad8-35d1-a9a0-a1410d66dab7,19c4a962-e55a-30c3-8b45-bc4eaf6536e5,bd294f78-bad8-35d1-8ffd-41bfbca20d68,0e789d9d-23ee-325a-9363-f2f19a8016f9,4def202e-68cd-367f-a83e-98ab6f8ef1c3,bd4f9da7-ac16-39b0-8313-27bf254d6b2e,bd294f78-bad8-35d1-a161-5df4bb676e5b,bd294f78-bad8-35d1-89e5-c6f61d7d02f0,bd294f78-bad8-35d1-ab94-b1dce32da829,bd294f78-bad8-35d1-b184-b8a14f54c563,bd294f78-bad8-35d1-9038-054aedf16032,bd294f78-bad8-35d1-8336-62a637fc6a44,bd294f78-bad8-35d1-b096-d303730fde1e,bd294f78-bad8-35d1-a1fa-c00fca164fd1"
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

    # Step 3 – write CSV report
    output_file = f"{ORG_ID}_api_endpoint_report.csv"
    sorted_endpoints = sorted(endpoint_counts.items(), key=lambda x: x[1], reverse=True)

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["API Endpoint", "Count"])
        writer.writerows(sorted_endpoints)

    print()
    print("=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"Unique API endpoints called : {len(endpoint_counts)}")
    print(f"CSV written to             : {output_file}")
    print()

    for endpoint, count in sorted_endpoints:
        print(f"  {count:>4}  {endpoint}")

    if no_endpoint:
        print()
        print(f"Dataflows with no 'API EndPoint' field ({len(no_endpoint)}):")
        for df_id in no_endpoint:
            print(f"  - {df_id}")


if __name__ == "__main__":
    main()
