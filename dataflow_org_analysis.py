#!/usr/bin/env python3

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List
from urllib.parse import quote

import requests


# =========================
# CONFIG
# =========================

ORG_ID = 2000091

BASE_URL = "https://uscrm.connectplus.capillarytech.com/api"

AUTH_HEADER = os.getenv(
    "CONNECTPLUS_AUTH_HEADER",
    "Basic YXV0b21hdGlvbl91c2VyOks1MjFyITMzYQ==",
)

NR_ACCOUNT_ID = os.getenv("NR_ACCOUNT_ID", "67421")

NR_QUERY_KEY = os.getenv(
    "NR_QUERY_KEY",
    "NRIQ-1kzjhqj-9OCR2Rmw_SAAcOsOROuvXU99",
)

NR_BASE_URL = "https://insights-api.newrelic.com/v1/accounts"

LOOKBACK_DAYS = 30


# =========================
# HELPERS
# =========================

def escape_nrql(v):
    return str(v).replace("'", "\\'")


def run_nrql(query):

    url = (
        f"{NR_BASE_URL}/{NR_ACCOUNT_ID}"
        f"/query?nrql={quote(query, safe='')}"
    )

    headers = {
        "Accept": "application/json",
        "X-Query-Key": NR_QUERY_KEY,
    }

    r = requests.get(url, headers=headers, timeout=30)

    r.raise_for_status()

    return r.json()


def human_duration(ms):

    if not ms:
        return "0s"

    sec = ms / 1000

    if sec < 60:
        return f"{sec:.1f}s"

    m = sec / 60

    if m < 60:
        return f"{m:.1f}m"

    h = m / 60

    return f"{h:.2f}h"


def avg_frequency(lineages):

    if len(lineages) < 2:
        return 0

    starts = sorted(
        [l["startMs"] for l in lineages if l["startMs"]],
        reverse=True,
    )

    diffs = []

    for i in range(len(starts) - 1):
        diffs.append(starts[i] - starts[i + 1])

    if not diffs:
        return 0

    return sum(diffs) / len(diffs)


# =========================
# CONNECTPLUS API
# =========================

def get_workspaces():

    url = f"{BASE_URL}/workspaces"

    headers = {
        "Authorization": AUTH_HEADER,
        "Content-Type": "application/json",
    }

    r = requests.get(url, headers=headers, timeout=30)

    r.raise_for_status()

    return r.json()


def get_dataflows(ws_id):

    url = f"{BASE_URL}/workspaces/{ws_id}/dataflows"

    headers = {
        "Authorization": AUTH_HEADER,
        "Content-Type": "application/json",
    }

    r = requests.get(url, headers=headers, timeout=30)

    r.raise_for_status()

    return r.json()


# =========================
# NEWRELIC
# =========================

def get_lineage_stats(dataflow_uuid):

    nrql = (
        f"SELECT lineageId, CreateCount, failureCount, retryCount, "
        f"lineageStartTime, timestamp "
        f"FROM ErrorFileGeneration "
        f"WHERE dataflowUuid = '{escape_nrql(dataflow_uuid)}' "
        f"SINCE {LOOKBACK_DAYS} days ago LIMIT 2000"
    )

    print("    NR:", dataflow_uuid[:8])

    response = run_nrql(nrql)

    results = response.get("results") or []

    events = []

    for r in results:
        if "events" in r:
            events.extend(r["events"])

    by_lineage = {}

    for ev in events:

        lid = ev.get("lineageId")

        if not lid:
            continue

        ts = ev.get("timestamp", 0)

        ex = by_lineage.get(lid)

        if ex is None or ts > ex.get("timestamp", 0):
            by_lineage[lid] = ev

    lineages = []

    for lid, ev in by_lineage.items():

        create = ev.get("CreateCount", 0)
        failure = ev.get("failureCount", 0)
        retry = ev.get("retryCount", 0)

        start = ev.get("lineageStartTime", 0)
        end = ev.get("timestamp", 0)

        duration = end - start if start and end else 0

        success = max(
            0,
            int(create)
            - int(failure)
            - int(retry),
        )

        lineages.append(
            {
                "lineageId": lid,
                "startMs": start,
                "endMs": end,
                "durationMs": duration,
                "createCount": int(create),
                "failureCount": int(failure),
                "retryCount": int(retry),
                "successCount": success,
            }
        )

    return lineages


# =========================
# REPORT
# =========================

def build_report():

    workspaces = get_workspaces()

    target_ws = []

    for ws in workspaces:

        for org in ws.get("organisations", []):

            if str(org.get("id")) == str(ORG_ID):
                target_ws.append(ws)

    report_ws = []

    for ws in target_ws:

        ws_id = ws["id"]
        ws_name = ws["name"]

        print("\nWorkspace:", ws_name)

        dfs = get_dataflows(ws_id)

        live = [
            d
            for d in dfs
            if str(
                (d.get("status") or {}).get("state")
            ).lower()
            == "live"
        ]

        report_dfs = []

        for df in live:

            name = df.get("name")
            uuid = df.get("uuid")

            print("  ->", name)

            lineages = get_lineage_stats(uuid)

            total_create = sum(
                l["createCount"] for l in lineages
            )

            total_failure = sum(
                l["failureCount"] for l in lineages
            )

            total_retry = sum(
                l["retryCount"] for l in lineages
            )

            total_success = sum(
                l["successCount"] for l in lineages
            )

            durations = [
                l["durationMs"]
                for l in lineages
                if l["durationMs"]
            ]

            avg_duration = (
                sum(durations) / len(durations)
                if durations
                else 0
            )

            max_duration = max(durations) if durations else 0
            min_duration = min(durations) if durations else 0

            freq = avg_frequency(lineages)

            report_dfs.append(
                {
                    "dataflowName": name,
                    "dataflowUuid": uuid,
                    "summary": {

                        "runs": len(lineages),

                        "total": total_create,
                        "success": total_success,
                        "failure": total_failure,
                        "retry": total_retry,

                        "avgRunDuration": human_duration(avg_duration),
                        "maxRunDuration": human_duration(max_duration),
                        "minRunDuration": human_duration(min_duration),

                        "avgFrequency": human_duration(freq),

                    },
                }
            )

        report_ws.append(
            {
                "workspaceName": ws_name,
                "dataflows": report_dfs,
            }
        )

    return {
        "orgId": ORG_ID,
        "workspaces": report_ws,
    }


# =========================

def main():

    report = build_report()

    with open(
        "dataflow_lineage_report.json",
        "w",
    ) as f:

        json.dump(report, f, indent=2)

    print("\nDONE")


if __name__ == "__main__":
    main()