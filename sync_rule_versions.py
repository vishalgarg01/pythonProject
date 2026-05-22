#!/usr/bin/env python3
"""
Fire the connectplus rule-version sync API for a list of {ruleId, versionId}.

Input JSON (pass via --input, default: rule_versions.json):
[
  {"ruleId": "871e9cfb-354d-491a-ab11-74fab494810b",
   "versionId": "dab5bb2d-97ba-4510-b1a7-3b0d970d82ba"},
  ...
]

Cookies are read from a file (default: sync_cookies.txt). The file should
contain the raw `Cookie:` header value from the browser — everything after
`-b '...'` in the curl command, e.g.:

  csrftoken=...; CS=...; CC=...; federated_id_token=...; CT=...; OID=100610

Cookies expire; refresh by copying from DevTools when you get 401/403.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import requests


def set_oid(cookie: str, namespace: Any) -> str:
    if re.search(r"(^|;\s*)OID=", cookie):
        return re.sub(r"(^|;\s*)OID=[^;]*", lambda m: f"{m.group(1)}OID={namespace}", cookie)
    sep = "" if cookie.endswith(";") or not cookie else "; "
    return f"{cookie}{sep}OID={namespace}"

BASE = "https://ushc.intouch.capillarytech.com/extensions/neo/api/v1/xto6x"
REFERER_BASE = "https://ushc.intouch.capillarytech.com/extensions/connectplus/ui/app/qsIRI"


def build_headers(cookie: str, rule_id: str, version_id: str) -> Dict[str, str]:
    return {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "content-length": "0",
        "origin": "https://ushc.intouch.capillarytech.com",
        "referer": f"{REFERER_BASE}/rule/{rule_id}/version/{version_id}?ruleType=org",
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
        ),
        "x-cap-remote-user": "75149935",
        "cookie": cookie,
    }


def sync_one(session: requests.Session, cookie: str, rule_id: str, version_id: str, namespace: Any, timeout: int) -> int:
    url = f"{BASE}/rule/{rule_id}/version/{version_id}/sync"
    params = {"context": "connectplus", "time": str(int(time.time() * 1000))}
    headers = build_headers(set_oid(cookie, namespace), rule_id, version_id)
    resp = session.post(url, headers=headers, params=params, timeout=timeout)
    return resp.status_code


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", default="rulthee_versions.json", help="JSON file with list of {ruleId, versionId}")
    parser.add_argument("--cookies", default="sync_cookies.txt", help="File with the raw Cookie header value")
    parser.add_argument("--sleep", type=float, default=0.2, help="Seconds to sleep between calls")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    parser.add_argument("--dry-run", action="store_true", help="Print URLs that would be called; do not send")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    input_path = Path(args.input)
    if not input_path.exists():
        logging.error("input file not found: %s", input_path)
        return 2
    pairs: List[Dict[str, str]] = json.loads(input_path.read_text())

    cookie = ""
    if not args.dry_run:
        cookie_path = Path(args.cookies)
        if not cookie_path.exists():
            logging.error("cookie file not found: %s (paste the raw Cookie header value)", cookie_path)
            return 2
        cookie = cookie_path.read_text().strip()

    session = requests.Session()
    ok = fail = 0
    results: List[Dict[str, Any]] = []

    for i, entry in enumerate(pairs, start=1):
        rule_id = entry.get("ruleId") or entry.get("rule_id")
        version_id = entry.get("versionId") or entry.get("version_id") or entry.get("liveVersion")
        namespace = entry.get("namespace")
        if not rule_id or not version_id or namespace is None:
            logging.warning("[%d/%d] skipping entry missing ruleId/versionId/namespace: %s", i, len(pairs), entry)
            fail += 1
            results.append({"ruleId": rule_id, "versionId": version_id, "namespace": namespace, "status": "SKIPPED"})
            continue

        if args.dry_run:
            logging.info("[%d/%d] DRY-RUN POST %s/rule/%s/version/%s/sync (OID=%s)", i, len(pairs), BASE, rule_id, version_id, namespace)
            continue

        try:
            status = sync_one(session, cookie, rule_id, version_id, namespace, args.timeout)
            if 200 <= status < 300:
                logging.info("[%d/%d] OK  %s / %s (OID=%s) -> %s", i, len(pairs), rule_id, version_id, namespace, status)
                ok += 1
            else:
                logging.error("[%d/%d] FAIL %s / %s (OID=%s) -> %s", i, len(pairs), rule_id, version_id, namespace, status)
                fail += 1
            results.append({"ruleId": rule_id, "versionId": version_id, "namespace": namespace, "status": status})
        except requests.RequestException as exc:
            logging.exception("[%d/%d] ERROR %s / %s (OID=%s): %s", i, len(pairs), rule_id, version_id, namespace, exc)
            fail += 1
            results.append({"ruleId": rule_id, "versionId": version_id, "namespace": namespace, "status": f"ERROR: {exc}"})

        if args.sleep:
            time.sleep(args.sleep)

    Path("sync_rule_versions_result.json").write_text(json.dumps(results, indent=2))
    logging.info("done. ok=%d fail=%d total=%d (results -> sync_rule_versions_result.json)", ok, fail, len(pairs))
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
