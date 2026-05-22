#!/usr/bin/env python3
"""
Rewrite `attribution_code` in ruleversion_cps blocks so that a leading `$`
becomes `$[0]` when the block is `convert_csv_to_json` with
config['split response'] == 'false'.

Example:
  $['Store_Till']   ->  $[0]['Store_Till']
  $.Store_Till      ->  $[0].Store_Till
Already-qualified values like `$[0]...` are left untouched.

Dry-run by default. Pass --apply to actually write the changes.

Env vars (override with CLI flags if you prefer):
  RULEVERSION_MONGO_HOST
  RULEVERSION_MONGO_USERNAME
  RULEVERSION_MONGO_PASSWORD
  RULEVERSION_MONGO_DB         (default: cps)
  RULEVERSION_MONGO_COLLECTION (default: ruleversion_cps)
  RULEVERSION_MONGO_AUTHSOURCE (default: admin)
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

from pymongo import MongoClient, UpdateOne

TARGET_BLOCK_TYPE = "convert_csv_to_json"
SPLIT_RESPONSE_KEY = "split response"
TARGET_FIELD = "attribution_code"

# Matches a leading `$` that is NOT already followed by `[0]`
LEADING_DOLLAR = re.compile(r"^\$(?!\[0\])")


def needs_rewrite(value: Any) -> bool:
    return isinstance(value, str) and bool(LEADING_DOLLAR.match(value))


def rewrite(value: str) -> str:
    return LEADING_DOLLAR.sub("$[0]", value, count=1)


def find_changes(doc: Dict[str, Any]) -> List[Tuple[int, str, str, str]]:
    """Return list of (block_index, block_name, old_value, new_value)."""
    changes: List[Tuple[int, str, str, str]] = []
    blocks = doc.get("blocks") or []
    for idx, block in enumerate(blocks):
        if not isinstance(block, dict):
            continue
        if block.get("type") != TARGET_BLOCK_TYPE:
            continue
        config = block.get("config") or {}
        if not isinstance(config, dict):
            continue
        if str(config.get(SPLIT_RESPONSE_KEY, "")).lower() != "false":
            continue
        old_value = config.get(TARGET_FIELD)
        if not needs_rewrite(old_value):
            continue
        new_value = rewrite(old_value)
        changes.append((idx, block.get("name", ""), old_value, new_value))
    return changes


def build_update(changes: List[Tuple[int, str, str, str]]) -> Dict[str, Any]:
    """Positional $set for each block that changed."""
    set_fields: Dict[str, str] = {}
    for idx, _name, _old, new in changes:
        set_fields[f"blocks.{idx}.config.{TARGET_FIELD}"] = new
    return {"$set": set_fields}


def iter_candidates(collection) -> Any:
    """Query only docs that *could* match — server-side filter, client-side confirm."""
    query = {
        "blocks": {
            "$elemMatch": {
                "type": TARGET_BLOCK_TYPE,
                f"config.{SPLIT_RESPONSE_KEY}": "false",
                f"config.{TARGET_FIELD}": {"$regex": r"^\$(?!\[0\])"},
            }
        }
    }
    # Only pull fields we need
    return collection.find(query, {"blocks": 1})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Actually write changes (default: dry-run).")
    parser.add_argument("--limit", type=int, default=0, help="Only process first N matching docs (0 = all).")
    parser.add_argument("--id", dest="only_id", default=None, help="Restrict to a single _id (useful for testing).")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    host = os.environ.get("RULEVERSION_MONGO_HOST")
    username = os.environ.get("RULEVERSION_MONGO_USERNAME")
    password = os.environ.get("RULEVERSION_MONGO_PASSWORD")
    db_name = os.environ.get("RULEVERSION_MONGO_DB", "cps")
    coll_name = os.environ.get("RULEVERSION_MONGO_COLLECTION", "ruleversion_cps")
    auth_source = os.environ.get("RULEVERSION_MONGO_AUTHSOURCE", "admin")

    if not host:
        logging.error("RULEVERSION_MONGO_HOST is not set.")
        return 2

    client = MongoClient(host, username=username, password=password, authSource=auth_source)
    collection = client[db_name][coll_name]

    cursor = iter_candidates(collection) if not args.only_id else collection.find({"_id": args.only_id}, {"blocks": 1})

    bulk_ops: List[UpdateOne] = []
    scanned = 0
    matched_docs = 0
    total_block_changes = 0

    for doc in cursor:
        scanned += 1
        changes = find_changes(doc)
        if not changes:
            continue
        matched_docs += 1
        total_block_changes += len(changes)

        logging.info("doc _id=%s -> %d block(s) to update", doc["_id"], len(changes))
        for idx, name, old, new in changes:
            logging.info("  blocks[%d] (%s): %r -> %r", idx, name, old, new)

        bulk_ops.append(UpdateOne({"_id": doc["_id"]}, build_update(changes)))

        if args.limit and matched_docs >= args.limit:
            break

    logging.info(
        "Summary: scanned=%d, docs_with_changes=%d, block_level_changes=%d",
        scanned, matched_docs, total_block_changes,
    )

    if not bulk_ops:
        logging.info("Nothing to update.")
        client.close()
        return 0

    if not args.apply:
        logging.info("Dry-run complete. Re-run with --apply to commit %d update(s).", len(bulk_ops))
        client.close()
        return 0

    result = collection.bulk_write(bulk_ops, ordered=False)
    logging.info(
        "bulk_write done. matched=%d modified=%d",
        result.matched_count, result.modified_count,
    )
    client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
