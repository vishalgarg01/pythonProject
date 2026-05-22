"""Read filtered CSV, emit a mongoshell .js script that performs the updates."""
import csv
import json

CSV_PATH = "/Users/vishalgarg/Documents/pythonProject/filtered data - Sheet1.csv"
OUT_PATH = "/Users/vishalgarg/Documents/pythonProject/update_authorization.js"

NAMESPACE_MAP = {
    "Optum UAT-Parent": 4000084,
    "Test2026 Merge OPTUM Parent": 97000117,
    "Optum Parent UATdevtest8": 93000084,
    "Optum Parent UAT": 4000084,
}

updates = []
skipped = []

with open(CSV_PATH, newline="") as f:
    for row in csv.DictReader(f):
        group = row["top_group_name"]
        ns = NAMESPACE_MAP.get(group)
        if ns is None:
            skipped.append(row)
            continue
        block_name = row["processor_name"].split("_")[0]
        updates.append({
            "namespace": ns,
            "name": row["dataflow_name"],
            "blockName": block_name,
            "authValue": row["auth_value"],
            # extra context for logging
            "topGroup": group,
            "processorName": row["processor_name"],
            "processorId": row["processor_id"],
        })

print(f"Built {len(updates)} updates; skipped {len(skipped)} rows (groups not in mapping)")
for s in skipped:
    print(f"  SKIPPED: {s['top_group_name']} / {s['dataflow_name']} / {s['processor_name']}")

js = (
    "// Auto-generated mongoshell script.\n"
    "// Run: mongosh <conn-string>/neometa update_authorization.js\n"
    "// Or paste contents into mongoshell after `use neometa`.\n\n"
    "const updates = " + json.dumps(updates, indent=2) + ";\n\n"
    + r"""
const summary = {
  total: updates.length,
  updated: 0,
  alreadySet: 0,
  ruleNotFound: 0,
  versionNotFound: 0,
  blockNotFound: 0,
  updateFailed: 0
};
const issues = [];
const syncEntriesUpdated = [];   // only docs we just modified
const syncEntriesAll = [];       // all docs where we found rule + version

for (const u of updates) {
  const rule = db.new_rule_cps.findOne(
    { namespace: u.namespace, name: u.name },
    { _id: 1, liveVersion: 1, name: 1, namespace: 1 }
  );
  if (!rule) {
    summary.ruleNotFound++;
    issues.push({ stage: "ruleNotFound", ...u });
    continue;
  }
  if (!rule.liveVersion) {
    summary.versionNotFound++;
    issues.push({ stage: "noLiveVersion", ruleId: rule._id, ...u });
    continue;
  }
  const version = db.ruleversion_cps.findOne(
    { _id: rule.liveVersion, refId: rule._id }
  );
  if (!version) {
    summary.versionNotFound++;
    issues.push({ stage: "versionNotFound", ruleId: rule._id, liveVersion: rule.liveVersion, ...u });
    continue;
  }

  // Track every doc where rule + version were resolved (regardless of update outcome)
  syncEntriesAll.push({ ruleId: rule._id, versionId: version._id, namespace: u.namespace });

  // Find first block where name === blockName AND type === 'http_write'
  let blockIdx = -1;
  for (let i = 0; i < version.blocks.length; i++) {
    const b = version.blocks[i];
    if (b && b.name === u.blockName && b.type === "http_write") {
      blockIdx = i;
      break;
    }
  }
  if (blockIdx < 0) {
    summary.blockNotFound++;
    issues.push({
      stage: "blockNotFound",
      ruleId: rule._id,
      versionId: version._id,
      availableBlocks: version.blocks.map(b => ({ name: b.name, type: b.type })),
      ...u
    });
    continue;
  }

  const block = version.blocks[blockIdx];
  const existing = block.config ? block.config.additionalHeaders : undefined;
  const isEmpty = (existing === undefined || existing === null || existing === "");
  if (!isEmpty) {
    summary.alreadySet++;
    issues.push({
      stage: "alreadySet",
      ruleId: rule._id,
      versionId: version._id,
      blockIdx: blockIdx,
      existing: existing,
      ...u
    });
    continue;
  }

  const newHeader = '"Authorization":"' + u.authValue + '"';
  const setPath = "blocks." + blockIdx + ".config.additionalHeaders";
  const setObj = {};
  setObj[setPath] = newHeader;
  const res = db.ruleversion_cps.updateOne(
    { _id: version._id },
    { $set: setObj }
  );
  if (res.modifiedCount === 1) {
    summary.updated++;
    syncEntriesUpdated.push({ ruleId: rule._id, versionId: version._id, namespace: u.namespace });
  } else {
    summary.updateFailed++;
    issues.push({
      stage: "updateFailed",
      ruleId: rule._id,
      versionId: version._id,
      blockIdx: blockIdx,
      result: res,
      ...u
    });
  }
}

print("=== SUMMARY ===");
printjson(summary);
print("=== ISSUES (" + issues.length + ") ===");
issues.forEach(i => printjson(i));

// Emit JSON arrays in the exact shape sync_rule_versions.py wants.
// Look for these markers in the output and copy the JSON between them.
print("=== SYNC_ENTRIES_UPDATED_BEGIN ===");
print(JSON.stringify(syncEntriesUpdated, null, 2));
print("=== SYNC_ENTRIES_UPDATED_END ===");

print("=== SYNC_ENTRIES_ALL_BEGIN ===");
print(JSON.stringify(syncEntriesAll, null, 2));
print("=== SYNC_ENTRIES_ALL_END ===");
"""
)

with open(OUT_PATH, "w") as f:
    f.write(js)

print(f"\nWrote: {OUT_PATH}")
