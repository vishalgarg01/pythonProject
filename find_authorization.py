import xml.etree.ElementTree as ET
import csv
import sys

FLOW_FILE = "/Users/vishalgarg/Documents/pythonProject/flow.xml"
SKIP_VALUE_SUBSTR = "ten:append('product_sdk_user@capillarytech.com'):base64Encode()"

def text(el, tag, default=""):
    child = el.find(tag)
    if child is None or child.text is None:
        return default
    return child.text

def find_authorization(processor):
    for prop in processor.findall("property"):
        name_el = prop.find("name")
        if name_el is not None and name_el.text == "Authorization":
            value_el = prop.find("value")
            if value_el is None or value_el.text is None:
                return None
            return value_el.text
    return None

def walk(group, top_group_name, dataflow_name, dataflow_id, results, depth=0):
    # The 'group' is a <processGroup> (or <rootGroup>) element.
    # Its direct <processor> children belong to this group.
    # Its direct <processGroup> children are nested groups.

    for processor in group.findall("processor"):
        auth_value = find_authorization(processor)
        if auth_value is None:
            continue
        if SKIP_VALUE_SUBSTR in auth_value:
            continue
        proc_name = text(processor, "name")
        proc_id = text(processor, "id")
        scheduled = text(processor, "scheduledState")
        results.append({
            "top_group_name": top_group_name,
            "dataflow_name": dataflow_name,
            "dataflow_id": dataflow_id,
            "processor_name": proc_name,
            "processor_id": proc_id,
            "scheduledState": scheduled,
            "auth_value": auth_value,
        })

    for child in group.findall("processGroup"):
        child_name = text(child, "name")
        child_id = text(child, "id")
        # If we're at the root level, this child becomes the top group.
        # If we're at the top group level, this child is a dataflow.
        # Beyond that, keep the same dataflow context (nested groups inside a dataflow).
        if top_group_name is None:
            walk(child, child_name, None, None, results, depth + 1)
        elif dataflow_name is None:
            walk(child, top_group_name, child_name, child_id, results, depth + 1)
        else:
            walk(child, top_group_name, dataflow_name, dataflow_id, results, depth + 1)

def main():
    print(f"Parsing {FLOW_FILE} ...", file=sys.stderr)
    tree = ET.parse(FLOW_FILE)
    root = tree.getroot()  # <flowController>
    root_group = root.find("rootGroup")
    if root_group is None:
        print("No rootGroup found", file=sys.stderr)
        return

    results = []
    # rootGroup-level processors (uncategorized) first
    # Treat rootGroup itself as a placeholder; its children processGroups are top-level groups.
    for processor in root_group.findall("processor"):
        auth_value = find_authorization(processor)
        if auth_value is None:
            continue
        if SKIP_VALUE_SUBSTR in auth_value:
            continue
        results.append({
            "top_group_name": "(rootGroup)",
            "dataflow_name": "(rootGroup)",
            "dataflow_id": text(root_group, "id"),
            "processor_name": text(processor, "name"),
            "processor_id": text(processor, "id"),
            "scheduledState": text(processor, "scheduledState"),
            "auth_value": auth_value,
        })

    for top_group in root_group.findall("processGroup"):
        top_name = text(top_group, "name")
        walk(top_group, top_name, None, None, results)

    # Write CSV
    out_csv = "/Users/vishalgarg/Documents/pythonProject/authorization_processors.csv"
    fieldnames = ["top_group_name", "dataflow_name", "dataflow_id",
                  "processor_name", "processor_id", "scheduledState", "auth_value"]
    with open(out_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(row)

    print(f"Found {len(results)} processors with non-skipped Authorization property", file=sys.stderr)
    print(f"Output: {out_csv}", file=sys.stderr)

    # Also print a summary to stdout
    for r in results:
        print(f"[{r['top_group_name']}] dataflow={r['dataflow_name']} ({r['dataflow_id']}) "
              f"processor={r['processor_name']} ({r['processor_id']}) "
              f"state={r['scheduledState']} value={r['auth_value']}")

if __name__ == "__main__":
    main()
