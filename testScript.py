import json
import re

# Input and output file paths
input_file = "/Users/vishalgarg/Downloads/hydraLogs/jan21_4.json"
output_file = "/Users/vishalgarg/Downloads/hydraLogs/lineage_ids4.txt"

# Load the JSON file
with open(input_file, "r") as file:
    data = json.load(file)

# Extract the log entries
log_entries = data["data"]["result"]

# List to store matching lineageIds
matching_lineage_ids = []

# Regex patterns
error_pattern = re.compile(r"Error while processing event")
invalid_event_pattern = re.compile(r"Invalid event:")
lineage_id_pattern = re.compile(r"lineageId=([\w-]+)")

# Process each log entry
for entry in log_entries:
    for value in entry["values"]:
        log_message = value[1]  # The log message part
        if error_pattern.search(log_message) and not invalid_event_pattern.search(log_message):
            match = lineage_id_pattern.search(log_message)
            if match:
                lineage_id = match.group(1)
                matching_lineage_ids.append(lineage_id)

# Write to output file
with open(output_file, "w") as file:
    for lineage_id in matching_lineage_ids:
        file.write(lineage_id + "\n")

print(f"Extracted {len(matching_lineage_ids)} lineageIds with errors (excluding 'Invalid event:').")
