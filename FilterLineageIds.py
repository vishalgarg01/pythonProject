import json
import re

# File paths
log_file = "/Users/vishalgarg/Downloads/merged_jan21.json"  # Input JSON log file
output_file = "/Users/vishalgarg/Downloads/hydraLogs/mergedfinal_filtered_lineage_ids.txt"  # Output file

# Load the JSON log data
with open(log_file, "r") as file:
    data = json.load(file)

# Extract log messages
log_entries = data["data"]["result"]

# Regex patterns
error_pattern = re.compile(r"Error while processing event", re.IGNORECASE)
invalid_event_pattern = re.compile(r"Invalid event:", re.IGNORECASE)
async_pattern = re.compile(r"Event will be processed in async manner", re.IGNORECASE)
lineage_id_pattern = re.compile(r"lineageId=([\w-]+)")

# Sets to store lineage IDs
error_lineage_ids = set()
async_lineage_ids = set()

# Iterate through log entries
for entry in log_entries:
    for value in entry["values"]:
        log_message = value[1]

        # Extract lineage ID if log contains "Error while processing event" but not "Invalid event:"
        if error_pattern.search(log_message) and not invalid_event_pattern.search(log_message):
            match = lineage_id_pattern.search(log_message)
            if match:
                error_lineage_ids.add(match.group(1))

        # Extract lineage IDs that appear in "Event will be processed in async manner"
        if async_pattern.search(log_message):
            match = lineage_id_pattern.search(log_message)
            if match:
                async_lineage_ids.add(match.group(1))

# Find lineage IDs that had errors but do NOT appear with "Event will be processed in async manner"
final_filtered_lineage_ids = error_lineage_ids - async_lineage_ids

# Write the final filtered lineage IDs to a new file
with open(output_file, "w") as file:
    for lineage_id in final_filtered_lineage_ids:
        file.write(lineage_id + "\n")

print(f"Filtered {len(final_filtered_lineage_ids)} lineage IDs. Saved to {output_file}.")
