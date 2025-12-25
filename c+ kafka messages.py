import matplotlib.pyplot as plt
import numpy as np
from math import ceil

# Data
clusters = ["US", "IN", "EU", "ASIA"]

# --- JULY DATA ---
events_july = [310, 2515, 1160, 5300]
failure_count_july = [0.1, 11.3, 0, 24.5]
retry_count_july = [0.11, 5.45, 0.15, 2.04]
avg_time_july = [7.52, 15, 72.7, 10]

# --- AUGUST DATA ---
events_august = [1210, 2430, 1180, 6080]
failure_count_august = [0, 159, 0, 22]
retry_count_august = [0,4,0,0]
avg_time_august = [7.51, 14.4, 39, 12]

width = 0.35

def nice_step(max_val):
    """Choose a readable tick step for the left axis given a small/large max."""
    steps = [0.01, 0.05, 0.2, 0.5, 2, 5, 10, 20, 50, 100, 200, 500, 1000]
    for step in steps:
        if max_val <= step * 10:
            return step
    return 1000

def nice_time_step(max_val):
    """Choose a readable tick step for the right (time) axis."""
    if max_val <= 10:   return 2
    if max_val <= 20:   return 4
    if max_val <= 50:   return 10
    if max_val <= 100:  return 20
    return 50

for i, cluster in enumerate(clusters):
    fig, ax1 = plt.subplots(figsize=(8, 5))

    x = np.array([0])  # single cluster

    # Calculate percentages for labels at runtime
    total_july = events_july[i] + failure_count_july[i] + retry_count_july[i]
    fail_percent_july = (failure_count_july[i] / total_july) * 100 if total_july > 0 else 0
    retry_percent_july = (retry_count_july[i] / total_july) * 100 if total_july > 0 else 0

    total_august = events_august[i] + failure_count_august[i] + retry_count_august[i]
    fail_percent_august = (failure_count_august[i] / total_august) * 100 if total_august > 0 else 0
    retry_percent_august = (retry_count_august[i] / total_august) * 100 if total_august > 0 else 0

    # --- Bars for July (Blue Theme) ---
    ax1.bar(x - width/2, [events_july[i]], width, label="Success (July)",
            color="#a6cde3", edgecolor="#4c72b0")
    ax1.bar(x - width/2, [failure_count_july[i]], width, bottom=[events_july[i]],
            label="Failure (July)", color="#d9e9f0", edgecolor="#4c72b0", hatch="//")
    ax1.bar(x - width/2, [retry_count_july[i]], width,
            bottom=[events_july[i] + failure_count_july[i]],
            label="Retry (July)", color="#cce5ff", edgecolor="#4c72b0", hatch="xx")

    # --- Bars for August (Orange Theme) ---
    ax1.bar(x + width/2, [events_august[i]], width, label="Success (August)",
            color="#ffbe98", edgecolor="#ff7f0e")
    ax1.bar(x + width/2, [failure_count_august[i]], width, bottom=[events_august[i]],
            label="Failure (August)", color="#ffe8d8", edgecolor="#ff7f0e", hatch="//")
    ax1.bar(x + width/2, [retry_count_august[i]], width,
            bottom=[events_august[i] + failure_count_august[i]],
            label="Retry (August)", color="#fff2e5", edgecolor="#ff7f0e", hatch="xx")

    # --- Left y-axis ---
    left_max = max(total_july, total_august)
    step = nice_step(left_max)
    ymax_left = ceil(left_max / step) * step + step
    ax1.set_ylim(-step * 0.8, ymax_left)
    ax1.set_yticks(np.arange(0, ymax_left + 1, step))
    ax1.set_ylabel("Total Events (Thousands)")

    # --- Right y-axis (Time) ---
    ax2 = ax1.twinx()
    # Calculate ymax based on the current cluster's data
    max_time = max(avg_time_july[i], avg_time_august[i]) * 1.3
    time_step = nice_time_step(max_time)
    # Round the max limit to the next step
    ymax_right = ceil(max_time / time_step) * time_step
    ax2.set_ylim(0, ymax_right)
    ax2.set_yticks(np.arange(0, ymax_right + 1, time_step))
    ax2.set_ylabel("C+ Transit Time (sec)")

    # --- Avg Time markers & labels ---
    july_x = x - width/2 + width * 0.2
    august_x = x + width/2 - width * 0.2
    ax2.plot(july_x, avg_time_july[i], "o", color="#333333", label="Avg Time (July)")
    ax2.plot(august_x, avg_time_august[i], "o", color="#333333", label="Avg Time (August)")
    ax2.plot([july_x, august_x], [avg_time_july[i], avg_time_august[i]], color="gray", linestyle="--")

    time_offset = ymax_right * 0.07
    ax2.text(july_x, avg_time_july[i] + time_offset, f"{avg_time_july[i]:.2f}s", ha="center", va="bottom", color="#333333",fontsize=14)
    ax2.text(august_x, avg_time_august[i] + time_offset, f"{avg_time_august[i]:.2f}s", ha="center", va="bottom", color="#333333",fontsize=14)

    # --- Bar Labels ---
    # Success
    ax1.text(x - width/2, events_july[i] / 2, f"{events_july[i]:.2f}k success", ha="center", va="center", color="black", fontweight="bold",fontsize=18)
    ax1.text(x + width/2, events_august[i] / 2, f"{events_august[i]:.2f}k success", ha="center", va="center", color="black", fontweight="bold",fontsize=18)

    # Failure
    y_fail_july = events_july[i] + failure_count_july[i] + step * 0.4
    ax1.text(x - width/2, y_fail_july, f"{fail_percent_july:.2f}% failure", ha="center", va="bottom", color="#d62728",fontsize=18)
    y_fail_august = events_august[i] + failure_count_august[i] + step * 0.4
    ax1.text(x + width/2, y_fail_august, f"{fail_percent_august:.2f}% failure", ha="center", va="bottom", color="#d62728",fontsize=18)

    # Retry
    y_retry_july = total_july + step * 0.8 # Increased offset to avoid collision
    ax1.text(x - width/2, y_retry_july, f"{retry_percent_july:.2f}% retry", ha="center", va="bottom", color="#e5a800",fontsize=18)
    y_retry_august = total_august + step * 0.8 # Increased offset to avoid collision
    ax1.text(x + width/2, y_retry_august, f"{retry_percent_august:.2f}% retry", ha="center", va="bottom", color="#e5a800",fontsize=18)


    # --- Axes and Title ---
    ax1.set_xticks(x)
    ax1.set_xticklabels([cluster])
    plt.title(f"Cluster: {cluster}")

    # Add month labels
    ax1.text(x - width/2, -step * 0.2, "August", ha="center", va="top", fontsize=9)
    ax1.text(x + width/2, -step * 0.2, "September", ha="center", va="top", fontsize=9)

    # --- Legend ---
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="upper center", bbox_to_anchor=(0.5, 1.15), ncol=4, fontsize=8, frameon=False)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.show()
