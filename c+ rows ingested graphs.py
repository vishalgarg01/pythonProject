import matplotlib.pyplot as plt
import numpy as np
from math import ceil

# Data
clusters = ["US", "IN", "EU", "ASIA"]
rows_ingested_july = [1.83, 2.15, 7.22, 0.061] #success
failure_count_july = [0.186, 0.008, 0, 0]
avg_time_july = [135, 142, 394, 445]

rows_ingested_august = [1.75,2.75,5.13,0.052]
failure_count_august = [0, 0.001, 0,0]
avg_time_august = [141, 118, 263, 478]

width = 0.35

def nice_step(max_val):
    """Choose a readable tick step for the left axis given a small/large max."""
    if max_val <= 0.1:   return 0.01
    if max_val <= 0.5:   return 0.05
    if max_val <= 2:     return 0.2
    if max_val <= 5:     return 0.5
    if max_val <= 20:    return 2
    return 5

for i, cluster in enumerate(clusters):
    fig, ax1 = plt.subplots(figsize=(8, 5))

    x = np.array([0])  # single cluster

    # Calculate failure percentages at runtime for labels
    total_july = rows_ingested_july[i] + failure_count_july[i]
    fail_percent_july = (failure_count_july[i] / total_july) * 100 if total_july > 0 else 0

    total_august = rows_ingested_august[i] + failure_count_august[i]
    fail_percent_august = (failure_count_august[i] / total_august) * 100 if total_august > 0 else 0


    # Bars for July
    bars_july = ax1.bar(
        x - width/2, [rows_ingested_july[i]], width,
        label="Successfully Rows Ingested (July)", color="#a6cde3", edgecolor="#4c72b0"
    )
    ax1.bar(
        x - width/2, [failure_count_july[i]], width,
        bottom=[rows_ingested_july[i]],
        label="Failure % (July)", color="#d9e9f0", edgecolor="#4c72b0", hatch="//"
    )

    # Bars for August
    bars_august = ax1.bar(
        x + width/2, [rows_ingested_august[i]], width,
        label="Successfully Rows Ingested (August)", color="#ffbe98", edgecolor="#ff7f0e"
    )
    ax1.bar(
        x + width/2, [failure_count_august[i]], width,
        bottom=[rows_ingested_august[i]],
        label="Failure % (August)", color="#ffe8d8", edgecolor="#ff7f0e", hatch="//"
    )

    # --- Left y-axis ---
    left_max = max(rows_ingested_july[i] + failure_count_july[i],
                   rows_ingested_august[i] + failure_count_august[i])
    step = nice_step(left_max)
    ymax_left = ceil(left_max / step) * step + step * 0.6
    # Adjust ylim to make space for month labels below the axis
    ax1.set_ylim(-step * 0.8, ymax_left)
    ax1.set_yticks(np.arange(0, ymax_left + step, step))
    ax1.set_ylabel("Successfully Rows Ingested (Millions)")

    # --- Right y-axis ---
    ax2 = ax1.twinx()
    ymax_right = max(max(avg_time_july), max(avg_time_august)) * 1.2
    ymax_right = ceil(ymax_right / 50) * 50
    ax2.set_ylim(0, ymax_right)
    ax2.set_yticks(np.arange(0, ymax_right + 1, 50))
    ax2.set_ylabel("Avg Time per row (ms)")

    # --- Avg Time markers & labels ---
    july_bar = bars_july[0]
    august_bar = bars_august[0]
    # Shift markers inward to avoid label collision
    july_x   = july_bar.get_x()   + july_bar.get_width() * 0.7
    august_x = august_bar.get_x() + august_bar.get_width() * 0.3

    ax2.plot(july_x,   avg_time_july[i],   "o", color="#333333", label="Avg Time (July)"   if i == 0 else "")
    ax2.plot(august_x, avg_time_august[i], "o", color="#333333", label="Avg Time (August)" if i == 0 else "")

    # Dynamically position time labels to avoid collisions
    time_label_offset = ymax_right * 0.07  # 7% of the axis height

    # --- July time label ---
    norm_july_bar_top = (rows_ingested_july[i] + failure_count_july[i]) / ymax_left
    norm_july_time = avg_time_july[i] / ymax_right
    are_july_points_close = abs(norm_july_bar_top - norm_july_time) < 0.15

    # Position below if time point is high OR if bar and time points are vertically close
    if avg_time_july[i] > ymax_right * 0.9 or are_july_points_close:
        y_pos = avg_time_july[i] - time_label_offset
        va = "top"
    else:
        y_pos = avg_time_july[i] + time_label_offset
        va = "bottom"
    ax2.text(july_x, y_pos, f"{avg_time_july[i]} ms",
             ha="center", va=va, fontsize=18, color="#333333")

    # --- August time label ---
    norm_august_bar_top = (rows_ingested_august[i] + failure_count_august[i]) / ymax_left
    norm_august_time = avg_time_august[i] / ymax_right
    are_august_points_close = abs(norm_august_bar_top - norm_august_time) < 0.15

    if avg_time_august[i] > ymax_right * 0.9 or are_august_points_close:
        y_pos = avg_time_august[i] - time_label_offset
        va = "top"
    else:
        y_pos = avg_time_august[i] + time_label_offset
        va = "bottom"
    ax2.text(august_x, y_pos, f"{avg_time_august[i]} ms",
             ha="center", va=va, fontsize=18, color="#333333")

    ax2.plot([july_x, august_x], [avg_time_july[i], avg_time_august[i]],
             color="gray", linestyle="--", linewidth=1)

    # --- Value labels on bars ---
    for bar in bars_july + bars_august:
        h = bar.get_height()
        if h > step * 0.8:
            y = h - (step * 0.2)
            va = "top"
        else:
            y = h + (step * 0.15)
            va = "bottom"
        ax1.text(bar.get_x() + bar.get_width()/2, y,
                 f"{h:.2f} million success", ha="center", va=va,
                 fontsize=18, color="black", fontweight="bold")

    # --- Failure % labels with extra spacing ---
    ax1.text(x - width/2, rows_ingested_july[i] + failure_count_july[i] + step * 0.4,
             f"{fail_percent_july:.2f}% failure", ha="center", va="bottom",
             fontsize=18, color="#d62728")
    ax1.text(x + width/2, rows_ingested_august[i] + failure_count_august[i] + step * 0.4,
             f"{fail_percent_august:.2f}% failure", ha="center", va="bottom",
             fontsize=18, color="#d62728")

    # X-axis and title
    ax1.set_xticks(x)
    ax1.set_xticklabels([cluster])
    plt.title(f"Cluster: {cluster}")

    # Add month labels at the bottom of the bars
    ax1.text(x - width/2, -step * 0.2, "August", ha="center", va="top", fontsize=9)
    ax1.text(x + width/2, -step * 0.2, "September", ha="center", va="top", fontsize=9)

    # Legends
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="upper center", bbox_to_anchor=(0.5, 1.15),
               ncol=2, fontsize=8, frameon=False)

    plt.tight_layout()
    plt.show()
