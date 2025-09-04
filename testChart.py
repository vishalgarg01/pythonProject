import matplotlib.pyplot as plt
import numpy as np
from math import ceil

# Data
clusters = ["US", "IN", "EU", "ASIA"]
rows_ingested_july = [41.5, 4.34, 8.8, 0.063]
rows_ingested_august = [1.83, 2.15, 7.22, 0.061]
failure_july = [0.86, 0.05, 0, 0]   # %
failure_august = [9.21, 0.39, 0, 0] # %
avg_time_july = [400, 196, 474, 454]
avg_time_august = [135, 142, 394, 445]

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

    # Bars for July
    bars_july = ax1.bar(
        x - width/2, [rows_ingested_july[i]], width,
        label="Successfully Rows Ingested (July)", color="#fcd5b5", edgecolor="peru"
    )
    ax1.bar(
        x - width/2, [failure_july[i]], width,
        bottom=[rows_ingested_july[i]],
        label="Failure % (July)", color="#fde9d9", edgecolor="peru", hatch="//"
    )

    # Bars for August
    bars_august = ax1.bar(
        x + width/2, [rows_ingested_august[i]], width,
        label="Successfully Rows Ingested (August)", color="#c6e0b4", edgecolor="darkgreen"
    )
    ax1.bar(
        x + width/2, [failure_august[i]], width,
        bottom=[rows_ingested_august[i]],
        label="Failure % (August)", color="#e2f0d9", edgecolor="darkgreen", hatch="//"
    )

    # --- Left y-axis ---
    left_max = max(rows_ingested_july[i] + failure_july[i],
                   rows_ingested_august[i] + failure_august[i])
    step = nice_step(left_max)
    ymax_left = ceil(left_max / step) * step + step * 0.6
    ax1.set_ylim(0, ymax_left)
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
    july_x   = july_bar.get_x()   + july_bar.get_width()/2
    august_x = august_bar.get_x() + august_bar.get_width()/2

    ax2.plot(july_x,   avg_time_july[i],   "bo", label="Avg Time (July)"   if i == 0 else "")
    ax2.plot(august_x, avg_time_august[i], "ro", label="Avg Time (August)" if i == 0 else "")

    # add bigger offset to avoid collision
    ax2.text(july_x,   avg_time_july[i]   + 20, f"{avg_time_july[i]} ms",
             ha="center", va="bottom", fontsize=9, color="blue")
    ax2.text(august_x, avg_time_august[i] + 20, f"{avg_time_august[i]} ms",
             ha="center", va="bottom", fontsize=9, color="red")

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
                 f"{h:.2f}", ha="center", va=va,
                 fontsize=9, color="black", fontweight="bold")

    # --- Failure % labels with extra spacing ---
    ax1.text(x - width/2, rows_ingested_july[i] + failure_july[i] + step * 0.3,
             f"{failure_july[i]:.2f}%", ha="center", va="bottom",
             fontsize=8, color="brown")
    ax1.text(x + width/2, rows_ingested_august[i] + failure_august[i] + step * 0.3,
             f"{failure_august[i]:.2f}%", ha="center", va="bottom",
             fontsize=8, color="darkgreen")

    # X-axis and title
    ax1.set_xticks(x)
    ax1.set_xticklabels([cluster])
    plt.title(f"Cluster: {cluster}")

    # Legends
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="upper center", bbox_to_anchor=(0.5, 1.15),
               ncol=2, fontsize=8, frameon=False)

    plt.tight_layout()
    plt.show()
