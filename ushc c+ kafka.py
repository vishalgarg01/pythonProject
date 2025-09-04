import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import make_interp_spline

# Data
clusters = ["USHC"]
rows_ingested_july = [70.7]
failure_july = [37.3]       #  values
retry_july = [0.014]       #  values
avg_time_july = [70.5]


rows_ingested_august = [40]
failure_august = [39.3]      #  values
retry_august = [0.092]       #  values
avg_time_august = [27]

x = np.arange(len(clusters))
width = 0.35

fig, ax1 = plt.subplots(figsize=(12, 6))

# Bars for July
bars_july = ax1.bar(
    x - width/2, rows_ingested_july, width,
    label="Successfully Rows Ingested (July)", color="#fcd5b5", edgecolor="peru"
)
fail_july_bars = ax1.bar(
    x - width/2, failure_july, width,
    bottom=rows_ingested_july,
    label="Failure count (July)", color="#fde9d9", edgecolor="peru", hatch="//"
)
retry_july_bars = ax1.bar(
    x - width/2, retry_july, width,
    bottom=[i + j for i, j in zip(rows_ingested_july, failure_july)],
    label="Retry count (July)", color="#fbe4d5", edgecolor="peru", hatch="\\\\"
)

# Bars for August
bars_august = ax1.bar(
    x + width/2, rows_ingested_august, width,
    label="Successfully Rows Ingested (August)", color="#c6e0b4", edgecolor="darkgreen"
)
fail_august_bars = ax1.bar(
    x + width/2, failure_august, width,
    bottom=rows_ingested_august,
    label="Failure count (August)", color="#e2f0d9", edgecolor="darkgreen", hatch="//"
)
retry_august_bars = ax1.bar(
    x + width/2, retry_august, width,
    bottom=[i + j for i, j in zip(rows_ingested_august, failure_august)],
    label="Retry count (August)", color="#d9ead3", edgecolor="darkgreen", hatch="\\\\"
)

# Secondary axis for Avg Time
ax2 = ax1.twinx()

# Smooth curves using spline interpolation
x_smooth = np.linspace(x.min(), x.max(), 300)

# July line (blue, dotted)
ax2.plot(x, avg_time_july, linestyle="--", color="blue", marker="o", linewidth=2, label="Avg Time (July)")

# August line (red, solid)
ax2.plot(x, avg_time_august, linestyle="-", color="red", marker="o", linewidth=2, label="Avg Time (August)")

# Add labels for successfully ingested rows
for bar in bars_july:
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height()-0.5,
             f"{bar.get_height():.2f}", ha="center", va="top", fontsize=9, color="black", fontweight="bold")
for bar in bars_august:
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height()-0.5,
             f"{bar.get_height():.2f}", ha="center", va="top", fontsize=9, color="black", fontweight="bold")

# Dynamic margin logic
# Dynamic margin logic
min_label_offset = 110.5  # Minimum spacing above bar segments

# Labels for Failure %
for i, val in enumerate(failure_july):
    base = rows_ingested_july[i]
    y_pos = base + 20
    ax1.text(x[i] - width/2, y_pos,
             f"{val:.2f}", ha="center", va="bottom", fontsize=8, color="red")

for i, val in enumerate(failure_august):
    base = rows_ingested_august[i]
    y_pos = base + max(val, 30)
    ax1.text(x[i] + width/2, y_pos,
             f"{val:.2f}", ha="center", va="bottom", fontsize=8, color="red")

# Labels for Retry %
for i, val in enumerate(retry_july):
    base = rows_ingested_july[i] + failure_july[i]
    y_pos = base + max(val, 0.1)
    ax1.text(x[i] - width/2, y_pos,
             f"{val:.2f}", ha="center", va="bottom", fontsize=8, color="green")

for i, val in enumerate(retry_august):
    base = rows_ingested_august[i] + failure_august[i]
    y_pos = base + max(val, 10.5)
    ax1.text(x[i] + width/2, y_pos,
             f"{val:.2f}", ha="center", va="bottom", fontsize=8, color="green")


# Avg time labels
for i, val in enumerate(avg_time_july):
    ax2.text(x[i] - width/2, val + 2, f"{val} sec", ha="center", va="bottom", fontsize=9, color="blue")
for i, val in enumerate(avg_time_august):
    ax2.text(x[i] + width/2, val + 2, f"{val} sec", ha="center", va="bottom", fontsize=9, color="red")

# Axes and labels
ax1.set_xticks(x)
ax1.set_xticklabels(clusters)
ax1.set_ylabel("Rows (Thousands)")
ax2.set_ylabel("C+ Transit Time (sec)")

# Combine all legend handles
handles1, labels1 = ax1.get_legend_handles_labels()
handles2, labels2 = ax2.get_legend_handles_labels()
handles = handles1 + handles2
labels = labels1 + labels2

# Legend on top
ax1.legend(handles, labels, loc="upper center",
           bbox_to_anchor=(0.5, 1.08),
           ncol=len(handles), fontsize=8, frameon=False)

plt.tight_layout()
plt.show()
