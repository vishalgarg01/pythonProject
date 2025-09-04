import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import make_interp_spline

# Data
clusters = ["US", "IN", "EU", "ASIA"]
rows_ingested_july = [41.5, 4.34, 8.8, 0.063]
rows_ingested_august = [1.83, 2.15, 7.22, 0.061]
failure_july = [0.86, 0.05, 0, 0]   # example %
failure_august = [9.21, 0.39, 0, 0]   # example %
avg_time_july = [400, 196, 474, 454]
avg_time_august = [135, 142, 394, 445]

x = np.arange(len(clusters))
width = 0.35

fig, ax1 = plt.subplots(figsize=(12, 6))

# Bars for july
bars_july = ax1.bar(
    x - width/2, rows_ingested_july, width,
    label="Successfully Rows Ingested (July)", color="#fcd5b5", edgecolor="peru"
)
fail_july_bars = ax1.bar(
    x - width/2, failure_july, width,
    bottom=rows_ingested_july,
    label="Failure % (July)", color="#fde9d9", edgecolor="peru", hatch="//"
)

# Bars for august
bars_august = ax1.bar(
    x + width/2, rows_ingested_august, width,
    label="Successfully Rows Ingested (August)", color="#c6e0b4", edgecolor="darkgreen"
)
fail_august_bars = ax1.bar(
    x + width/2, failure_august, width,
    bottom=rows_ingested_august,
    label="Failure % (August)", color="#e2f0d9", edgecolor="darkgreen", hatch="//"
)

# Secondary axis for Avg Time
ax2 = ax1.twinx()

# Smooth curves using spline interpolation
x_smooth = np.linspace(x.min(), x.max(), 300)

# july dotted blue
spl_july = make_interp_spline(x, avg_time_july, k=3)
y_july_smooth = spl_july(x_smooth)
ax2.plot(x_smooth, y_july_smooth, linestyle="--", color="blue", linewidth=2, label="Avg Time (July)")

# July solid red
spl_august = make_interp_spline(x, avg_time_august, k=3)
y_august_smooth = spl_august(x_smooth)
ax2.plot(x_smooth, y_august_smooth, linestyle="-", color="red", linewidth=2, label="Avg Time (August)")

# Add labels for Rows Ingested
for bar in bars_july:
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height()-0.5,
             f"{bar.get_height():.2f}", ha="center", va="top", fontsize=9, color="black", fontweight="bold")
for bar in bars_august:
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height()-0.5,
             f"{bar.get_height():.2f}", ha="center", va="top", fontsize=9, color="black", fontweight="bold")

# Add labels for Failure %
for i, val in enumerate(failure_july):
    ax1.text(x[i] - width / 2, rows_ingested_july[i] + val + 0.2,
             f"{val:.2f}%", ha="center", va="bottom", fontsize=8, color="brown")
for i, val in enumerate(failure_august):
    ax1.text(x[i] + width / 2, rows_ingested_august[i] + val + 0.2,
             f"{val:.2f}%", ha="center", va="bottom", fontsize=8, color="darkgreen")

# Add labels for Avg Time
for i, val in enumerate(avg_time_july):
    ax2.text(x[i] - width/2, val+5, f"{val}", ha="center", va="bottom", fontsize=9, color="blue")
for i, val in enumerate(avg_time_august):
    ax2.text(x[i] + width/2, val+5, f"{val}", ha="center", va="bottom", fontsize=9, color="red")

# Final formatting
ax1.set_xticks(x)
ax1.set_xticklabels(clusters)
ax1.set_ylabel("Successfully Rows Ingested (Millions) & Failure %")
ax2.set_ylabel("Avg Time per row (ms)")


# Combine all legend handles & labels
handles1, labels1 = ax1.get_legend_handles_labels()
handles2, labels2 = ax2.get_legend_handles_labels()

# Merge them
handles = handles1 + handles2
labels = labels1 + labels2

# Place legend at the top center (where title was)
ax1.legend(handles, labels, loc="upper center",
           bbox_to_anchor=(0.5, 1.05),   # adjust height if needed
           ncol=len(handles), fontsize=9, frameon=False)

plt.tight_layout()
plt.show()
