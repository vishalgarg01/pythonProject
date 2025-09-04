import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import make_interp_spline

# Data
clusters = ["US", "IN", "EU", "ASIA"]
rows_ingested_july = [41.5, 4.34, 8.8, 0.063]
rows_ingested_august = [27.6, 2.33, 3.49, 2.24]
failure_july = [0.86, 0.05, 0, 0]   # example %
failure_august = [1.9, 0.5, 0.9, 0.6]   # example %
avg_time_july = [173, 215, 117, 305]
avg_time_august = [182, 225, 124, 315]

x = np.arange(len(clusters))
width = 0.35

fig, ax1 = plt.subplots(figsize=(12, 6))

# Bars for June
bars_july = ax1.bar(
    x - width/2, rows_ingested_july, width,
    label="Rows Ingested (July)", color="#fcd5b5", edgecolor="peru"
)
fail_june_bars = ax1.bar(
    x - width/2, failure_july, width,
    bottom=rows_ingested_july,
    label="Failure % (July)", color="#fde9d9", edgecolor="peru", hatch="//"
)

# Bars for July
bars_august = ax1.bar(
    x + width/2, rows_ingested_august, width,
    label="Rows Ingested (August)", color="#c6e0b4", edgecolor="darkgreen"
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

# June dotted blue
spl_june = make_interp_spline(x, avg_time_july, k=3)
y_june_smooth = spl_june(x_smooth)
ax2.plot(x_smooth, y_june_smooth, linestyle="--", color="blue", linewidth=2, label="Avg Time (July)")

# July solid red
spl_july = make_interp_spline(x, avg_time_august, k=3)
y_july_smooth = spl_july(x_smooth)
ax2.plot(x_smooth, y_july_smooth, linestyle="-", color="red", linewidth=2, label="Avg Time (August)")

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
ax1.set_ylabel("Rows Ingested & Failure %")
ax2.set_ylabel("Avg Time (ms)")
ax1.legend(loc="upper left")
ax2.legend(loc="upper right")

plt.title("Cluster Comparison: Rows Ingested, Failure %, and Avg Time")
plt.tight_layout()
plt.show()
