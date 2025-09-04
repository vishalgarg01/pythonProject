import matplotlib.pyplot as plt
import numpy as np

# Data
months = ["July'25", "August'25"]
total_runs = np.array([34800, 61540])
success_percent = np.array([100, 100])

# Calculate successful runs based on the success percentage
successful_runs = total_runs * (success_percent / 100)

# Bar width
bar_width = 0.5

# X-axis positions for each bar group
index = np.arange(len(months))

# Create figure and axes
fig, ax = plt.subplots()

# Plotting total runs (as the base layer)
bars_total = ax.bar(index, total_runs, bar_width, label='Total Runs', color='red')

# Plotting successful runs on top of the total runs
bars_successful = ax.bar(index, successful_runs, bar_width, label='Successful Runs', color='skyblue')

# Adding labels and title
ax.set_xlabel('Month')
ax.set_ylabel('Runs')
plt.title('Total Runs and Successful Runs for july 2025 and August 2025')

# X-axis ticks and labels
ax.set_xticks(index)
ax.set_xticklabels(months)

# Annotating success percentage on top of the bars
for i in range(len(index)):
    ax.text(index[i], successful_runs[i] + 500, f'{success_percent[i]:.2f}%', ha='center', color='black', fontsize=12)

# Adding legends
ax.legend(loc='upper left')

# Show plot
plt.show()
