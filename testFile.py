import pandas as pd
import numpy as np

# Define the number of rows to reach approximately 100 MB
num_rows = 2_500_000

# Generate data
data = {
    "RowNumber": np.arange(1, num_rows + 1),
    "Column1": np.random.randint(1, 1000000, num_rows),
    "Column2": np.random.uniform(0, 10000, num_rows),
    "Column3": np.random.choice(["A", "B", "C", "D"], num_rows),
    "Column4": np.random.choice(["Yes", "No"], num_rows),
    "Column5": np.random.randn(num_rows)
}

# Create DataFrame
df = pd.DataFrame(data)

# Save to CSV
file_path = "/Users/vishalgarg/Downloads/sample_100MB.csv"
df.to_csv(file_path, index=False)

print(f"CSV file generated: {file_path}")
