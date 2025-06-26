import pandas as pd
from db_connection import DataManager

# Step 1: Load data
data_manager = DataManager()
queries = """
SELECT h.*
FROM lsd.matters m
JOIN lsd.hearings h ON m.matter_id = h.matter_id
WHERE m.court_id = 1
  AND CAST(h.hearing_date AS DATE) BETWEEN '2021-01-01' AND '2024-12-31';
"""
df1 = data_manager.execute_query(queries)
df1 = df1[['hearing_id', 'matter_id']]

queries = """
SELECT h.*
FROM lsd.matters m
JOIN lsd.hearings h ON m.matter_id = h.matter_id
WHERE m.court_id = 14
  AND CAST(h.hearing_date AS DATE) BETWEEN '2021-01-01' AND '2024-12-31';
"""
df2 = data_manager.execute_query(queries)
df2 = df2[['hearing_id', 'matter_id']]
# Merge (concatenate) both DataFrames vertically
merged_df = pd.concat([df1, df2], ignore_index=True)

# Optional: Check the result
print(merged_df.head())
# Step 1: Group by matter_id and count hearings
hearings_per_matter = merged_df.groupby('matter_id').size().reset_index(name='hearing_count')

# Step 2: Calculate mean
mean_hearings = hearings_per_matter['hearing_count'].mean()

# Step 3: Print result
print(f"📊 Mean number of hearings per matter: {mean_hearings:.2f}")
