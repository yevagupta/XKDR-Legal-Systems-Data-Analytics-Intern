import pandas as pd
from db_connection import DataManager

# Step 1: Load data
data_manager = DataManager()
queries="""SELECT 
  h.*, 
  m.case_status
FROM lsd.matters m
JOIN lsd.hearings h ON m.matter_id = h.matter_id
WHERE m.court_id = 14
  AND CAST(h.hearing_date AS DATE) BETWEEN '2021-01-01' AND '2024-12-31'
;
"""


df = data_manager.execute_query(queries)
df = df[['hearing_id', 'matter_id', 'hearing_date','case_status']]

# Step 3: Convert to datetime format safely
df['hearing_date'] = pd.to_datetime(df['hearing_date'], errors='coerce')

# Step 4: Drop rows with invalid/missing dates
df = df.dropna(subset=['hearing_date'])

# Step 5: Extract year column
df['year'] = df['hearing_date'].dt.year

# Total unique matters
total_matters = df['matter_id'].nunique()
print("✅ Total unique matters:", total_matters)
# --- Part 1: Year-wise counts WITH overlaps ---
yearly_counts = df.groupby('year')['matter_id'].nunique().reset_index(name='unique_matters')
yearly_counts['percentage'] = (yearly_counts['unique_matters'] / total_matters * 100).round(2)

print("\n📊 Year-wise unique matter counts (with overlaps):")
print(yearly_counts)
print("⚠️  These may overcount because some matters appear in multiple years.")

# --- Part 2: Year-wise counts based on FIRST appearance only ---
first_hearings = df.sort_values('hearing_date').drop_duplicates(subset='matter_id')
first_hearings['year'] = first_hearings['hearing_date'].dt.year

first_year_counts = first_hearings.groupby('year')['matter_id'].count().reset_index(name='first_seen_matters')
first_year_counts['percentage'] = (first_year_counts['first_seen_matters'] / total_matters * 100).round(2)

print("\n📊 Year-wise unique matter counts (first appearance only):")
print(first_year_counts)
print("✅ Sum of first-seen matters:", first_year_counts['first_seen_matters'].sum())


# Group by matter_id to count hearings
hearings_per_matter = df.groupby('matter_id').size().reset_index(name='hearing_count')
# Mean number of hearings across all matters
mean_hearings = hearings_per_matter['hearing_count'].mean()
print(f"\n📊 Overall mean hearings per matter: {mean_hearings:.2f}")
df['hearing_date'] = pd.to_datetime(df['hearing_date'], errors='coerce')

# Drop rows with missing hearing dates (to avoid issues)
df = df.dropna(subset=['hearing_date'])

# Sort so latest hearing is last per matter
df_sorted = df.sort_values(['matter_id', 'hearing_date'])

# Keep only the last hearing per matter_id
latest_hearings = df_sorted.drop_duplicates(subset='matter_id', keep='last')

# Exclude rows where case_status is empty string
final_cases = latest_hearings[latest_hearings['case_status'].str.strip() != '']

# Count the final verdicts
status_counts = final_cases['case_status'].value_counts()

# Display result
print("📊 Final case status (based on last hearing per matter):")
print(status_counts)


# Export 1: Year-wise counts WITH overlaps
yearly_counts.to_csv("yearly_counts_with_overlaps.csv", index=False)

# Export 2: Year-wise counts based on FIRST appearance only
first_year_counts.to_csv("yearly_counts_first_appearance.csv", index=False)

# Export 3: Final case status (from last hearing per matter)
status_df = status_counts.reset_index()
status_df.columns = ['case_status', 'count']
status_df.to_csv("final_case_status_summary.csv", index=False)

print("\n✅ CSVs exported:")
print("- yearly_counts_with_overlaps.csv")
print("- yearly_counts_first_appearance.csv")
print("- final_case_status_summary.csv")
