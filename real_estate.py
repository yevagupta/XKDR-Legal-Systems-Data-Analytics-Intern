import pandas as pd
from db_connection import DataManager

# Step 1: Load data
data_manager = DataManager()
queries = """
SELECT * 
FROM lsd.matters
WHERE court_id = 1
  AND filing_date BETWEEN DATE '2021-01-01' AND DATE '2024-12-31'
  AND EXISTS (
    SELECT 1 
    FROM unnest(respondents) AS r
    WHERE r ILIKE ANY (ARRAY[
      '%builder%',
      '%developer%',
      '%realty%',
      '%housing%',
      '%society%',
      '%construction%',
      '%home%',
      '%real estate%'
    ])
  );
"""
df1 = data_manager.execute_query(queries)

# Step 2: Keep only necessary columns
df1 = df1[['matter_id','court_id','filing_date','petitioners','respondents']]
print(df1)
unique_matter_ids = df1['matter_id'].nunique()
print(f"Number of unique matter_ids: {unique_matter_ids}")
#df1.to_csv("bhc_real_estate.csv", index=False)

queries = """
SELECT * 
FROM lsd.matters
WHERE court_id = 14
  AND filing_date BETWEEN DATE '2021-01-01' AND DATE '2024-12-31'
  AND EXISTS (
    SELECT 1 
    FROM unnest(respondents) AS r 
    WHERE r ILIKE ANY (ARRAY[
      '%builder%',
      '%developer%',
      '%realty%',
      '%housing%',
      '%society%',
      '%construction%',
      '%home%',
      '%real estate%'
    ])
  );
"""
df2 = data_manager.execute_query(queries)

# Step 2: Keep only necessary columns
df2 = df2[['matter_id','court_id','filing_date','petitioners','respondents']]
print(df2)
unique_matter_ids = df2['matter_id'].nunique()
print(f"Number of unique matter_ids: {unique_matter_ids}")

# Concatenate df1 and df2
df_combined = pd.concat([df1, df2], ignore_index=True)

# Check number of unique matter_ids
unique_matter_ids = df_combined['matter_id'].nunique()
print(f"Total unique matter_ids: {unique_matter_ids}")

# Optional: Save to CSV
df_combined.to_csv("real_estate_petitioners.csv", index=False)

