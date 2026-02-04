#!/usr/bin/env python3
"""
Optimized for matching against a specific pool of IDs
"""

import requests
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

SUPABASE_CONNECTION_STRING = "postgresql://postgres:S1OK1u7=AfHd@db.sgrimhjfcvifsdyuymiq.supabase.co:5432/postgres"
NEW_API_URL = "https://data.cms.gov/data-api/v1/dataset/0e9f2f2b-7bf9-451a-912c-e02e654dd725/data"
ID_COLUMN = "Rndrng_NPI"

engine = create_engine(SUPABASE_CONNECTION_STRING)

# ========================================
# OPTION 1: Load IDs from a CSV file
# ========================================



phycisian_ID = pd.read_sql(
    'SELECT DISTINCT "Rndrng_NPI" ' \
    'FROM "cms_balanced_sample_2023_20K"', 
    engine
)
VALID_IDS = set(phycisian_ID['Rndrng_NPI'].dropna().astype(np.int64))

# ========================================
# Step 2: Fetch and filter against YOUR pool
# ========================================
all_data = []
offset = 0
batch_size = 1000
total_fetched = 0
total_matched = 0

while True:
    response = requests.get(NEW_API_URL, params={'size': batch_size, 'offset': offset})
    batch = response.json()
    
    if not batch:
        break
    
    total_fetched += len(batch)
    
    # Filter: only keep rows where the ID matches YOUR pool
    filtered = [
        row for row in batch 
        if row.get(ID_COLUMN) and int(row[ID_COLUMN]) in VALID_IDS
    ]
    
    total_matched += len(filtered)
    all_data.extend(filtered)
    
    if total_fetched % 10000 == 0:
        print(f"   Fetched: {total_fetched:,} | Matched: {total_matched:,} ({total_matched/total_fetched*100:.1f}%)")
    
    offset += batch_size
    
    # Remove this break if you want to fetch ALL data from the API
    if total_fetched >= 800000:
        print("Should have everything you need now")
        break


# ========================================
# Step 3: Upload filtered data
# ========================================
if all_data:
    df = pd.DataFrame(all_data)
    df.to_sql('physician_procedures', engine, if_exists='replace', index=False, chunksize=1000)
    print(f"Uploaded {len(df):,} rows to the database")
else:
    print("⚠️  No matching rows found!")