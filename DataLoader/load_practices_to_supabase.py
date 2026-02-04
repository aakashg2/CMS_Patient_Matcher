#!/usr/bin/env python3
"""
Optimized for millions of IDs using integer sets
"""

import requests
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

SUPABASE_CONNECTION_STRING = "postgresql://postgres:S1OK1u7=AfHd@db.sgrimhjfcvifsdyuymiq.supabase.co:5432/postgres"
NEW_API_URL = "https://data.cms.gov/data-api/v1/dataset/0e9f2f2b-7bf9-451a-912c-e02e654dd725/data"
ID_COLUMN = "Rndrng_NPI"  # Change to actual column name

engine = create_engine(SUPABASE_CONNECTION_STRING)


print("Loading valid NPIs...")
valid_ids_df = pd.read_sql(
    'SELECT DISTINCT "Rndrng_NPI" FROM cms_provider_data_2023', 
    engine
)


VALID_IDS = set(valid_ids_df['Rndrng_NPI'].dropna().astype(np.int64))
print(f"✅ Loaded {len(VALID_IDS):,} valid NPIs (~{len(VALID_IDS) * 8 / 1_000_000:.0f} MB)")


all_data = []
offset = 0
batch_size = 1000
total_fetched = 0
total_matched = 0

print("\nFetching and filtering...")

while True:
    response = requests.get(NEW_API_URL, params={'size': batch_size, 'offset': offset})
    batch = response.json()
    
    if not batch:
        break
    
    total_fetched += len(batch)
    
    filtered = [
        row for row in batch 
        if row.get(ID_COLUMN) and int(row[ID_COLUMN]) in VALID_IDS
    ]
    
    total_matched += len(filtered)
    all_data.extend(filtered)
    
    if total_fetched % 10000 == 0:
        print(f"   Fetched: {total_fetched:,} | Matched: {total_matched:,} ({total_matched/total_fetched*100:.1f}%)")
    
    offset += batch_size
    if total_fetched >= 80000:
        print("You should be done now")
        break
print(f"\n✅ Complete: {total_matched:,} / {total_fetched:,} rows matched")

# Step 3: Upload filtered data
if all_data:
    df = pd.DataFrame(all_data)
    df.to_sql('physician_practices_2023', engine, if_exists='replace', index=False, chunksize=1000)
    print(f"✅ Uploaded {len(df):,} rows")