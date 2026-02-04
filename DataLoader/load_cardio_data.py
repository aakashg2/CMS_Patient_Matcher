#!/usr/bin/env python3
"""
Optimized for millions of IDs using integer sets
"""

import requests
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

SUPABASE_CONNECTION_STRING = "postgresql://postgres:S1OK1u7=AfHd@db.sgrimhjfcvifsdyuymiq.supabase.co:5432/postgres"
NEW_API_URL = "https://data.cms.gov/data-api/v1/dataset/8ba584c6-a43a-4b0b-a35a-eb9a59e3a571/data?filter[Rndrng_Prvdr_Type]=Cardiology"
ID_COLUMN = "Rndrng_NPI"  # Change to actual column name

engine = create_engine(SUPABASE_CONNECTION_STRING)

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
        if row.get("Rndrng_Prvdr_Type") == "Cardiology"
    ]
    
    total_matched += len(filtered)
    all_data.extend(filtered)
    
    if total_fetched % 10000 == 0:
        print(f"   Fetched: {total_fetched:,} | Matched: {total_matched:,} ({total_matched/total_fetched*100:.1f}%)")
    if total_matched > 20000:
        print("Collected everything")
    offset += batch_size
print(f"\n✅ Complete: {total_matched:,} / {total_fetched:,} rows matched")

# Step 3: Upload filtered data
if all_data:
    df = pd.DataFrame(all_data)
    df.to_sql('cardiologist_data_full', engine, if_exists='replace', index=False, chunksize=1000)
    print(f"✅ Uploaded {len(df):,} rows")