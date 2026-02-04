#!/usr/bin/env python3
"""
Fetch data and create balanced sample across Rndrng_Prvdr_Type
10,000 records balanced across 94 types = ~106 records per type
"""

import requests
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# ============================================================================
# CONFIGURATION
# ============================================================================
SUPABASE_CONNECTION_STRING = "postgresql://postgres:S1OK1u7=AfHd@db.sgrimhjfcvifsdyuymiq.supabase.co:5432/postgres"
API_URL = "https://data.cms.gov/data-api/v1/dataset/0e9f2f2b-7bf9-451a-912c-e02e654dd725/data" # 2023
DESTINATION_TABLE = "balanced_physician_data"
TARGET_TOTAL_RECORDS = 40000
BALANCE_COLUMN = "Rndrng_Prvdr_Type"
NUM_UNIQUE_TYPES = 104

# Calculate records per type
RECORDS_PER_TYPE = TARGET_TOTAL_RECORDS // NUM_UNIQUE_TYPES
EXTRA_RECORDS = TARGET_TOTAL_RECORDS % NUM_UNIQUE_TYPES

print(f"Target: {TARGET_TOTAL_RECORDS:,} balanced records")
print(f"Types: {NUM_UNIQUE_TYPES}")
print(f"Records per type: {RECORDS_PER_TYPE} (with {EXTRA_RECORDS} extras)")

engine = create_engine(SUPABASE_CONNECTION_STRING)



# ============================================================================
# STEP 1: Fetch data until we have enough of each type
# ============================================================================

print("\nFetching data...")

# Track how many we have of each type
type_counts = {}
all_data_by_type = {}  # Store records grouped by type

offset = 0
batch_size = 1000
total_fetched = 0
total_valid = 0
total_filtered = 0
complete = False

while not complete:
    response = requests.get(API_URL, params={'size': batch_size, 'offset': offset})
    batch = response.json()
    
    if not batch:
        print("⚠️  Reached end of API data")
        break
    
    total_fetched += len(batch)
    
    # Group records by type (only valid ones)
    for row in batch:
        
        total_valid += 1
        provider_type = row.get(BALANCE_COLUMN, "Unknown")  
        
        # Initialize if new type
        if provider_type not in all_data_by_type:
            all_data_by_type[provider_type] = []
            type_counts[provider_type] = 0
        
        # Only add if we still need more of this type
        if type_counts[provider_type] < RECORDS_PER_TYPE:
            all_data_by_type[provider_type].append(row)
            type_counts[provider_type] += 1
    
    # Check if we have enough of all types
    min_count = min(type_counts.values()) if type_counts else 0
    
    if total_fetched % 10000 == 0:
        print(f"   Fetched: {total_fetched:,} | Valid: {total_valid:,} | Filtered: {total_filtered:,} | Types: {len(type_counts)} | Min/type: {min_count}")
    
    # Stop when all types have enough records
    if min_count >= RECORDS_PER_TYPE and len(type_counts) >= NUM_UNIQUE_TYPES:
        print(f"\n✅ Found enough valid records for all types!")
        complete = True
        break
    
    offset += batch_size

print(f"\nTotal fetched: {total_fetched:,}")
print(f"Valid records: {total_valid:,} ({total_valid/total_fetched*100:.1f}%)")
print(f"Filtered out: {total_filtered:,} ({total_filtered/total_fetched*100:.1f}%)")
print(f"Unique types found: {len(type_counts)}")

# ============================================================================
# STEP 2: Create balanced sample
# ============================================================================

print("\nCreating balanced sample...")

balanced_data = []

# Take equal amounts from each type
for provider_type, records in all_data_by_type.items():
    # Take exactly RECORDS_PER_TYPE from each
    sample_size = min(RECORDS_PER_TYPE, len(records))
    balanced_data.extend(records[:sample_size])

# Add extra records to reach exactly 10,000 (if needed)
if len(balanced_data) < TARGET_TOTAL_RECORDS:
    extras_needed = TARGET_TOTAL_RECORDS - len(balanced_data)
    print(f"   Adding {extras_needed} extra records to reach target...")
    
    # Add extras from types with available records
    for provider_type, records in all_data_by_type.items():
        if extras_needed == 0:
            break
        if len(records) > RECORDS_PER_TYPE:
            available = records[RECORDS_PER_TYPE:]
            to_add = min(extras_needed, len(available))
            balanced_data.extend(available[:to_add])
            extras_needed -= to_add

print(f"✅ Balanced sample created: {len(balanced_data):,} records")

# ============================================================================
# STEP 3: Verify balance
# ============================================================================

df = pd.DataFrame(balanced_data)

print("\nBalance check:")
type_distribution = df[BALANCE_COLUMN].value_counts()
print(f"   Types in sample: {len(type_distribution)}")
print(f"   Min per type: {type_distribution.min()}")
print(f"   Max per type: {type_distribution.max()}")
print(f"   Mean per type: {type_distribution.mean():.1f}")

# Show top 10 types
print("\nTop 10 provider types:")
print(type_distribution.head(10))

# ============================================================================
# STEP 4: Upload to Supabase
# ============================================================================

print(f"\nUploading {len(df):,} records to {DESTINATION_TABLE}...")

df.to_sql(
    DESTINATION_TABLE,
    engine,
    if_exists='replace',
    index=False,
    chunksize=1000
)

print(f"✅ Uploaded successfully!")

# Verify in database
count = pd.read_sql(f"SELECT COUNT(*) as count FROM {DESTINATION_TABLE}", engine)
print(f"✅ Verified: {count['count'][0]:,} rows in database")

# Show distribution in database
dist = pd.read_sql(f'''
    SELECT "{BALANCE_COLUMN}", COUNT(*) as count
    FROM {DESTINATION_TABLE}
    GROUP BY "{BALANCE_COLUMN}"
    ORDER BY count DESC
    LIMIT 10
''', engine)
print("\nDistribution in database:")
print(dist.to_string(index=False))