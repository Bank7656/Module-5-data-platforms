"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: vendor_id
    type: integer
  - name: pickup_datetime
    type: timestamp
  - name: dropoff_datetime
    type: timestamp
  - name: passenger_count
    type: float
  - name: trip_distance
    type: float
  - name: ratecode_id
    type: float
  - name: store_and_fwd_flag
    type: string
  - name: pu_location_id
    type: integer
  - name: do_location_id
    type: integer
  - name: payment_type
    type: integer
  - name: fare_amount
    type: float
  - name: extra
    type: float
  - name: mta_tax
    type: float
  - name: tip_amount
    type: float
  - name: tolls_amount
    type: float
  - name: improvement_surcharge
    type: float
  - name: total_amount
    type: float
  - name: congestion_surcharge
    type: float
  - name: taxi_type
    type: string
  - name: extracted_at
    type: timestamp

@bruin"""

import os
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

def materialize():
    # Read environment variables
    start_date_str = os.environ.get("BRUIN_START_DATE")
    end_date_str = os.environ.get("BRUIN_END_DATE")
    
    if not start_date_str or not end_date_str:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE must be set.")

    # Read pipeline variables
    try:
        import json
        bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    except Exception:
        bruin_vars = {}
        
    taxi_types = bruin_vars.get("taxi_types", ["yellow", "green"])
    
    # Parse dates
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Generate list of months between start and end date
    months = []
    curr = start_dt.replace(day=1)
    while curr <= end_dt:
        months.append(curr.strftime("%Y-%m"))
        curr += relativedelta(months=1)

    all_dfs = []
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    extracted_at = datetime.now()

    for taxi_type in taxi_types:
        for month in months:
            file_url = f"{base_url}/{taxi_type}_tripdata_{month}.parquet"
            print(f"Fetching data from: {file_url}")
            
            try:
                # Read parquet directly into pandas
                df = pd.read_parquet(file_url)
                
                # Normalize column names to snake_case (matching what dlt did)
                # Helper for specific NYC Taxi columns
                renames = {
                    'VendorID': 'vendor_id',
                    'RatecodeID': 'ratecode_id',
                    'PULocationID': 'pu_location_id',
                    'DOLocationID': 'do_location_id',
                    'tpep_pickup_datetime': 'pickup_datetime', # Yellow
                    'tpep_dropoff_datetime': 'dropoff_datetime', # Yellow
                    'lpep_pickup_datetime': 'pickup_datetime', # Green
                    'lpep_dropoff_datetime': 'dropoff_datetime', # Green
                }
                
                df["taxi_type"] = taxi_type
                df["extracted_at"] = extracted_at
                
                dfs.append(df)
            else:
                print(f"Failed to fetch {url}: Status code {response.status_code}")
        
        current_date += relativedelta(months=1)
    
    if not dfs:
        return pd.DataFrame()
        
    return pd.concat(dfs, ignore_index=True)
