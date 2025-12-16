# DataSet1.py - script to extract data from its source and load into ADLS.

print("DataSet1 ingestion")

dbutils.secrets.listScopes()
my_scope = "DLCSCI622"  
dbutils.secrets.list(my_scope)
my_key = "CSCI-key"  
noaa_token = dbutils.secrets.get(scope="DLCSCI622", key="NOAA-key")
print(dbutils.secrets.get(my_scope, my_key))

storage_end_point = "firstassignmentstore.dfs.core.windows.net"
container_name = "assign1" 

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))


uri = "abfss://" + container_name + "@" + storage_end_point + "/" 
print(uri)

# NOAA API token
token = noaa_token

import requests, time, pandas as pd

def fetch_noaa(station_id, start_date, end_date, token):
    url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
    headers = {"token": token}
    params = {
        "datasetid": "GHCND",
        "stationid": station_id,
        "startdate": start_date,
        "enddate": end_date,
        "units": "metric",
        "limit": 1000
    }

    retries = 5
    delay = 5
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, params=params)
            if r.status_code in [429, 503]:
                print(f"⚠️ NOAA server busy ({r.status_code}). Retry {attempt+1}/{retries} in {delay}s...")
                time.sleep(delay)
                delay *= 2
                continue
            r.raise_for_status()
            results = r.json().get("results", [])
            return pd.DataFrame(results)
        except requests.exceptions.RequestException as e:
            print(f"❌ Error on attempt {attempt+1}/{retries}: {e}")
            time.sleep(delay)
            delay *= 2
    print(f"🚫 Failed to fetch {station_id} from {start_date} to {end_date}")
    return pd.DataFrame()

import os

save_dir = "/dbfs/FileStore/tables"
os.makedirs(save_dir, exist_ok=True)  # ✅ create if missing

save_path = f"{save_dir}/noaa_partial.csv"
df_all.to_csv(save_path, index=False)

print("✅ File saved to:", save_path)

import os


save_path = "/dbfs/FileStore/tables/noaa_partial.csv"


# Try to load any existing partial data
if os.path.exists(save_path):
    df_all = pd.read_csv(save_path)
    fetched = set(df_all["Location"].unique())
    print(f"🔁 Resuming — already have data for: {fetched}")
else:
    df_all = pd.DataFrame()
    fetched = set()

# Loop through remaining requests
for i, row in dfdates.toPandas().iterrows():
    loc = row["Location"]
    if loc in fetched:
        continue  # skip already completed cities

    print(f"🌤️ Fetching data for {loc} ({row['start_date']} → {row['end_date']})")
    df = fetch_noaa(row["station_id"], row["start_date"], row["end_date"], token)
    if not df.empty:
        df["Location"] = loc
        df_all = pd.concat([df_all, df], ignore_index=True)
        # ✅ Save progress after each successful fetch
        df_all.to_csv(save_path, index=False)
    time.sleep(1.5)

print("✅ Finished all available data (with retry & checkpointing).")

display(df_all)
spark_df_all = spark.createDataFrame(df_all)
spark_df_all.write.mode("overwrite").format("delta").save(uri + "PDGA_project/Bronze")
