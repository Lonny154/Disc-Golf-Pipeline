# ExploratoryAnalysis.py - script to do EDA (exploratory data analysis) on the two primary data sets for the project. 

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

putting_df = spark.read.format("delta").load(uri + "PDGA_project/Putting")

display(putting_df)

weather_df = spark.read.format("delta").load(uri + "PDGA_project/Weather_ingestion")

display(weather_df)

from pyspark.sql.functions import col, sum as _sum, isnan, when

missing_count = weather_df.select([
    _sum(when(col(c).isNull() | isnan(col(c)), 1).otherwise(0))
    for c in weather_df.columns
]).rdd.flatMap(lambda x: x).sum()

if missing_count > 0:
    print(f"There are {missing_count} missing values in the DataFrame.")
else:
    print("No missing values found.")

    from pyspark.sql.functions import col, sum as _sum, isnan, when
from pyspark.sql.types import NumericType

missing_counts = putting_df.select([
    _sum(
        when(
            col(c).isNull() | 
            (isnan(col(c)) if isinstance(putting_df.schema[c].dataType, NumericType) else False),
            1
        ).otherwise(0)
    ).alias(c)
    for c in putting_df.columns
])

missing_counts.show()

putting_df.describe('C1X_Putt_Pct').show()
putting_df.describe('C2_Putt_Pct').show()
putting_df.describe('SG_Putting').show()   