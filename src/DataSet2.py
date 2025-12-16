# DataSet2.py - script to extract data from its source and load into ADLS.

import os
from sshtunnel import SSHTunnelForwarder
import pymysql
from dotenv import load_dotenv
load_dotenv()


ssh_host = os.getenv("SSH_HOST")
ssh_user = os.getenv("SSH_USER")
ssh_key = os.getenv("SSH_KEY_PATH")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
db_name = os.getenv("DB_NAME")

with SSHTunnelForwarder(
    (ssh_host, 22),
    ssh_username=ssh_user,
    ssh_pkey=ssh_key,
    remote_bind_address=("127.0.0.1", 3306)
) as tunnel:
    conn = pymysql.connect(
        host="127.0.0.1",
        port=tunnel.local_bind_port,
        user=db_user,
        password=db_pass,
        db=db_name
    )


SET @div := 'MPO', @tour := 'DGPT', @season := 2024;

SELECT
    e.pdga_event_id,
    e.name AS event_name,
    e.end_date,
    CONCAT(IFNULL(p.first_name,''), ' ', IFNULL(p.last_name,'')) AS player_name,
    r.place,
    r.evt_rating,
    
    -- Core putting stats
    ROUND(SUM(CASE WHEN agg.stat_id = 7 THEN total_stat_count / total_opp_count * 100 ELSE 0 END), 2) AS C1X_Putt_Pct,
    ROUND(SUM(CASE WHEN agg.stat_id = 8 THEN total_stat_count / total_opp_count * 100 ELSE 0 END), 2) AS C2_Putt_Pct,
    ROUND(SUM(CASE WHEN agg.stat_id = 101 THEN total_stat_value ELSE 0 END), 2) AS SG_Putting,
    
    -- Optional for context
    ROUND(SUM(CASE WHEN agg.stat_id = 9 THEN total_stat_count / total_opp_count * 18 ELSE 0 END), 2) AS OB_per_18,
    ROUND(SUM(CASE WHEN agg.stat_id = 16 THEN total_stat_value / total_opp_count ELSE 0 END), 2) AS Avg_Putt_Distance

FROM (
    SELECT
        rd.division,
        rd.player_id,
        rd.pdga_number,
        rd.pdga_event_id,
        erp.stat_id,
        SUM(erp.value) AS total_stat_value,
        SUM(erp.stat_count) AS total_stat_count,
        SUM(erp.opportunity_count) AS total_opp_count
    FROM event_round_player_stats erp
    JOIN event_rounds rd ON rd.id = erp.event_round_id
    WHERE
        rd.division = @div
        AND rd.event_id IN (
            SELECT DISTINCT event_id
            FROM view_tour_events
            WHERE tour = @tour AND season >= @season
        )
        AND rd.round < 13
    GROUP BY rd.division, rd.pdga_event_id, rd.player_id, rd.pdga_number, erp.stat_id
) agg
JOIN events e ON e.pdga_event_id = agg.pdga_event_id
JOIN event_results r ON (r.event_id = e.id AND r.pdga_number = agg.pdga_number)
LEFT JOIN players p ON p.id = agg.player_id

GROUP BY
    e.pdga_event_id, e.name, e.end_date, r.place, r.evt_rating, p.first_name, p.last_name, agg.pdga_number
ORDER BY
    e.end_date DESC, r.place ASC;


print("DataSet2 ingestion")

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

tables = spark.sql("SHOW TABLES IN default")
display(tables)

pdga_putting_stats = spark.table("pdga_puttings_stats")
display(pdga_putting_stats)

spark_putting = pdga_putting_stats
spark_putting.write.mode("overwrite").format("delta").save(uri + "PDGA_project/Bronze")
