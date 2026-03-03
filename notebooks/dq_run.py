# Databricks / PySpark Execution Notebook
# This notebook runs the Data Quality Framework end-to-end

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import json
import uuid
from datetime import datetime

# Import DQ Engine
from pyspark.dq_engine import DataQualityEngine

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------
# CONFIGURATION
# ---------------------------------------------

dataset_name = "customer_orders"
rules_path = "/dbfs/FileStore/rules/rule_config.json"  # adjust path
source_path = "/mnt/raw/customer_orders/"               # adjust path

run_id = str(uuid.uuid4())
start_time = datetime.utcnow()

# ---------------------------------------------
# LOAD DATA
# ---------------------------------------------

df = spark.read.format("delta").load(source_path)

print(f"Loaded dataset: {dataset_name}")
print(f"Record count: {df.count()}")

# ---------------------------------------------
# LOAD RULE CONFIG
# ---------------------------------------------

with open(rules_path, "r") as f:
    rules_config = json.load(f)

# ---------------------------------------------
# RUN DQ ENGINE
# ---------------------------------------------

engine = DataQualityEngine(spark)
dq_output = engine.run(df, rules_config)

rule_results_df = dq_output["rule_results_df"]
failed_df = dq_output["failed_records_df"]
passed_df = dq_output["passed_records_df"]
dq_score = dq_output["dq_score"]

print(f"DQ Score: {dq_score}")

# ---------------------------------------------
# WRITE OUTPUTS
# ---------------------------------------------

# 1️⃣ Write passed records
passed_df.write.mode("overwrite").format("delta").save("/mnt/curated/customer_orders_clean")

# 2️⃣ Write failed records (quarantine)
failed_df.write.mode("overwrite").format("delta").save("/mnt/quarantine/customer_orders_failed")

# 3️⃣ Write rule results
rule_results_df.write.mode("append").format("delta").save("/mnt/dq_logs/rule_results")

# ---------------------------------------------
# LOG RUN TO SQL (Optional)
# ---------------------------------------------

end_time = datetime.utcnow()

dq_run_log = spark.createDataFrame(
    [(dataset_name, run_id, start_time, end_time, dq_score, "SUCCESS")],
    ["dataset_name", "run_id", "start_time_utc", "end_time_utc", "dq_score", "status"]
)

dq_run_log.write \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://<server>.database.windows.net:1433;database=<db>") \
    .option("dbtable", "dbo.dq_run_log") \
    .option("user", "<username>") \
    .option("password", "<password>") \
    .mode("append") \
    .save()

print("DQ execution completed successfully.")
