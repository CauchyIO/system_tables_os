# Databricks notebook source

# COMMAND ----------

import sys
sys.path.append("..")
from utils import DataCopierConfig, load_config_from_yaml, get_timestamp_column

# COMMAND ----------

from pyspark.sql.functions import col, lit
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("target_catalog", "", "Target Catalog Name")
dbutils.widgets.dropdown("create_catalog", "false", ["true", "false"], "Create Catalog")
dbutils.widgets.dropdown("force_full_refresh", "false", ["true", "false"], "Force Full Refresh")
dbutils.widgets.text("excluded_schemas", "information_schema,metadata", "Excluded Schemas (comma-separated)")
dbutils.widgets.text("frequency_hours", "6", "Sync Frequency (hours)")
dbutils.widgets.text("config_path", "", "YAML Config Path (optional)")

# COMMAND ----------

config_path = dbutils.widgets.get("config_path")

if config_path:
    yaml_config = load_config_from_yaml(config_path)
    config = DataCopierConfig(**yaml_config)
else:
    excluded = dbutils.widgets.get("excluded_schemas")
    config = DataCopierConfig(
        target_catalog=dbutils.widgets.get("target_catalog"),
        create_catalog=dbutils.widgets.get("create_catalog") == "true",
        force_full_refresh=dbutils.widgets.get("force_full_refresh") == "true",
        excluded_schemas=[s.strip() for s in excluded.split(",")] if excluded else [],
        frequency_hours=int(dbutils.widgets.get("frequency_hours")),
    )

print(f"Config: target={config.target_catalog}, frequency={config.frequency_hours}h")

# COMMAND ----------

if config.create_catalog:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.target_catalog}")
else:
    catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
    assert config.target_catalog in catalogs, f"Catalog '{config.target_catalog}' does not exist"

# COMMAND ----------

control_table_name = f"{config.target_catalog}.metadata.sync_control"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.target_catalog}.metadata")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {control_table_name} (
        schema_name STRING,
        table_name STRING,
        full_table_name STRING,
        last_sync_timestamp TIMESTAMP,
        last_sync_mode STRING,
        sync_status STRING,
        updated_at TIMESTAMP,
        PRIMARY KEY (schema_name, table_name)
    ) USING DELTA
""")

# COMMAND ----------

system_schemas = spark.sql("SHOW SCHEMAS IN system").collect()
schema_names = [
    row.databaseName
    for row in system_schemas
    if row.databaseName not in config.excluded_schemas
    and not row.databaseName.startswith("__")
]

system_tables = []
for schema in schema_names:
    tables = spark.sql(f"SHOW TABLES IN system.{schema}").collect()
    for table in tables:
        system_tables.append({
            "schema": schema,
            "table": table.tableName,
            "full_name": f"system.{schema}.{table.tableName}",
        })

print(f"Discovered {len(system_tables)} system tables")

# COMMAND ----------

processed_tables = []

for table_info in system_tables:
    schema = table_info["schema"]
    table = table_info["table"]
    full_table_name = table_info["full_name"]
    target_table_name = f"{config.target_catalog}.{schema}.{table}"

    source_df = spark.table(full_table_name)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.target_catalog}.{schema}")

    last_sync_df = spark.sql(f"""
        SELECT last_sync_timestamp, last_sync_mode
        FROM {control_table_name}
        WHERE schema_name = '{schema}' AND table_name = '{table}'
    """).collect()

    is_first_run = len(last_sync_df) == 0 or config.force_full_refresh
    last_sync_timestamp = None if is_first_run else last_sync_df[0].last_sync_timestamp

    timestamp_col = get_timestamp_column(source_df)

    if is_first_run:
        (
            source_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table_name)
        )
        sync_mode = "FULL"

    elif timestamp_col is None:
        (
            source_df.write.format("delta")
            .mode("overwrite")
            .saveAsTable(target_table_name)
        )
        sync_mode = "FULL_FALLBACK"

    else:
        new_records_df = source_df.filter(col(timestamp_col) > lit(last_sync_timestamp))
        (
            new_records_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(target_table_name)
        )
        sync_mode = "INCREMENTAL"

    current_time = datetime.now()
    spark.sql(f"""
        MERGE INTO {control_table_name} AS target
        USING (SELECT
            '{schema}' as schema_name,
            '{table}' as table_name,
            '{full_table_name}' as full_table_name,
            timestamp'{current_time}' as last_sync_timestamp,
            '{sync_mode}' as last_sync_mode,
            'SUCCESS' as sync_status,
            timestamp'{current_time}' as updated_at
        ) AS source
        ON target.schema_name = source.schema_name
            AND target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET
            full_table_name = source.full_table_name,
            last_sync_timestamp = source.last_sync_timestamp,
            last_sync_mode = source.last_sync_mode,
            sync_status = source.sync_status,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (schema_name, table_name, full_table_name, last_sync_timestamp, last_sync_mode, sync_status, updated_at)
            VALUES (source.schema_name, source.table_name, source.full_table_name, source.last_sync_timestamp, source.last_sync_mode, source.sync_status, source.updated_at)
    """)

    processed_tables.append({
        "table": target_table_name,
        "mode": sync_mode,
    })

print(f"Processed {len(processed_tables)} tables")

# COMMAND ----------

for table_info in processed_tables:
    if table_info["mode"] in ["FULL", "FULL_FALLBACK"]:
        spark.sql(f"OPTIMIZE {table_info['table']}")

# COMMAND ----------

full_loads = sum(1 for t in processed_tables if "FULL" in t["mode"])
incremental_loads = sum(1 for t in processed_tables if t["mode"] == "INCREMENTAL")

print(f"Full loads: {full_loads}, Incremental loads: {incremental_loads}")
