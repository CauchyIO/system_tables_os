# Databricks notebook source

# COMMAND ----------

import sys
sys.path.append("..")
from utils import ViewsConfig, load_config_from_yaml

# COMMAND ----------

dbutils.widgets.text("workspace_sql_filter", "", "Workspace SQL Filter (e.g. workspace_name RLIKE '.*dev.*')")
dbutils.widgets.text("target_group", "", "Target Group Name")
dbutils.widgets.text("target_catalog", "", "Target Catalog Name")
dbutils.widgets.dropdown("create_catalog", "false", ["true", "false"], "Create Catalog")
dbutils.widgets.text("config_path", "", "YAML Config Path (optional)")

# COMMAND ----------

config_path = dbutils.widgets.get("config_path")

if config_path:
    yaml_config = load_config_from_yaml(config_path)
    config = ViewsConfig(**yaml_config)
else:
    config = ViewsConfig(
        workspace_sql_filter=dbutils.widgets.get("workspace_sql_filter"),
        target_group=dbutils.widgets.get("target_group"),
        target_catalog=dbutils.widgets.get("target_catalog"),
        create_catalog=dbutils.widgets.get("create_catalog") == "true",
    )

print(f"workspace_sql_filter: {config.workspace_sql_filter}")
print(f"target_group: {config.target_group}")
print(f"target_catalog: {config.target_catalog}")
print(f"create_catalog: {config.create_catalog}")

# COMMAND ----------

groups = [row.name for row in spark.sql("SHOW GROUPS").collect()]
assert config.target_group in groups, f"Group '{config.target_group}' does not exist"

# COMMAND ----------

_ = spark.sql("SELECT 1 FROM system.access.workspaces_latest LIMIT 1").collect()

# COMMAND ----------

if config.create_catalog:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.target_catalog}")
else:
    catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
    assert config.target_catalog in catalogs, f"Catalog '{config.target_catalog}' does not exist"

# COMMAND ----------

system_schemas = spark.sql("SHOW SCHEMAS IN system").collect()
schema_names = [row.databaseName for row in system_schemas if row.databaseName != "information_schema"]

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

created_views = []
skipped_tables = []

for table_info in system_tables:
    schema = table_info["schema"]
    table = table_info["table"]
    full_table_name = table_info["full_name"]

    table_columns = spark.table(full_table_name).columns

    if "workspace_id" not in table_columns:
        skipped_tables.append(full_table_name)
        continue

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.target_catalog}.{schema}")

    view_name = f"{config.target_catalog}.{schema}.{table}"

    spark.sql(f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT t.*
        FROM {full_table_name} t
        WHERE t.workspace_id IN (
            SELECT workspace_id
            FROM system.access.workspaces_latest
            WHERE {config.workspace_sql_filter}
        )
    """)

    created_views.append(view_name)

print(f"Created {len(created_views)} views")
print(f"Skipped {len(skipped_tables)} tables (no workspace_id)")

# COMMAND ----------

for view_name in created_views:
    _ = spark.sql(f"SELECT 1 FROM {view_name} LIMIT 1").collect()

print(f"Validated {len(created_views)} views")

# COMMAND ----------

spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {config.target_catalog} TO `{config.target_group}`")
print(f"Granted privileges to {config.target_group}")
