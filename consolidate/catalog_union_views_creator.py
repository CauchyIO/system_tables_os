# Databricks notebook source

# COMMAND ----------

import sys
sys.path.append("..")
from utils import UnionViewsConfig, load_config_from_yaml
from table_registry import get_table_definition, TableRegionality

# COMMAND ----------

dbutils.widgets.text("source_catalogs", "", "Source Catalogs (comma-separated)")
dbutils.widgets.text("target_catalog", "", "Target Catalog for Views")
dbutils.widgets.dropdown("create_catalog", "false", ["true", "false"], "Create Catalog")
dbutils.widgets.text("excluded_schemas", "information_schema,metadata", "Excluded Schemas (comma-separated)")
dbutils.widgets.text("config_path", "", "YAML Config Path (optional)")

# COMMAND ----------

config_path = dbutils.widgets.get("config_path")

if config_path:
    yaml_config = load_config_from_yaml(config_path)
    config = UnionViewsConfig(**yaml_config)
else:
    sources = dbutils.widgets.get("source_catalogs")
    excluded = dbutils.widgets.get("excluded_schemas")
    config = UnionViewsConfig(
        source_catalogs=[s.strip() for s in sources.split(",")],
        target_catalog=dbutils.widgets.get("target_catalog"),
        create_catalog=dbutils.widgets.get("create_catalog") == "true",
        excluded_schemas=[s.strip() for s in excluded.split(",")] if excluded else [],
    )

print(f"source_catalogs: {config.source_catalogs}")
print(f"target_catalog: {config.target_catalog}")
print(f"create_catalog: {config.create_catalog}")

# COMMAND ----------

assert len(config.source_catalogs) >= 1, "At least one source catalog is required"

for catalog in config.source_catalogs:
    catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
    assert catalog in catalogs, f"Source catalog '{catalog}' does not exist"

# COMMAND ----------

if config.create_catalog:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.target_catalog}")
else:
    catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
    assert config.target_catalog in catalogs, f"Target catalog '{config.target_catalog}' does not exist"

# COMMAND ----------

primary_catalog = config.source_catalogs[0]

schemas = spark.sql(f"SHOW SCHEMAS IN `{primary_catalog}`").collect()
schema_names = [
    row.databaseName
    for row in schemas
    if row.databaseName not in config.excluded_schemas
    and not row.databaseName.startswith("__")
]

tables = []
for schema in schema_names:
    schema_tables = spark.sql(f"SHOW TABLES IN `{primary_catalog}`.{schema}").collect()
    for table in schema_tables:
        if not table.tableName.startswith("__"):
            tables.append({"schema": schema, "table": table.tableName})

print(f"Discovered {len(tables)} tables from {primary_catalog}")

# COMMAND ----------

def analyze_schemas(catalogs, schema, table):
    schemas_by_catalog = {}
    valid_catalogs = []
    for catalog in catalogs:
        if spark.catalog.tableExists(f"`{catalog}`.{schema}.{table}"):
            df = spark.table(f"`{catalog}`.{schema}.{table}")
            schemas_by_catalog[catalog] = {f.name: f.dataType for f in df.schema.fields}
            valid_catalogs.append(catalog)

    if not valid_catalogs:
        return None, None, None, None

    all_columns = []
    seen = set()
    for catalog in valid_catalogs:
        for col in schemas_by_catalog[catalog].keys():
            if col not in seen:
                all_columns.append(col)
                seen.add(col)

    mismatched = set()
    for col in all_columns:
        types = [schemas_by_catalog[c].get(col) for c in valid_catalogs if col in schemas_by_catalog[c]]
        if len(set(str(t) for t in types)) > 1:
            mismatched.add(col)

    return all_columns, mismatched, schemas_by_catalog, valid_catalogs

def get_select_for_catalog(catalog, schema, table, all_columns, mismatched_columns, schemas_by_catalog):
    catalog_columns = schemas_by_catalog[catalog]
    select_parts = []
    for col in all_columns:
        if col not in catalog_columns:
            select_parts.append(f"NULL as {col}")
        elif col in mismatched_columns:
            select_parts.append(f"to_json({col}) as {col}")
        else:
            select_parts.append(col)
    return f"SELECT {', '.join(select_parts)} FROM `{catalog}`.{schema}.{table}"

created_views = []

for table_info in tables:
    schema = table_info["schema"]
    table = table_info["table"]

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.target_catalog}.{schema}")

    view_name = f"{config.target_catalog}.{schema}.{table}"

    table_path = f"system.{schema}.{table}"
    definition = get_table_definition(table_path)

    if definition and definition.regionality == TableRegionality.GLOBAL:
        primary_source = f"`{primary_catalog}`.{schema}.{table}"
        if spark.catalog.tableExists(primary_source):
            spark.sql(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {primary_source}")
            created_views.append(view_name)
        continue

    all_columns, mismatched, schemas_by_catalog, valid_catalogs = analyze_schemas(config.source_catalogs, schema, table)

    if not valid_catalogs:
        print(f"Skipping {view_name}: table not found in any catalog")
        continue

    union_parts = [
        get_select_for_catalog(catalog, schema, table, all_columns, mismatched, schemas_by_catalog)
        for catalog in valid_catalogs
    ]
    union_sql = " UNION ALL ".join(union_parts)

    spark.sql(f"CREATE OR REPLACE VIEW {view_name} AS {union_sql}")

    created_views.append(view_name)

    if mismatched:
        print(f"{view_name}: converted {len(mismatched)} columns to JSON: {mismatched}")

print(f"Created {len(created_views)} views")

# COMMAND ----------

for view_name in created_views[:5]:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {view_name}").collect()[0].cnt
    print(f"{view_name}: {count:,} rows")

print(f"Validated sample of {min(5, len(created_views))} views")
