# System Tables

Multi-tenant system table views framework for Databricks. Aggregate system table data across accounts and distribute filtered views to business units.

## Repository Structure

```
system_tables/
├── utils.py                    # Shared Pydantic models + helpers
├── consolidate/                # Cross-account data aggregation
│   ├── system_table_data_copier.py
│   └── catalog_union_views_creator.py
├── distribute/                 # BU/workspace-specific views
│   └── system_table_views_creator.py
├── configs/
│   └── example/
├── dashboards/
│   ├── databricks.yml          # Dashboards bundle
│   └── *.lvdash.json
└── databricks.yml              # Jobs bundle (copier + union views)
```

## Workflows

### Consolidate

Aggregate system table data across Databricks accounts via delta sharing and union views.

1. **system_table_data_copier.py** - Copy system tables to a catalog for delta sharing. Supports incremental sync via timestamp columns.
2. Delta share the catalog to other accounts
3. **catalog_union_views_creator.py** - Create unified views from N source catalogs (local + delta-shared)

### Distribute

Create filtered views for specific workspaces or business units from consolidated or local system tables.

1. **system_table_views_creator.py** - Create filtered views per workspace/BU based on workspace_id

## User Flow

1. Account admin clones the repo
2. Configure YAMLs according to in-house naming convention
3. Deploy code + dashboards via DAB (`databricks bundle deploy`)
4. (Optional) Find+replace values in dashboards to create BU-specific dashboards
5. (Optional) Distribute dashboards to teams

## Configuration

All notebooks support two configuration methods:
- **Widget parameters** as defaults
- **YAML file** as optional override

### Example: views.yaml

```yaml
workspace_sql_filter: "workspace_name RLIKE '.*dev.*'"
target_group: "finance_analysts"
target_catalog: "finance_system_views"
create_catalog: true
```

### Example: data_copier.yaml

```yaml
target_catalog: "system_table_data"
create_catalog: true
force_full_refresh: false
```

### Example: union_views.yaml

```yaml
source_catalogs:
  - "system"
  - "cauchy-us"
  - "partner-shared"
target_catalog: "system_union"
create_catalog: true
```

## Pydantic Models

### ViewsConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| workspace_sql_filter | str | required | SQL WHERE clause to filter workspaces (e.g. `workspace_name RLIKE '.*dev.*'`) |
| target_group | str | required | Group to grant access |
| target_catalog | str | required | Target catalog for views |
| create_catalog | bool | false | Create catalog if not exists |

### DataCopierConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| target_catalog | str | required | Target catalog for copied tables |
| create_catalog | bool | false | Create catalog if not exists |
| force_full_refresh | bool | false | Force full table refresh |
| excluded_schemas | list[str] | ["information_schema", "metadata"] | Schemas to exclude |

### UnionViewsConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| source_catalogs | list[str] | required | List of catalogs to union |
| target_catalog | str | required | Target catalog for views |
| create_catalog | bool | false | Create catalog if not exists |
| excluded_schemas | list[str] | ["information_schema", "metadata"] | Schemas to exclude |

## Deployment

The project uses two separate DAB bundles: one for jobs and one for dashboards.

### Jobs

```bash
databricks bundle deploy -t default              # data copier
databricks bundle deploy -t union_workspace       # union views
```

### Dashboards

Dashboards have their own bundle in `dashboards/`. The `dataset_catalog` variable controls which catalog the dashboard queries resolve against (default: `system_union`).

```bash
cd dashboards && databricks bundle deploy
```

Override the catalog for a different environment:

```bash
cd dashboards && databricks bundle deploy --var dataset_catalog=my_catalog
```

| Variable | Default | Description |
|----------|---------|-------------|
| dataset_catalog | system_union | Default catalog for dashboard queries |
| warehouse_id | feea94c5c21202c4 | SQL Warehouse ID |
