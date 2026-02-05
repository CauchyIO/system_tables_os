from enum import Enum
from pydantic import BaseModel, Field


class TableStatus(str, Enum):
    GA = "GA"
    PUBLIC_PREVIEW = "PUBLIC_PREVIEW"
    BETA = "BETA"


class TableRegionality(str, Enum):
    GLOBAL = "GLOBAL"
    REGIONAL = "REGIONAL"
    MIXED = "MIXED"


class SystemColumn(BaseModel):
    name: str
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_table: str | None = None
    foreign_key_column: str | None = None


def _pk(name: str) -> SystemColumn:
    return SystemColumn(name=name, is_primary_key=True)


def _fk(name: str, foreign_key_table: str, foreign_key_column: str | None = None) -> SystemColumn:
    return SystemColumn(
        name=name,
        is_foreign_key=True,
        foreign_key_table=foreign_key_table,
        foreign_key_column=foreign_key_column or name,
    )


class SystemTableDefinition(BaseModel):
    table_path: str
    preview_status: TableStatus
    regionality: TableRegionality
    columns: list[SystemColumn] = Field(default_factory=list)

    @property
    def primary_keys(self) -> list[SystemColumn]:
        return [c for c in self.columns if c.is_primary_key]

    @property
    def foreign_keys(self) -> list[SystemColumn]:
        return [c for c in self.columns if c.is_foreign_key]


SYSTEM_TABLES: dict[str, SystemTableDefinition] = {
    "system.access.audit": SystemTableDefinition(
        table_path="system.access.audit",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.MIXED,
        columns=[
            _pk("event_id"),
            _fk("workspace_id", "system.access.workspaces_latest"),
        ],
    ),
    "system.billing.usage": SystemTableDefinition(
        table_path="system.billing.usage",
        preview_status=TableStatus.GA,
        regionality=TableRegionality.GLOBAL,
        columns=[
            _pk("record_id"),
            _fk("workspace_id", "system.access.workspaces_latest"),
            _fk("sku_name", "system.billing.list_prices"),
            _fk("job_id", "system.lakeflow.jobs"),
            _fk("warehouse_id", "system.compute.warehouses"),
            _fk("cluster_id", "system.compute.clusters"),
            _fk("node_type", "system.compute.node_types"),
            _fk("endpoint_id", "system.serving.served_entities"),
        ],
    ),
    "system.access.clean_room_events": SystemTableDefinition(
        table_path="system.access.clean_room_events",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("event_id"),
        ],
    ),
    "system.compute.clusters": SystemTableDefinition(
        table_path="system.compute.clusters",
        preview_status=TableStatus.GA,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("cluster_id"),
        ],
    ),
    "system.access.column_lineage": SystemTableDefinition(
        table_path="system.access.column_lineage",
        preview_status=TableStatus.GA,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _fk("entity_id", "system.access.table_lineage"),
        ],
    ),
    "system.data_classification.results": SystemTableDefinition(
        table_path="system.data_classification.results",
        preview_status=TableStatus.BETA,
        regionality=TableRegionality.REGIONAL,
    ),
    "system.data_quality_monitoring.table_results": SystemTableDefinition(
        table_path="system.data_quality_monitoring.table_results",
        preview_status=TableStatus.BETA,
        regionality=TableRegionality.REGIONAL,
    ),
    "system.access.assistant_events": SystemTableDefinition(
        table_path="system.access.assistant_events",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("event_id"),
            _fk("workspace_id", "system.access.workspaces_latest"),
        ],
    ),
    "system.sharing.materialization_history": SystemTableDefinition(
        table_path="system.sharing.materialization_history",
        preview_status=TableStatus.GA,
        regionality=TableRegionality.MIXED,
    ),
    "system.lakeflow.job_run_timeline": SystemTableDefinition(
        table_path="system.lakeflow.job_run_timeline",
        preview_status=TableStatus.GA,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("run_id"),
            _fk("job_id", "system.lakeflow.jobs"),
        ],
    ),
    "system.lakeflow.job_task_run_timeline": SystemTableDefinition(
        table_path="system.lakeflow.job_task_run_timeline",
        preview_status=TableStatus.GA,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _fk("job_id", "system.lakeflow.jobs"),
            _fk("task_key", "system.lakeflow.job_tasks"),
            _fk("job_run_id", "system.lakeflow.job_run_timeline", "run_id"),
        ],
    ),
    "system.lakeflow.job_tasks": SystemTableDefinition(
        table_path="system.lakeflow.job_tasks",
        preview_status=TableStatus.GA,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("task_key"),
            _fk("job_id", "system.lakeflow.jobs"),
        ],
    ),
    "system.lakeflow.jobs": SystemTableDefinition(
        table_path="system.lakeflow.jobs",
        preview_status=TableStatus.GA,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("job_id"),
        ],
    ),
    "system.marketplace.listing_funnel_events": SystemTableDefinition(
        table_path="system.marketplace.listing_funnel_events",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("provider_id"),
            _pk("listing_id"),
        ],
    ),
    "system.marketplace.listing_access_events": SystemTableDefinition(
        table_path="system.marketplace.listing_access_events",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _fk("provider_id", "system.marketplace.listing_funnel_events"),
            _fk("listing_id", "system.marketplace.listing_funnel_events"),
        ],
    ),
    "system.mlflow.experiments_latest": SystemTableDefinition(
        table_path="system.mlflow.experiments_latest",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("experiment_id"),
        ],
    ),
    "system.mlflow.runs_latest": SystemTableDefinition(
        table_path="system.mlflow.runs_latest",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("run_id"),
            _fk("experiment_id", "system.mlflow.experiments_latest"),
        ],
    ),
    "system.mlflow.run_metrics_history": SystemTableDefinition(
        table_path="system.mlflow.run_metrics_history",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("record_id"),
            _fk("run_id", "system.mlflow.runs_latest"),
        ],
    ),
    "system.serving.served_entities": SystemTableDefinition(
        table_path="system.serving.served_entities",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("served_entity_id"),
            _pk("endpoint_id"),
        ],
    ),
    "system.serving.endpoint_usage": SystemTableDefinition(
        table_path="system.serving.endpoint_usage",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("databricks_request_id"),
            _fk("served_entity_id", "system.serving.served_entities"),
        ],
    ),
    "system.access.inbound_network": SystemTableDefinition(
        table_path="system.access.inbound_network",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("event_id"),
        ],
    ),
    "system.access.outbound_network": SystemTableDefinition(
        table_path="system.access.outbound_network",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("event_id"),
        ],
    ),
    "system.compute.node_timeline": SystemTableDefinition(
        table_path="system.compute.node_timeline",
        preview_status=TableStatus.GA,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _fk("cluster_id", "system.compute.clusters"),
            _fk("node_type", "system.compute.node_types"),
        ],
    ),
    "system.compute.node_types": SystemTableDefinition(
        table_path="system.compute.node_types",
        preview_status=TableStatus.GA,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("node_type"),
        ],
    ),
    "system.lakeflow.pipeline_update_timeline": SystemTableDefinition(
        table_path="system.lakeflow.pipeline_update_timeline",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _fk("pipeline_id", "system.lakeflow.pipelines"),
        ],
    ),
    "system.lakeflow.pipelines": SystemTableDefinition(
        table_path="system.lakeflow.pipelines",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("pipeline_id"),
        ],
    ),
    "system.storage.predictive_optimization_operations_history": SystemTableDefinition(
        table_path="system.storage.predictive_optimization_operations_history",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
    ),
    "system.billing.list_prices": SystemTableDefinition(
        table_path="system.billing.list_prices",
        preview_status=TableStatus.GA,
        regionality=TableRegionality.GLOBAL,
        columns=[
            _pk("sku_name"),
        ],
    ),
    "system.query.history": SystemTableDefinition(
        table_path="system.query.history",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("statement_id"),
            _fk("warehouse_id", "system.compute.warehouses"),
        ],
    ),
    "system.compute.warehouse_events": SystemTableDefinition(
        table_path="system.compute.warehouse_events",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _fk("warehouse_id", "system.compute.warehouses"),
        ],
    ),
    "system.compute.warehouses": SystemTableDefinition(
        table_path="system.compute.warehouses",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("warehouse_id"),
        ],
    ),
    "system.access.table_lineage": SystemTableDefinition(
        table_path="system.access.table_lineage",
        preview_status=TableStatus.GA,
        regionality=TableRegionality.REGIONAL,
        columns=[
            _pk("entity_id"),
        ],
    ),
    "system.access.workspaces_latest": SystemTableDefinition(
        table_path="system.access.workspaces_latest",
        preview_status=TableStatus.PUBLIC_PREVIEW,
        regionality=TableRegionality.GLOBAL,
        columns=[
            _pk("workspace_id"),
        ],
    ),
    "system.lakeflow.zerobus_stream": SystemTableDefinition(
        table_path="system.lakeflow.zerobus_stream",
        preview_status=TableStatus.BETA,
        regionality=TableRegionality.REGIONAL,
    ),
    "system.lakeflow.zerobus_ingest": SystemTableDefinition(
        table_path="system.lakeflow.zerobus_ingest",
        preview_status=TableStatus.BETA,
        regionality=TableRegionality.REGIONAL,
    ),
}


def get_table_definition(table_path: str) -> SystemTableDefinition | None:
    return SYSTEM_TABLES.get(table_path)


def get_regional_tables() -> list[SystemTableDefinition]:
    return [t for t in SYSTEM_TABLES.values() if t.regionality == TableRegionality.REGIONAL]


def get_global_tables() -> list[SystemTableDefinition]:
    return [t for t in SYSTEM_TABLES.values() if t.regionality == TableRegionality.GLOBAL]


def is_global(table_path: str) -> bool:
    definition = SYSTEM_TABLES.get(table_path)
    return definition is not None and definition.regionality == TableRegionality.GLOBAL
