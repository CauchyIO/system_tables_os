from pydantic import BaseModel
import yaml
from pyspark.sql import DataFrame


class ViewsConfig(BaseModel):
    workspace_sql_filter: str
    target_group: str
    target_catalog: str
    create_catalog: bool = False


class DataCopierConfig(BaseModel):
    target_catalog: str
    create_catalog: bool = False
    force_full_refresh: bool = False
    excluded_schemas: list[str] = ["information_schema", "metadata"]
    frequency_hours: int = 6


class UnionViewsConfig(BaseModel):
    source_catalogs: list[str]
    target_catalog: str
    create_catalog: bool = False
    excluded_schemas: list[str] = ["information_schema", "metadata"]


def load_config_from_yaml(yaml_path: str) -> dict:
    with open(yaml_path, "r") as f:
        return yaml.safe_load(f)


def get_timestamp_column(df: DataFrame) -> str | None:
    timestamp_candidates = [
        "event_time",
        "created_at",
        "updated_at",
        "modified_at",
        "timestamp",
        "event_date",
        "created_time",
        "update_time",
        "usage_date",
        "change_time",
        "start_time",
    ]

    columns_lower = {c.lower(): c for c in df.columns}

    for candidate in timestamp_candidates:
        if candidate in columns_lower:
            return columns_lower[candidate]

    return None
