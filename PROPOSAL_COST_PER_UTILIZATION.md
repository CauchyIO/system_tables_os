# Proposal: Cost per Utilization Dashboard

## Overview

A dashboard page that correlates **resource cost** with **actual utilization**, enabling ROI analysis for jobs, tables, Vector Search endpoints, and DLT pipelines. The core metric is **Cost per Utilization** - answering "how much am I paying vs. how much value am I getting?"

## Problem Statement

Current dashboards show:
- Total cost of resources (jobs, clusters, warehouses)
- Execution counts and success rates

Missing insight:
- **Cost attribution to downstream artifacts** (e.g., a job costs $X and produces Y tables)
- **Utilization of produced artifacts** (are those tables actually being queried?)
- **Cost efficiency metrics** (cost per query, cost per downstream consumer)

## Proposed Solution

### Core Concept: Cost-to-Value Ratio

```
Cost per Utilization = Resource Cost / Utilization Count
```

Where:
- **Resource Cost** = DBU cost from `billing.usage` joined with pricing
- **Utilization Count** = downstream queries, consumers, or access events

---

## Data Sources

| Source Table | Purpose |
|-------------|---------|
| `system_union.billing.usage` | Cost attribution (DBU consumption) |
| `system_union.billing.list_prices` | Price per SKU |
| `system_union.access.table_lineage` | Producer → Consumer relationships |
| `system_union.lakeflow.jobs` | Job definitions |
| `system_union.lakeflow.job_run_timeline` | Job execution history |
| `system_union.lakeflow.pipelines` | DLT pipeline definitions |
| `system_union.serving.served_entities` | Vector Search endpoints |
| `system_union.query.history` | Query execution (endpoint queries, AI functions) |
| `system_union.compute.node_timeline` | Resource utilization (CPU/memory) |

---

## Dashboard Components

### Page 1: Job Cost Attribution

**Use Case**: "My job in cauchy-dev creates 30 downstream tables. What does it cost and are those tables being used?"

#### Metrics:
1. **Job Cost Breakdown**
   - Total cost per job (30-day rolling)
   - Cost per successful run vs. failed run
   - Cost trend over time

2. **Downstream Table Production**
   - Tables produced by job (via `table_lineage.source_table_full_name`)
   - Number of downstream tables per job
   - Table freshness (last update timestamp)

3. **Downstream Utilization**
   - Query count per produced table (consumers from lineage)
   - Number of distinct consumers per table
   - Consumer job/notebook breakdown

4. **Cost per Utilization Ratio**
   ```sql
   job_cost / SUM(downstream_query_count)
   ```

#### Visualizations:
- **Sankey diagram**: Job → Tables → Consumers
- **Heat map**: Cost vs. Utilization (identify expensive/unused tables)
- **Table**: Job detail with cost, tables produced, consumer count

---

### Page 2: Table-Level Cost Attribution

**Use Case**: "How much does it cost to maintain this table and who is using it?"

#### Metrics:
1. **Table Maintenance Cost**
   - Cost of jobs that write to table (producers)
   - Cost of compute for table operations

2. **Table Utilization**
   - Read query count (from lineage consumers)
   - Distinct consumers (jobs, notebooks, dashboards)
   - Query patterns (time of day, frequency)

3. **Cost per Read**
   ```sql
   table_maintenance_cost / read_count
   ```

4. **Unused Tables**
   - Tables with zero consumers in 30 days
   - Tables with high cost but low utilization

#### Visualizations:
- **Scatter plot**: Cost vs. Query Count (quadrant analysis)
- **List**: Top 10 most expensive per-read tables
- **List**: Unused tables with maintenance cost

---

### Page 3: Vector Search Index Cost Attribution

**Use Case**: "My Vector Search endpoint serves an index. How much does it cost vs. how often is it queried?"

#### Metrics:
1. **Endpoint Cost**
   - Serving infrastructure cost (from `billing.usage` where `usage_metadata.serving_endpoint_id`)
   - Cost by endpoint over time

2. **Query Volume**
   - Queries per endpoint (from `query.history` or serving logs)
   - Query latency distribution
   - Peak query times

3. **Cost per Query**
   ```sql
   endpoint_cost / query_count
   ```

4. **Index Efficiency**
   - Index size vs. query volume
   - Embeddings served per dollar

#### Visualizations:
- **Line chart**: Endpoint cost vs. query volume (dual axis)
- **Table**: Endpoint breakdown with cost/query ratio
- **Gauge**: Cost efficiency score

---

### Page 4: DLT Pipeline Cost Attribution

**Use Case**: "My DLT pipeline refreshes multiple tables. What's the cost breakdown per output table?"

#### Metrics:
1. **Pipeline Cost**
   - Total pipeline cost (from `billing.usage` where DLT SKU)
   - Cost per pipeline run
   - Cost by pipeline over time

2. **Output Tables**
   - Tables produced by pipeline (via lineage)
   - Table refresh frequency
   - Data volume per table

3. **Downstream Utilization**
   - Consumers of pipeline output tables
   - Query frequency on output tables

4. **Pipeline ROI**
   ```sql
   pipeline_cost / SUM(output_table_consumers)
   ```

#### Visualizations:
- **Tree map**: Pipeline → Output Tables (sized by cost)
- **Table**: Pipeline detail with output tables and consumer counts
- **Trend**: Pipeline cost efficiency over time

---

### Page 5: Unified Cost Explorer (Drill-down)

**Use Case**: "Give me a single view where I can search any resource and see its cost + utilization"

#### Features:
1. **Search bar**: Filter by job name, table name, endpoint name, pipeline name
2. **Resource type selector**: Jobs | Tables | Endpoints | Pipelines
3. **Time range selector**: 7d | 30d | 90d

#### Detail View:
- Resource metadata
- Cost breakdown
- Utilization metrics
- Cost per utilization trend
- Related resources (upstream/downstream)

---

## SQL Query Patterns

### Job Cost with Downstream Tables
```sql
WITH job_costs AS (
  SELECT
    usage_metadata.job_id,
    SUM(usage_quantity * p.pricing.default) as total_cost
  FROM system_union.billing.usage u
  JOIN system_union.billing.list_prices p
    ON u.sku_name = p.sku_name
    AND u.usage_date BETWEEN p.price_start_time AND p.price_end_time
  WHERE usage_metadata.job_id IS NOT NULL
    AND usage_date >= CURRENT_DATE - INTERVAL 30 DAY
  GROUP BY 1
),
job_tables AS (
  SELECT
    source_table_full_name,
    entity_id as job_id,
    COUNT(DISTINCT target_table_full_name) as downstream_tables
  FROM system_union.access.table_lineage
  WHERE entity_type = 'JOB'
  GROUP BY 1, 2
),
table_consumers AS (
  SELECT
    source_table_full_name,
    COUNT(*) as consumer_count
  FROM system_union.access.table_lineage
  WHERE target_type IN ('NOTEBOOK', 'JOB', 'DASHBOARD')
  GROUP BY 1
)
SELECT
  j.name as job_name,
  jc.total_cost,
  jt.downstream_tables,
  COALESCE(SUM(tc.consumer_count), 0) as total_consumers,
  jc.total_cost / NULLIF(SUM(tc.consumer_count), 0) as cost_per_consumer
FROM system_union.lakeflow.jobs j
JOIN job_costs jc ON j.job_id = jc.job_id
LEFT JOIN job_tables jt ON j.job_id = jt.job_id
LEFT JOIN table_consumers tc ON jt.source_table_full_name = tc.source_table_full_name
GROUP BY 1, 2, 3
ORDER BY cost_per_consumer DESC NULLS LAST
```

### Vector Search Endpoint Utilization
```sql
WITH endpoint_costs AS (
  SELECT
    usage_metadata.serving_endpoint_id as endpoint_id,
    SUM(usage_quantity * p.pricing.default) as total_cost
  FROM system_union.billing.usage u
  JOIN system_union.billing.list_prices p
    ON u.sku_name = p.sku_name
  WHERE sku_name LIKE '%VECTOR_SEARCH%'
    AND usage_date >= CURRENT_DATE - INTERVAL 30 DAY
  GROUP BY 1
),
endpoint_queries AS (
  SELECT
    -- endpoint identifier from query metadata
    COUNT(*) as query_count
  FROM system_union.query.history
  WHERE statement_type = 'VECTOR_SEARCH'
    AND start_time >= CURRENT_DATE - INTERVAL 30 DAY
  GROUP BY 1
)
SELECT
  se.name as endpoint_name,
  ec.total_cost,
  eq.query_count,
  ec.total_cost / NULLIF(eq.query_count, 0) as cost_per_query
FROM system_union.serving.served_entities se
JOIN endpoint_costs ec ON se.endpoint_id = ec.endpoint_id
LEFT JOIN endpoint_queries eq ON se.endpoint_id = eq.endpoint_id
```

---

## Implementation Plan

### Phase 1: Data Validation
1. Verify table_lineage captures job → table relationships
2. Confirm query.history contains Vector Search queries
3. Validate billing.usage has required metadata fields

### Phase 2: Query Development
1. Build CTEs for cost attribution
2. Build CTEs for utilization metrics
3. Create joined queries with cost-per-utilization calculations

### Phase 3: Dashboard Build
1. Create dashboard JSON with 5 pages
2. Add parameterized filters (workspace, time range, resource type)
3. Build visualizations (tables, charts, heat maps)

### Phase 4: Testing
1. Test with cauchy-dev job (30 downstream tables)
2. Validate Vector Search endpoint metrics
3. Verify DLT pipeline attribution

---

## Open Questions

1. **Lineage granularity**: Does `table_lineage` capture all job → table writes, or only explicit lineage?
2. **Vector Search queries**: Are VS queries captured in `query.history` with identifiable metadata?
3. **DLT pipeline identification**: Can we reliably link DLT costs to specific pipelines via `usage_metadata`?
4. **Historical depth**: How far back does lineage data go? (impacts trend analysis)

---

## Success Metrics

1. Users can identify jobs with high cost but low downstream utilization
2. Users can find "orphaned" tables (produced but never consumed)
3. Users can compare cost efficiency across Vector Search endpoints
4. Users can drill down from any resource to its full cost/utilization picture

---

## Appendix: Available Metadata Fields

### billing.usage.usage_metadata
- `job_id` - Job identifier
- `cluster_id` - Cluster identifier
- `warehouse_id` - SQL warehouse identifier
- `serving_endpoint_id` - Model/Vector serving endpoint
- `pipeline_id` - DLT pipeline identifier
- Custom tags (key-value pairs)

### access.table_lineage
- `source_table_full_name` - Producer table (catalog.schema.table)
- `target_table_full_name` - Consumer table
- `entity_type` - JOB, NOTEBOOK, PIPELINE, DASHBOARD
- `entity_id` - ID of the producing/consuming entity
