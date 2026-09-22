//! The `nutmeg` data source for Sail, and a session mutator that installs it.
//!
//! Read: `spark.read.format("nutmeg").option("graph", "g").option("algorithm",
//! "pagerank")` runs one Grust algorithm on a staged graph and returns its
//! rows. Every other option is Grust's configuration for that algorithm
//! (`orientation`, `weightProperty`, `damping`, `source`, ...), validated by
//! Grust. Without `algorithm` the read lists the staged graphs. The one
//! option that is Nutmeg's, `columnNames` (`grust`, the default, or `gds`),
//! chooses whether result columns keep Grust's names or take GDS's where they
//! differ (`nutmeg_graph::GDS_COLUMN_ALIASES`).
//!
//! Write: `df.write.format("nutmeg").option("graph", "g").option("part",
//! "edges")` stages the DataFrame's rows as the graph's edges (`part=nodes`
//! for its nodes). grust-sail's `grust_nodes`/`grust_edges` tables and the
//! grust-arrow layout are recognized as they are; other tables name their
//! columns with `sourceColumn`, `targetColumn`, `typeColumn`, `edgeIdColumn`,
//! `idColumn`, `labelColumn`. `mode("overwrite")` replaces that part;
//! `append` adds to it. `order` is `canonical` (the default: the whole part,
//! appends included, is kept sorted, so results do not depend on the order
//! Sail's scan delivered the rows in) or `asStaged` (rows kept in arrival
//! order, with no sort); see `nutmeg_graph::StageOrder`.
//!
//! This is the shape of Neo4j's Spark connector `gds` option and of Aura's
//! project-then-run session, without a database or a second instance: the
//! rows come from whatever DataFrame Sail can produce, the algorithm runs in
//! the Sail process, and the result is a DataFrame.
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::provider_as_source;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
};
use datafusion::prelude::SessionConfig;
use datafusion_common::{Result, not_impl_err, plan_err};
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, TableSource, TableType};
use futures::TryStreamExt;
use nutmeg_graph::{AlgorithmTable, ColumnMapping, GraphsTable, Part, Registry, StageOrder};
use sail_common_datafusion::datasource::{
    DataSource, DataSourceRegistry, OptionLayer, SinkInfo, SinkMode, SourceInfo,
};
use sail_session::session_factory::{ServerSessionInfo, ServerSessionMutator};

fn flatten_options(layers: &[OptionLayer]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for layer in layers {
        let items = match layer {
            OptionLayer::TablePropertyList { items } | OptionLayer::OptionList { items } => items,
            _ => continue,
        };
        for (key, value) in items {
            let key = key.to_ascii_lowercase();
            out.retain(|(k, _)| *k != key);
            out.push((key, value.clone()));
        }
    }
    out
}

fn take(options: &mut Vec<(String, String)>, key: &str) -> Option<String> {
    let index = options.iter().position(|(k, _)| k == key)?;
    Some(options.remove(index).1)
}

/// The `nutmeg` data source.
#[derive(Debug, Default)]
pub struct NutmegDataSource;

#[async_trait]
impl DataSource for NutmegDataSource {
    fn name(&self) -> &str {
        "nutmeg"
    }

    async fn create_source(
        &self,
        _ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        if !info.paths.is_empty() {
            return plan_err!("nutmeg: reads take options, not paths");
        }
        let mut options = flatten_options(&info.options);
        take(&mut options, "path");
        let graph = take(&mut options, "graph");
        let Some(algorithm) = take(&mut options, "algorithm") else {
            if let Some((key, _)) = options.first() {
                return plan_err!("nutmeg: option `{key}` needs `algorithm`");
            }
            return Ok(provider_as_source(Arc::new(GraphsTable)));
        };
        let Some(graph) = graph else {
            return plan_err!("nutmeg: option `graph` names the staged graph to run on");
        };
        let Some(name) = nutmeg_graph::resolve_algorithm(&algorithm) else {
            return plan_err!(
                "nutmeg: unknown algorithm `{algorithm}`; Grust registers {:?}",
                nutmeg_graph::algorithm_names()
            );
        };
        let names = match take(
            &mut options,
            &nutmeg_graph::COLUMN_NAMES_OPTION.to_ascii_lowercase(),
        ) {
            Some(text) => nutmeg_graph::ColumnNames::parse(&text)?,
            None => nutmeg_graph::ColumnNames::Grust,
        };
        let configuration = nutmeg_graph::options_from_strings(name, options)?;
        let table = AlgorithmTable::named(name, graph, &configuration, names)?;
        Ok(provider_as_source(Arc::new(table)))
    }

    async fn create_writer(&self, _ctx: &dyn Session, info: SinkInfo) -> Result<LogicalPlan> {
        let SinkInfo {
            input,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            options,
            lakehouse_table: _,
        } = info;
        if !partition_by.is_empty() || bucket_by.is_some() || !sort_order.is_empty() {
            return not_impl_err!("nutmeg: writes take no partitioning, bucketing or sort order");
        }
        let mut options = flatten_options(&options);
        let Some(graph) = take(&mut options, "graph") else {
            return plan_err!("nutmeg: option `graph` names the graph to stage into");
        };
        let part = take(&mut options, "part").unwrap_or_else(|| "edges".to_string());
        let part = match part.to_ascii_lowercase().as_str() {
            "edges" => Part::Edges,
            "nodes" => Part::Nodes,
            other => return plan_err!("nutmeg: `part` must be `nodes` or `edges`, got `{other}`"),
        };
        take(&mut options, "path");
        let order = match take(
            &mut options,
            &nutmeg_graph::ORDER_OPTION.to_ascii_lowercase(),
        ) {
            Some(text) => StageOrder::parse(&text)?,
            None => StageOrder::Canonical,
        };
        let mut mapping = ColumnMapping::default();
        for (key, value) in options {
            if !mapping.set(&key, value) {
                return plan_err!("nutmeg: unknown write option `{key}`");
            }
        }
        let replace = match mode {
            SinkMode::Overwrite | SinkMode::OverwriteIf { .. } | SinkMode::OverwritePartitions => {
                true
            }
            SinkMode::Append => false,
            SinkMode::ErrorIfExists | SinkMode::IgnoreIfExists => {
                return not_impl_err!("nutmeg: writes are `append` or `overwrite`");
            }
        };
        let sink = Arc::new(StageSink {
            graph,
            part,
            mapping,
            replace,
            order,
            schema: Arc::new(input.schema().as_arrow().clone()),
        });
        let plan = LogicalPlanBuilder::insert_into(
            input,
            format!("nutmeg.{}", sink.graph),
            provider_as_source(sink),
            InsertOp::Append,
        )?
        .build()?;
        Ok(plan)
    }
}

/// The write target. Planning it yields a [`StageWriter`] that stages the
/// input's rows when the plan runs.
#[derive(Debug)]
struct StageSink {
    graph: String,
    part: Part,
    mapping: ColumnMapping,
    replace: bool,
    order: StageOrder,
    schema: arrow::datatypes::SchemaRef,
}

#[async_trait]
impl TableProvider for StageSink {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan_err!("nutmeg: the write target is not readable; read with an `algorithm` option")
    }

    /// Called by DataFusion's physical planner while it builds the plan, before
    /// its physical optimizer has run over the whole tree. `input` is therefore
    /// not yet runnable: a join in it still has `PartitionMode::Auto`, which
    /// only the `JoinSelection` rule resolves. So nothing is executed here; the
    /// returned `DataSinkExec` stages the rows when the optimized plan runs.
    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let sink = StageWriter {
            graph: self.graph.clone(),
            part: self.part,
            mapping: self.mapping.clone(),
            replace: self.replace,
            order: self.order,
            schema: self.schema.clone(),
        };
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
    }
}

/// The execution half of [`StageSink`]: receives the input's rows when the
/// plan runs, in the process that owns the projection registry, and stages
/// them. `DataSinkExec` asks for a single input partition and reports the
/// returned row count as its `count` column, as DataFusion's own sinks do.
#[derive(Debug)]
struct StageWriter {
    graph: String,
    part: Part,
    mapping: ColumnMapping,
    replace: bool,
    order: StageOrder,
    schema: arrow::datatypes::SchemaRef,
}

impl DisplayAs for StageWriter {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "NutmegStage: graph={}, part={:?}", self.graph, self.part)
    }
}

#[async_trait]
impl DataSink for StageWriter {
    fn schema(&self) -> &arrow::datatypes::SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let batches: Vec<_> = data.try_collect().await?;
        let rows = Registry::stage(
            &self.graph,
            self.part,
            &batches,
            &self.mapping,
            self.replace,
            self.order,
        )?;
        Ok(rows as u64)
    }
}

/// Installs Nutmeg in every Sail session: the data source into the session's
/// registry, the table functions into the session state. Wraps the mutator
/// Sail would otherwise use so nothing of Sail's own setup changes.
pub struct NutmegSessionMutator {
    inner: Box<dyn ServerSessionMutator>,
}

impl NutmegSessionMutator {
    pub fn wrap(inner: Box<dyn ServerSessionMutator>) -> Box<dyn ServerSessionMutator> {
        Box::new(Self { inner })
    }
}

impl ServerSessionMutator for NutmegSessionMutator {
    fn mutate_config(
        &self,
        config: SessionConfig,
        info: &ServerSessionInfo,
    ) -> Result<SessionConfig> {
        let config = self.inner.mutate_config(config, info)?;
        let registry = config
            .get_extension::<DataSourceRegistry>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "nutmeg: no data source registry".into(),
                )
            })?;
        registry.register_data_source(Arc::new(NutmegDataSource))?;
        Ok(config)
    }

    fn mutate_state(
        &self,
        builder: SessionStateBuilder,
        info: &ServerSessionInfo,
    ) -> Result<SessionStateBuilder> {
        let builder = self.inner.mutate_state(builder, info)?;
        let functions = nutmeg_graph::table_functions().into_iter().collect();
        Ok(builder.with_table_functions(functions))
    }

    fn mutate_runtime_env(
        &self,
        builder: datafusion::execution::runtime_env::RuntimeEnvBuilder,
        info: &ServerSessionInfo,
    ) -> Result<datafusion::execution::runtime_env::RuntimeEnvBuilder> {
        self.inner.mutate_runtime_env(builder, info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, StringArray, UInt64Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::{JoinType, col};

    fn table(name: &str, columns: [(&str, ArrayRef); 2]) -> Result<LogicalPlan> {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(n, a)| Field::new(*n, a.data_type().clone(), false))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(schema.clone(), columns.map(|(_, a)| a).to_vec())?;
        let provider = MemTable::try_new(schema, vec![vec![batch]])?;
        LogicalPlanBuilder::scan(name, provider_as_source(Arc::new(provider)), None)?.build()
    }

    type ArrayRef = Arc<dyn Array>;

    /// A graph built the ordinary way: trips joined to stations, then staged.
    /// With more than one target partition DataFusion plans the join as a
    /// `HashJoinExec` in `PartitionMode::Auto`, which the `JoinSelection`
    /// physical optimizer rule resolves. The sink used to execute its input
    /// inside `insert_into`, during planning and before that rule ran, and
    /// failed with "Invalid HashJoinExec, unsupported PartitionMode Auto in
    /// execute()" — the Citi Bike example's failure, without Sail.
    #[tokio::test]
    async fn a_joined_dataframe_is_staged() -> Result<()> {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        let trips = table(
            "trips",
            [
                ("start", Arc::new(Int32Array::from(vec![1, 2, 2]))),
                ("end", Arc::new(Int32Array::from(vec![2, 3, 1]))),
            ],
        )?;
        let starts = table(
            "starts",
            [
                ("id", Arc::new(Int32Array::from(vec![1, 2, 3]))),
                ("source", Arc::new(StringArray::from(vec!["a", "b", "c"]))),
            ],
        )?;
        let ends = table(
            "ends",
            [
                ("id", Arc::new(Int32Array::from(vec![1, 2, 3]))),
                ("target", Arc::new(StringArray::from(vec!["a", "b", "c"]))),
            ],
        )?;
        let input = LogicalPlanBuilder::from(trips)
            .join(
                starts,
                JoinType::Inner,
                (vec!["trips.start"], vec!["starts.id"]),
                None,
            )?
            .join(
                ends,
                JoinType::Inner,
                (vec!["trips.end"], vec!["ends.id"]),
                None,
            )?
            .project(vec![col("source"), col("target")])?
            .build()?;
        let info = SinkInfo {
            input,
            mode: SinkMode::Overwrite,
            partition_by: vec![],
            bucket_by: None,
            sort_order: vec![],
            options: vec![OptionLayer::OptionList {
                items: vec![
                    ("graph".into(), "joined".into()),
                    ("part".into(), "edges".into()),
                ],
            }],
            lakehouse_table: None,
        };
        let plan = NutmegDataSource.create_writer(&ctx.state(), info).await?;
        let physical = ctx.state().create_physical_plan(&plan).await?;
        let shown = datafusion::physical_plan::displayable(physical.as_ref())
            .indent(false)
            .to_string();
        assert!(shown.contains("HashJoinExec"), "{shown}");
        let batches = datafusion::physical_plan::collect(physical, ctx.task_ctx()).await?;
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 3);
        let staged = Registry::list()?
            .into_iter()
            .find(|g| g.name == "joined")
            .expect("staged");
        assert_eq!(staged.staged_edges, 3);
        assert!(Registry::drop("joined")?);
        Ok(())
    }
}
