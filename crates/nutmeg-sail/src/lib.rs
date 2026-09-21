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
//! `append` adds to it.
//!
//! This is the shape of Neo4j's Spark connector `gds` option and of Aura's
//! project-then-run session, without a database or a second instance: the
//! rows come from whatever DataFrame Sail can produce, the algorithm runs in
//! the Sail process, and the result is a DataFrame.
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::provider_as_source;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionConfig;
use datafusion_common::{Result, not_impl_err, plan_err};
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, TableSource, TableType};
use nutmeg_graph::{AlgorithmTable, ColumnMapping, GraphsTable, Part, Registry};
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

/// The write target: collects the input's rows and stages them.
#[derive(Debug)]
struct StageSink {
    graph: String,
    part: Part,
    mapping: ColumnMapping,
    replace: bool,
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

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Sail plans this on the server; the rows are consumed here, in the
        // process that owns the projection registry.
        let batches = collect(input, state.task_ctx()).await?;
        let rows = Registry::stage(
            &self.graph,
            self.part,
            &batches,
            &self.mapping,
            self.replace,
        )? as u64;
        // Report the staged row count the way DataFusion's sinks do.
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("count", arrow::datatypes::DataType::UInt64, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::UInt64Array::from(vec![rows]))],
        )?;
        let table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])?;
        table.scan(state, None, &[], None).await
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
