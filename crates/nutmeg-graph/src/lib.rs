//! Grust's graph algorithms as DataFusion table functions over named graphs.
//!
//! Nutmeg adds no algorithm surface of its own. The catalog — which
//! algorithms exist, their arguments, option names, defaults and declared
//! outputs — is Grust's procedure registry (`grust-algorithm-procedures`),
//! the same one behind `CALL grust.algorithms.pagerank(...)`. A call here
//! takes the same configuration map: `orientation`, `nodeLabels`,
//! `relationshipTypes`, `weightProperty`, `defaultWeight`, plus the
//! algorithm's own options; unknown keys are rejected by Grust's validator.
//! An algorithm registered in Grust appears here by name; serving it needs
//! one dispatch arm, and a test fails until that arm exists.
//!
//! A graph is staged once under a name as node and edge record batches in
//! the grust-arrow layout (`node_id`, `label` / `source`, `target`, `label`,
//! `edge_id`, `property.<key>` + `present.<key>`). Rows in other layouts —
//! grust-sail's `grust_nodes`/`grust_edges` tables, or arbitrary tables —
//! are renamed into it when staged ([`ColumnMapping`]). Projections are built
//! by Grust's `GraphProjection::from_arrow_batches` and cached per
//! (graph revision, projection options), like Grust's own preparation cache.
//! Kernels run in process with Grust's work charging and memory admission,
//! and results leave through Grust's Arrow result cursors.
//!
//! This crate knows nothing about Sail or Spark: it registers into any
//! DataFusion 55 `SessionContext`. `nutmeg-sail` adapts it to a Sail session.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float64Array, Int64Array, StringArray,
    new_null_array,
};
use arrow::compute::{cast, is_not_null};
use arrow::datatypes::{DataType, Field, Float32Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunction, TableFunctionImpl, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{Expr, TableType};
use grust_algorithms::{
    ArrowResultCursor, CsrEstimate, GraphProjection, NodeProperties, ProjectionOptions,
    PropertyKind, WeightSelection,
};
use grust_core::Value;
use grust_procedures::{
    ExecutionContext, ExecutionLimits, ProcedureDefinition, ProcedureRegistry, RegistryBuilder,
    SnapshotIdentity, ValidatedArguments, ValueType,
};
use once_cell::sync::Lazy;

const PREFIX: &str = "grust.algorithms.";

fn err(e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(format!("nutmeg: {e}"))
}

static PROCEDURES: Lazy<ProcedureRegistry> = Lazy::new(|| {
    let mut builder = RegistryBuilder::default();
    grust_algorithm_procedures::register_algorithms(&mut builder)
        .expect("grust algorithm procedures register");
    builder.build()
});

/// Grust's definitions of every algorithm, straight from its registry.
pub fn definitions() -> Vec<&'static ProcedureDefinition> {
    let mut all: Vec<_> = PROCEDURES
        .definitions()
        .filter(|d| d.name.starts_with(PREFIX))
        .collect();
    all.sort_by(|a, b| a.name.cmp(&b.name));
    all
}

// Grust's registry keeps names case-folded. The spellings Grust registers
// them under, used for display and to derive SQL names, are its projection
// kernels' own names plus the two inspection procedures Nutmeg serves by hand
// in `run`; a name found in neither is shown as the registry has it.
const INSPECTIONS: [&str; 2] = ["estimateCsr", "projectionStats"];

fn spelled(registered: &'static str) -> &'static str {
    grust_algorithm_procedures::projection_kernel_names()
        .into_iter()
        .chain(INSPECTIONS)
        .find(|s| s.eq_ignore_ascii_case(registered))
        .unwrap_or(registered)
}

/// The names a result's columns are reported under.
///
/// `Grust` (the default) is the registry's declared outputs, the names
/// `CALL grust.algorithms.<name>(...) YIELD ...` uses. `Gds` renames the
/// columns listed in [`GDS_COLUMN_ALIASES`] to the names Neo4j Graph Data
/// Science gives the same quantity, for code moving over from GDS; every
/// other column keeps its Grust name. The choice is per read, so no Grust
/// name is ever unreachable: a read without it gets Grust's.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ColumnNames {
    #[default]
    Grust,
    Gds,
}

/// The read option, SQL configuration key and client keyword that choose
/// [`ColumnNames`]. It is Nutmeg's, not an algorithm option: it is taken
/// out before Grust validates the rest, and a test fails if any registered
/// kernel ever declares an option or argument of the same name.
pub const COLUMN_NAMES_OPTION: &str = "columnNames";

impl ColumnNames {
    pub fn parse(text: &str) -> Result<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "grust" => Ok(Self::Grust),
            "gds" => Ok(Self::Gds),
            other => {
                plan_err!("nutmeg: `{COLUMN_NAMES_OPTION}` is `grust` or `gds`, got `{other}`")
            }
        }
    }

    /// Remove the choice from a JSON configuration, leaving Grust's options.
    pub fn take(options: &mut serde_json::Map<String, serde_json::Value>) -> Result<Self> {
        let Some(key) = options
            .keys()
            .find(|k| k.eq_ignore_ascii_case(COLUMN_NAMES_OPTION))
            .cloned()
        else {
            return Ok(Self::Grust);
        };
        match options.remove(&key) {
            Some(serde_json::Value::String(text)) => Self::parse(&text),
            other => plan_err!("nutmeg: `{COLUMN_NAMES_OPTION}` must be a string, got {other:?}"),
        }
    }

    fn rename(self, name: &str) -> &str {
        match self {
            Self::Grust => name,
            Self::Gds => GDS_COLUMN_ALIASES
                .iter()
                .find(|(grust, _)| *grust == name)
                .map_or(name, |(_, gds)| gds),
        }
    }

    /// `schema` with each field renamed. Every column is looked up by its
    /// Grust name, so a rename is simultaneous: `triangles` → `triangleCount`
    /// and `triangleCount` → `globalTriangleCount` in one result is a
    /// relabelling, not a chain. A rename that would give two columns one
    /// name is refused rather than shadowing one of them.
    pub fn rename_schema(self, schema: &SchemaRef) -> Result<SchemaRef> {
        if self == Self::Grust {
            return Ok(schema.clone());
        }
        let fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_name(self.rename(f.name())))
            .collect();
        let mut seen = HashSet::new();
        if let Some(twice) = fields.iter().find(|f| !seen.insert(f.name().as_str())) {
            return exec_err!(
                "nutmeg: `{COLUMN_NAMES_OPTION}: gds` would name two columns `{}`",
                twice.name()
            );
        }
        Ok(Arc::new(Schema::new_with_metadata(
            fields,
            schema.metadata().clone(),
        )))
    }

    fn rename_batch(self, batch: RecordBatch) -> Result<RecordBatch> {
        if self == Self::Grust {
            return Ok(batch);
        }
        let schema = self.rename_schema(&batch.schema())?;
        Ok(RecordBatch::try_new(schema, batch.columns().to_vec())?)
    }
}

/// Grust's column name → the name Neo4j Graph Data Science uses for the same
/// quantity. Data, keyed by column and not by algorithm: an entry applies
/// wherever Grust yields that column, so it is listed only where the column
/// means the same thing in every kernel that yields it, and GDS's name is
/// cited from GDS's documentation (the procedure pages under
/// <https://neo4j.com/docs/graph-data-science/current/algorithms/>):
///
/// - `pathIndex` → `index`: `gds.shortestPath.yens.stream` yields `index`.
///   Grust cannot use `index`, a reserved word in its Cypher dialect.
/// - `iterations` → `ranIterations`, `converged` → `didConverge`: the stats
///   mode of `gds.pageRank`, `gds.articleRank`, `gds.eigenvector`,
///   `gds.hits`, `gds.labelPropagation`, `gds.k1coloring`, and
///   (`didConverge`) `gds.leiden`.
/// - `levels` → `ranLevels`: stats mode of `gds.louvain` and `gds.leiden`.
/// - `triangles` → `triangleCount`, `triangleCount` → `globalTriangleCount`:
///   `gds.triangleCount` streams a node's triangles as `triangleCount` and
///   reports the graph's total as `globalTriangleCount`; Grust's per-node
///   `triangles` and total `triangleCount` are those two.
/// - `coefficient` → `localClusteringCoefficient`, `averageCoefficient` →
///   `averageClusteringCoefficient`: `gds.localClusteringCoefficient` stream
///   and stats modes.
///
/// GDS reports the iteration and level counts in its stats mode, and Grust
/// repeats them on every row; the rename gives them GDS's names, not GDS's
/// row shape.
pub const GDS_COLUMN_ALIASES: &[(&str, &str)] = &[
    ("pathIndex", "index"),
    ("iterations", "ranIterations"),
    ("converged", "didConverge"),
    ("levels", "ranLevels"),
    ("triangles", "triangleCount"),
    ("triangleCount", "globalTriangleCount"),
    ("coefficient", "localClusteringCoefficient"),
    ("averageCoefficient", "averageClusteringCoefficient"),
];

fn short(definition: &'static ProcedureDefinition) -> &'static str {
    spelled(&definition.name[PREFIX.len()..])
}

/// Grust's short algorithm names (`pagerank`, `shortestPaths`, …).
pub fn algorithm_names() -> Vec<&'static str> {
    definitions().into_iter().map(short).collect()
}

/// `shortestPaths` → `shortest_paths`, for SQL function names.
pub fn snake(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        if c.is_ascii_uppercase() {
            out.push('_');
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// Resolve a user-supplied name in either spelling to Grust's.
pub fn resolve_algorithm(name: &str) -> Option<&'static str> {
    let wanted = name.trim().to_ascii_lowercase().replace(['_', '-'], "");
    algorithm_names()
        .into_iter()
        .find(|n| n.to_ascii_lowercase() == wanted)
}

/// Memory admitted to one projection and the kernels run on it.
static MEMORY_BYTES: AtomicUsize = AtomicUsize::new(8 << 30);

/// Set the admission limit for projections built from now on. The
/// `NUTMEG_MEMORY_BYTES` environment variable, when set, wins.
pub fn set_memory_bytes(bytes: usize) {
    MEMORY_BYTES.store(bytes, Ordering::Relaxed);
}

fn memory_bytes() -> usize {
    static ENV: Lazy<Option<usize>> =
        Lazy::new(|| std::env::var("NUTMEG_MEMORY_BYTES").ok()?.parse().ok());
    ENV.unwrap_or_else(|| MEMORY_BYTES.load(Ordering::Relaxed))
}

/// Which input columns hold the structural fields. Anything unset is found
/// by name: the grust-arrow names first, then grust-sail's table columns.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ColumnMapping {
    pub id: Option<String>,
    pub label: Option<String>,
    pub source: Option<String>,
    pub target: Option<String>,
    pub edge_type: Option<String>,
    pub edge_id: Option<String>,
}

impl ColumnMapping {
    /// Set one field from a lowercase option key; false when not a mapping key.
    pub fn set(&mut self, key: &str, value: String) -> bool {
        let slot = match key {
            "idcolumn" => &mut self.id,
            "labelcolumn" => &mut self.label,
            "sourcecolumn" => &mut self.source,
            "targetcolumn" => &mut self.target,
            "typecolumn" => &mut self.edge_type,
            "edgeidcolumn" => &mut self.edge_id,
            _ => return false,
        };
        *slot = Some(value);
        true
    }
}

fn pick<'a>(
    batch: &'a RecordBatch,
    explicit: &Option<String>,
    candidates: &[&str],
    what: &str,
    required: bool,
) -> Result<Option<(&'a ArrayRef, String)>> {
    if let Some(name) = explicit {
        return match batch.column_by_name(name) {
            Some(column) => Ok(Some((column, name.clone()))),
            None => plan_err!("nutmeg: {what} column `{name}` not found"),
        };
    }
    for name in candidates {
        if let Some(column) = batch.column_by_name(name) {
            return Ok(Some((column, name.to_string())));
        }
    }
    if required {
        let have: Vec<_> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        return plan_err!(
            "nutmeg: no {what} column; looked for {candidates:?} in {have:?}; name it with an option"
        );
    }
    Ok(None)
}

fn utf8(column: &ArrayRef) -> Result<ArrayRef> {
    Ok(cast(column, &DataType::Utf8)?)
}

fn constant(value: &str, rows: usize) -> ArrayRef {
    Arc::new(StringArray::from(vec![value; rows]))
}

fn node_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("node_id", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, true),
    ]))
}

/// Rename node rows into the grust-arrow layout: `node_id`, `label`, and for
/// every other column `c` a kernel could read as a node property, the pair
/// `property.c` and `present.c`. Integers become Int64 and other numbers
/// Float64, as on edges; strings stay Utf8, for categories; fixed-size lists
/// of floats stay as they are, for vectors. Columns already named
/// `property.*`/`present.*` pass through.
///
/// Until these were kept, every node column but the id and label was dropped
/// here, so no kernel that reads node properties could run on a staged graph.
pub fn normalize_nodes(batch: &RecordBatch, mapping: &ColumnMapping) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let (id, id_name) =
        pick(batch, &mapping.id, &["node_id", "id"], "node id", true)?.expect("required");
    let label = pick(batch, &mapping.label, &["label"], "node label", false)?;
    let mut fields: Vec<Field> = node_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    let mut columns: Vec<ArrayRef> = vec![
        utf8(id)?,
        match &label {
            Some((column, _)) => utf8(column)?,
            None => constant("", rows),
        },
    ];
    let mut used: HashSet<String> = [id_name].into();
    used.extend(label.map(|(_, name)| name));
    lift_properties(batch, &used, Lift::Node, &mut fields, &mut columns)?;
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// Which column kinds become properties: edges carry weights, which are
/// numbers; nodes also carry categories and vectors.
#[derive(Clone, Copy, PartialEq)]
enum Lift {
    Edge,
    Node,
}

/// Append `property.c` + `present.c` for every unused column `c` of a kind
/// `lift` admits, and pass `property.*`/`present.*` columns through.
fn lift_properties(
    batch: &RecordBatch,
    used: &HashSet<String>,
    lift: Lift,
    fields: &mut Vec<Field>,
    columns: &mut Vec<ArrayRef>,
) -> Result<()> {
    let schema = batch.schema();
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        let name = field.name();
        if used.contains(name) {
            continue;
        }
        if name.starts_with("property.") || name.starts_with("present.") {
            fields.push(field.as_ref().clone());
            columns.push(column.clone());
            continue;
        }
        let kind = field.data_type();
        let values = if kind.is_integer() {
            cast(column, &DataType::Int64)?
        } else if kind.is_numeric() {
            cast(column, &DataType::Float64)?
        } else if lift == Lift::Node && matches!(kind, DataType::Utf8 | DataType::LargeUtf8) {
            cast(column, &DataType::Utf8)?
        } else if lift == Lift::Node && matches!(kind, DataType::FixedSizeList(..)) {
            column.clone()
        } else {
            continue;
        };
        fields.push(Field::new(
            format!("property.{name}"),
            values.data_type().clone(),
            true,
        ));
        columns.push(values);
        fields.push(Field::new(
            format!("present.{name}"),
            DataType::Boolean,
            false,
        ));
        columns.push(Arc::new(is_not_null(column)?));
    }
    Ok(())
}

/// Rename edge rows into the grust-arrow layout: `source`, `target`, `label`,
/// `edge_id`, and for every other numeric column `c` the pair `property.c`
/// (Float64 or Int64) and `present.c`, so `weightProperty: "c"` selects it.
/// Columns already named `property.*`/`present.*` pass through.
pub fn normalize_edges(batch: &RecordBatch, mapping: &ColumnMapping) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let (source, source_name) = pick(
        batch,
        &mapping.source,
        &["source", "src_id", "src"],
        "edge source",
        true,
    )?
    .expect("required");
    let (target, target_name) = pick(
        batch,
        &mapping.target,
        &["target", "dst_id", "dst"],
        "edge target",
        true,
    )?
    .expect("required");
    let label = pick(
        batch,
        &mapping.edge_type,
        &["label", "edge_type", "type"],
        "edge type",
        false,
    )?;
    let edge_id = pick(
        batch,
        &mapping.edge_id,
        &["edge_id", "id"],
        "edge id",
        false,
    )?;
    let mut fields = vec![
        Field::new("source", DataType::Utf8, true),
        Field::new("target", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("edge_id", DataType::Utf8, true),
    ];
    let mut columns = vec![
        utf8(source)?,
        utf8(target)?,
        match &label {
            Some((column, _)) => utf8(column)?,
            None => constant("", rows),
        },
        match &edge_id {
            Some((column, _)) => utf8(column)?,
            None => new_null_array(&DataType::Utf8, rows),
        },
    ];
    let mut used: HashSet<String> = [source_name, target_name].into();
    used.extend(label.into_iter().chain(edge_id).map(|(_, name)| name));
    lift_properties(batch, &used, Lift::Edge, &mut fields, &mut columns)?;
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// One named graph: staged rows, and projections built from them.
#[derive(Default)]
struct Entry {
    nodes: Vec<RecordBatch>,
    edges: Vec<RecordBatch>,
    revision: u64,
    projections: HashMap<String, GraphProjection>,
}

/// What [`Registry::list`] reports for one graph.
#[derive(Clone, Debug)]
pub struct GraphInfo {
    pub name: String,
    pub staged_nodes: usize,
    pub staged_edges: usize,
    pub revision: u64,
    pub projections: usize,
}

static GRAPHS: Lazy<RwLock<HashMap<String, Arc<RwLock<Entry>>>>> = Lazy::new(Default::default);

fn poisoned() -> DataFusionError {
    DataFusionError::Execution("nutmeg: registry lock poisoned".into())
}

/// Which rows a staging call carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Part {
    Nodes,
    Edges,
}

/// The process-wide registry of named graphs.
pub struct Registry;

impl Registry {
    fn entry(name: &str, create: bool) -> Result<Option<Arc<RwLock<Entry>>>> {
        if !create {
            return Ok(GRAPHS.read().map_err(|_| poisoned())?.get(name).cloned());
        }
        let mut map = GRAPHS.write().map_err(|_| poisoned())?;
        Ok(Some(map.entry(name.to_string()).or_default().clone()))
    }

    /// Stage rows under `name`, renaming them into the grust-arrow layout.
    /// `replace` discards that part's earlier rows. Nodes are optional: a
    /// graph staged from edges alone takes its nodes from their endpoints.
    /// Returns the rows now staged for that part.
    pub fn stage(
        name: &str,
        part: Part,
        batches: &[RecordBatch],
        mapping: &ColumnMapping,
        replace: bool,
    ) -> Result<usize> {
        let normalized: Vec<RecordBatch> = batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .map(|b| match part {
                Part::Nodes => normalize_nodes(b, mapping),
                Part::Edges => normalize_edges(b, mapping),
            })
            .collect::<Result<_>>()?;
        let entry = Self::entry(name, true)?.expect("created");
        let mut e = entry.write().map_err(|_| poisoned())?;
        let rows = match part {
            Part::Nodes => &mut e.nodes,
            Part::Edges => &mut e.edges,
        };
        if replace {
            rows.clear();
        }
        rows.extend(normalized);
        let total = rows.iter().map(|b| b.num_rows()).sum();
        e.revision += 1;
        e.projections.clear();
        Ok(total)
    }

    /// Forget `name` and everything built from it.
    pub fn drop(name: &str) -> Result<bool> {
        Ok(GRAPHS
            .write()
            .map_err(|_| poisoned())?
            .remove(name)
            .is_some())
    }

    pub fn list() -> Result<Vec<GraphInfo>> {
        let map = GRAPHS.read().map_err(|_| poisoned())?;
        let mut out = Vec::new();
        for (name, entry) in map.iter() {
            let e = entry.read().map_err(|_| poisoned())?;
            out.push(GraphInfo {
                name: name.clone(),
                staged_nodes: e.nodes.iter().map(|b| b.num_rows()).sum(),
                staged_edges: e.edges.iter().map(|b| b.num_rows()).sum(),
                revision: e.revision,
                projections: e.projections.len(),
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    fn staged_counts(name: &str) -> Result<(usize, usize)> {
        let Some(entry) = Self::entry(name, false)? else {
            return exec_err!("nutmeg: no graph named `{name}`; stage its rows first");
        };
        let e = entry.read().map_err(|_| poisoned())?;
        let edges: usize = e.edges.iter().map(|b| b.num_rows()).sum();
        let nodes = if e.nodes.is_empty() {
            derive_nodes(&e.edges)?.num_rows()
        } else {
            e.nodes.iter().map(|b| b.num_rows()).sum()
        };
        Ok((nodes, edges))
    }

    /// The projection of `name` under the projection options in `args`,
    /// built on first use and kept until the graph is staged again.
    /// The node batches a projection of `name` is built from: those staged,
    /// or, when only edges were staged, the endpoints derived from them. A
    /// kernel's node properties are read from these, row-aligned with the
    /// projection by node id.
    fn node_batches(name: &str) -> Result<Vec<RecordBatch>> {
        let Some(entry) = Self::entry(name, false)? else {
            return exec_err!("nutmeg: no graph named `{name}`; stage its rows first");
        };
        let e = entry.read().map_err(|_| poisoned())?;
        if e.nodes.is_empty() {
            Ok(vec![derive_nodes(&e.edges)?])
        } else {
            Ok(e.nodes.clone())
        }
    }

    pub fn projection(name: &str, args: &ValidatedArguments) -> Result<GraphProjection> {
        let Some(entry) = Self::entry(name, false)? else {
            return exec_err!("nutmeg: no graph named `{name}`; stage its rows first");
        };
        let key = projection_key(args);
        if let Some(found) = entry.read().map_err(|_| poisoned())?.projections.get(&key) {
            return Ok(found.clone());
        }
        let mut e = entry.write().map_err(|_| poisoned())?;
        if let Some(found) = e.projections.get(&key) {
            return Ok(found.clone());
        }
        let derived;
        let nodes: &[RecordBatch] = if e.nodes.is_empty() {
            derived = [derive_nodes(&e.edges)?];
            &derived
        } else {
            &e.nodes
        };
        let context = ExecutionContext::new(ExecutionLimits {
            memory_bytes: memory_bytes(),
            work_units: usize::MAX,
            batch_rows: 8192,
            deadline: None,
        })
        .map_err(err)?;
        let identity = SnapshotIdentity::new(
            name.to_string(),
            format!("r{}", e.revision),
            "nutmeg".into(),
        )
        .map_err(err)?;
        let graph = GraphProjection::from_arrow_batches(
            identity,
            nodes,
            &e.edges,
            projection_options(args)?,
            &context,
        )
        .map_err(err)?;
        e.projections.insert(key, graph.clone());
        Ok(graph)
    }
}

/// Distinct edge endpoints in first-appearance order, as a node batch.
fn derive_nodes(edges: &[RecordBatch]) -> Result<RecordBatch> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut ids: Vec<&str> = Vec::new();
    for batch in edges {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| err(format!("staged edges lack `{name}`")))
        };
        let (sources, targets) = (column("source")?, column("target")?);
        for row in 0..batch.num_rows() {
            for endpoint in [sources, targets] {
                if endpoint.is_null(row) {
                    return exec_err!("nutmeg: null edge endpoint at row {row}");
                }
                let id = endpoint.value(row);
                if seen.insert(id) {
                    ids.push(id);
                }
            }
        }
    }
    let rows = ids.len();
    Ok(RecordBatch::try_new(
        node_schema(),
        vec![Arc::new(StringArray::from(ids)), constant("", rows)],
    )?)
}

const PROJECTION_KEYS: [&str; 5] = [
    "orientation",
    "nodeLabels",
    "relationshipTypes",
    "weightProperty",
    "defaultWeight",
];

fn projection_key(args: &ValidatedArguments) -> String {
    let picked: BTreeMap<&str, String> = PROJECTION_KEYS
        .iter()
        .map(|k| (*k, format!("{:?}", args.options().get(*k))))
        .collect();
    format!("{picked:?}")
}

// Grust's own reading of the projection options, so a projection built here
// means what the registered procedure's would.
fn projection_options(args: &ValidatedArguments) -> Result<ProjectionOptions<'_>> {
    grust_algorithm_procedures::projection_options(args).map_err(err)
}

fn json_text(value: serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    }
}

/// Turn a call's JSON options into Grust's validated arguments. Positional
/// arguments (`source`, `sources`) are given by name in the same object.
pub fn validate(
    algorithm: &str,
    options: &serde_json::Map<String, serde_json::Value>,
) -> Result<ValidatedArguments> {
    let resolved = PROCEDURES
        .resolve(&format!("{PREFIX}{algorithm}"))
        .map_err(err)?;
    let definition = resolved.definition();
    let mut configuration = options.clone();
    let mut args = Vec::new();
    for (index, argument) in definition.arguments.iter().enumerate() {
        if Some(index) == definition.options_argument {
            continue;
        }
        let name = &argument.field.name;
        let Some(given) = configuration.remove(name) else {
            return plan_err!("nutmeg: `{algorithm}` needs `{name}`");
        };
        args.push(match (argument.field.value_type, given) {
            (ValueType::Strings, serde_json::Value::Array(items)) => {
                Value::StringArray(items.into_iter().map(json_text).collect())
            }
            (ValueType::Strings, other) => Value::StringArray(
                json_text(other)
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect(),
            ),
            (_, other) => Value::String(json_text(other)),
        });
    }
    args.push(Value::Json(serde_json::Value::Object(configuration)));
    resolved
        .validate_arguments(args)
        .map_err(|e| DataFusionError::Plan(format!("nutmeg: {algorithm}: {e}")))
}

/// Data source options arrive as lowercase-keyed strings; restore Grust's
/// spelling of each key and give each value its JSON type, so `"0.9"`
/// validates as a number and `["a","b"]` as an array.
pub fn options_from_strings(
    algorithm: &str,
    pairs: impl IntoIterator<Item = (String, String)>,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    let resolved = PROCEDURES
        .resolve(&format!("{PREFIX}{algorithm}"))
        .map_err(err)?;
    let definition = resolved.definition();
    let mut spelled: HashMap<String, (&str, ValueType)> = HashMap::new();
    for option in &definition.options {
        spelled.insert(
            option.field.name.to_ascii_lowercase(),
            (&option.field.name, option.field.value_type),
        );
    }
    for argument in &definition.arguments {
        spelled.insert(
            argument.field.name.to_ascii_lowercase(),
            (&argument.field.name, argument.field.value_type),
        );
    }
    let mut out = serde_json::Map::new();
    for (key, value) in pairs {
        let (name, kind) = match spelled.get(&key.to_ascii_lowercase()) {
            Some((name, kind)) => (name.to_string(), Some(*kind)),
            None => (key, None),
        };
        let parsed = match kind {
            Some(ValueType::String) | None => serde_json::Value::String(value),
            Some(_) => serde_json::from_str(&value).unwrap_or(serde_json::Value::String(value)),
        };
        out.insert(name, parsed);
    }
    Ok(out)
}

fn drain(mut cursor: ArrowResultCursor) -> Result<Vec<RecordBatch>> {
    let mut out = Vec::new();
    while let Some(batch) = cursor.next_batch().map_err(err)? {
        out.push(batch.record_batch().clone());
    }
    Ok(out)
}

fn int_row(names: &[&str], values: &[usize]) -> Result<Vec<RecordBatch>> {
    if names.len() != values.len() {
        return exec_err!(
            "nutmeg: Grust declares {names:?}; this build produces {} values",
            values.len()
        );
    }
    let fields: Vec<Field> = names
        .iter()
        .map(|n| Field::new(*n, DataType::Int64, false))
        .collect();
    let columns: Vec<ArrayRef> = values
        .iter()
        .map(|v| Arc::new(Int64Array::from(vec![*v as i64])) as ArrayRef)
        .collect();
    Ok(vec![RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?])
}

fn definition_of(algorithm: &str) -> Result<&'static ProcedureDefinition> {
    definitions()
        .into_iter()
        .find(|d| short(d).eq_ignore_ascii_case(algorithm))
        .ok_or_else(|| err(format!("unknown algorithm `{algorithm}`")))
}

fn output_names(algorithm: &str) -> Result<Vec<&'static str>> {
    Ok(definition_of(algorithm)?
        .outputs
        .iter()
        .map(|f| f.name.as_str())
        .collect())
}

/// Run one Grust algorithm on the named graph. Every batch carries the
/// nullability Grust declares for each output column, whatever the rows in it
/// happen to hold (see [`conform`]).
pub fn run(
    algorithm: &str,
    graph_name: &str,
    args: &ValidatedArguments,
) -> Result<Vec<RecordBatch>> {
    let definition = definition_of(algorithm)?;
    run_kernel(algorithm, graph_name, args)?
        .into_iter()
        .map(|batch| conform(definition, batch))
        .collect()
}

/// Restate a result batch's schema with each column's declared nullability.
///
/// Grust's `run_on_projection` and `run_with_properties` now give every batch
/// the nullability each kernel's registration declares, so on a current Grust
/// this changes nothing. It stays as the boundary check: Grust releases before
/// that fix built batches with `RecordBatch::try_from_iter`, which marks a
/// column nullable exactly when that batch holds a null, and a column name or
/// count that differs from the declaration is refused here either way. The
/// declaration is the contract; the data types are kept as produced. A
/// declared non-nullable column that holds a null is refused, by Arrow's own
/// check, rather than passed on.
fn conform(definition: &ProcedureDefinition, batch: RecordBatch) -> Result<RecordBatch> {
    let observed = batch.schema();
    if observed.fields().len() != definition.outputs.len() {
        return exec_err!(
            "nutmeg: `{}` produced {} columns, Grust declares {}",
            definition.name,
            observed.fields().len(),
            definition.outputs.len()
        );
    }
    let fields: Vec<Field> = observed
        .fields()
        .iter()
        .zip(&definition.outputs)
        .map(|(field, declared)| {
            if *field.name() != declared.name {
                return exec_err!(
                    "nutmeg: `{}` produced column `{}` where Grust declares `{}`",
                    definition.name,
                    field.name(),
                    declared.name
                );
            }
            Ok(field.as_ref().clone().with_nullable(declared.nullable))
        })
        .collect::<Result<_>>()?;
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        observed.metadata().clone(),
    ));
    RecordBatch::try_new(schema, batch.columns().to_vec()).map_err(|e| {
        err(format!(
            "`{}` broke its declared output schema: {e}",
            definition.name
        ))
    })
}

fn run_kernel(
    algorithm: &str,
    graph_name: &str,
    args: &ValidatedArguments,
) -> Result<Vec<RecordBatch>> {
    if algorithm == "estimateCsr" {
        // Sizing before building: counts only, as in Grust's procedure.
        let (nodes, edges) = Registry::staged_counts(graph_name)?;
        let options = projection_options(args)?;
        let weighted = matches!(options.weight, WeightSelection::Property { .. });
        let e =
            CsrEstimate::upper_bound(nodes, edges, options.orientation, weighted).map_err(err)?;
        return int_row(
            &output_names(algorithm)?,
            &[
                nodes,
                edges,
                e.max_arcs,
                e.outgoing_bytes,
                e.reverse_bytes,
                e.positions_bytes,
            ],
        );
    }
    let graph = Registry::projection(graph_name, args)?;
    let g = &graph;
    let cursor = match algorithm {
        "projectionStats" => {
            let s = g.statistics().map_err(err)?;
            return int_row(
                &output_names(algorithm)?,
                &[s.nodes, s.edges, s.arcs, s.self_loops, s.csr_bytes],
            );
        }
        // Every projection kernel Grust registers, by name: Grust finds the
        // kernel, reads its options and returns its typed Arrow results, so a
        // new registration is served here with no code of Nutmeg's own. A
        // kernel that reads node properties names them through its options;
        // they are read from the staged node rows and handed over with the
        // projection. One that reads none takes the projection alone, so the
        // common path builds nothing extra.
        _ => {
            let wanted =
                grust_algorithm_procedures::node_property_requests(algorithm, args).map_err(err)?;
            if wanted.is_empty() {
                grust_algorithm_procedures::run_on_projection(algorithm, g, args).map_err(err)?
            } else {
                let nodes = Registry::node_batches(graph_name)?;
                let properties =
                    NodeProperties::from_arrow_batches(&nodes, g, &wanted).map_err(err)?;
                grust_algorithm_procedures::run_with_properties(algorithm, &properties, args)
                    .map_err(err)?
            }
        }
    };
    drain(cursor)
}

fn probe_args(algorithm: &str, orientation: Option<&str>) -> Result<ValidatedArguments> {
    let mut options = serde_json::Map::new();
    if let Some(orientation) = orientation {
        options.insert("orientation".into(), serde_json::json!(orientation));
    }
    let definition = definition_of(algorithm)?;
    // Successive node arguments name different probe nodes: a kernel that takes
    // a source and a target, such as max flow, refuses the same node twice.
    let mut ids = ["a", "b", "c"].into_iter().cycle();
    for (index, argument) in definition.arguments.iter().enumerate() {
        if Some(index) != definition.options_argument {
            let id = ids.next().unwrap_or("a");
            let value = match argument.field.value_type {
                ValueType::Strings => serde_json::json!([id]),
                _ => serde_json::json!(id),
            };
            options.insert(argument.field.name.clone(), value);
        }
    }
    // A kernel that reads node properties would otherwise name its defaults,
    // which the probe graph does not have; point each at the probe's column.
    // Only projection kernels declare properties: `estimateCsr` and
    // `projectionStats` are served here by hand and have none to declare.
    if grust_algorithm_procedures::projection_kernel_names().contains(&algorithm) {
        for declared in grust_algorithm_procedures::node_property_options(algorithm).map_err(err)? {
            options.insert(
                declared.option.to_string(),
                serde_json::json!(probe_key(declared.option, declared.kind)),
            );
        }
    }
    validate(algorithm, &options)
}

/// The probe column standing in for one declared property option. Keyed by
/// option and kind together, so two kernels that happen to share an option name
/// with different kinds do not collide.
fn probe_key(option: &str, kind: PropertyKind) -> String {
    let kind = match kind {
        PropertyKind::Number => "number",
        PropertyKind::Integer => "integer",
        PropertyKind::Vector => "vector",
        PropertyKind::Category => "category",
    };
    format!("probe.{option}.{kind}")
}

/// The probe's three nodes, carrying a column for every property option any
/// registered kernel declares. Values are chosen to be valid for every reader:
/// numbers lie within ±90, so they serve as latitudes and longitudes; integers
/// split the nodes into two communities; vectors are distinct and nonzero.
fn probe_nodes() -> Result<RecordBatch> {
    let ids: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    let mut fields = vec![
        Field::new("node_id", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, true),
    ];
    let mut columns: Vec<ArrayRef> = vec![ids, Arc::new(StringArray::from(vec!["", "", ""]))];
    let mut seen = HashSet::new();
    for name in grust_algorithm_procedures::projection_kernel_names() {
        for declared in grust_algorithm_procedures::node_property_options(name).map_err(err)? {
            let key = probe_key(declared.option, declared.kind);
            if !seen.insert(key.clone()) {
                continue;
            }
            let values: ArrayRef = match declared.kind {
                PropertyKind::Number => Arc::new(Float64Array::from(vec![0.0, 0.5, 1.0])),
                PropertyKind::Integer => Arc::new(Int64Array::from(vec![0, 1, 0])),
                PropertyKind::Category => Arc::new(StringArray::from(vec!["x", "y", "x"])),
                PropertyKind::Vector => {
                    Arc::new(
                        FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                            vec![
                                Some(vec![Some(1.0), Some(0.0)]),
                                Some(vec![Some(0.0), Some(1.0)]),
                                Some(vec![Some(1.0), Some(1.0)]),
                            ],
                            2,
                        ),
                    )
                }
            };
            fields.push(Field::new(
                format!("property.{key}"),
                values.data_type().clone(),
                true,
            ));
            columns.push(values);
            fields.push(Field::new(
                format!("present.{key}"),
                DataType::Boolean,
                false,
            ));
            columns.push(Arc::new(BooleanArray::from(vec![true, true, true])));
        }
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

const PROBE: &str = "nutmeg.schema-probe";

static SCHEMAS: Lazy<RwLock<HashMap<String, SchemaRef>>> = Lazy::new(Default::default);

/// Run `algorithm` once on the probe graph, which must be staged, returning
/// the arguments it ran with and its batches. A kernel defined on undirected
/// graphs refuses the default directed projection, and says so; it is probed
/// on an undirected one instead.
fn probe(algorithm: &str) -> Result<(ValidatedArguments, Vec<RecordBatch>)> {
    let args = probe_args(algorithm, None)?;
    match run(algorithm, PROBE, &args) {
        Err(error) if error.to_string().contains("undirected") => {
            let args = probe_args(algorithm, Some("undirected"))?;
            let batches = run(algorithm, PROBE, &args)?;
            Ok((args, batches))
        }
        other => Ok((args, other?)),
    }
}

/// The Arrow schema an algorithm's result has: the kernel's own column names
/// and types, observed by running it once on a three-node graph, not a
/// transcription of them. Nullability is Grust's declaration, not what the
/// probe's rows held: on three nodes most nullable columns are full. These
/// are Grust's names; [`output_schema_named`] reports them as a read with a
/// [`ColumnNames`] choice returns them.
pub fn output_schema_named(algorithm: &str, names: ColumnNames) -> Result<SchemaRef> {
    names.rename_schema(&output_schema(algorithm)?)
}

/// See [`output_schema_named`]; Grust's names.
pub fn output_schema(algorithm: &str) -> Result<SchemaRef> {
    if let Some(found) = SCHEMAS.read().map_err(|_| poisoned())?.get(algorithm) {
        return Ok(found.clone());
    }
    let mut schemas = SCHEMAS.write().map_err(|_| poisoned())?;
    if Registry::entry(PROBE, false)?.is_none() {
        let edges = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("source", DataType::Utf8, false),
                Field::new("target", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
                Arc::new(StringArray::from(vec!["b", "c", "c"])),
            ],
        )?;
        Registry::stage(
            PROBE,
            Part::Edges,
            &[edges],
            &ColumnMapping::default(),
            true,
        )?;
        Registry::stage(
            PROBE,
            Part::Nodes,
            &[probe_nodes()?],
            &ColumnMapping::default(),
            true,
        )?;
    }
    let (_, batches) = probe(algorithm)?;
    let Some(first) = batches.first() else {
        return exec_err!("nutmeg: probing `{algorithm}` produced no batch");
    };
    schemas.insert(algorithm.to_string(), first.schema());
    Ok(first.schema())
}

/// A table whose scan runs one algorithm on one named graph.
#[derive(Debug)]
pub struct AlgorithmTable {
    algorithm: &'static str,
    graph: String,
    args: Arc<ValidatedArguments>,
    names: ColumnNames,
    schema: SchemaRef,
}

impl AlgorithmTable {
    /// A table over `graph` with Grust's column names.
    pub fn new(
        algorithm: &str,
        graph: String,
        options: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Self> {
        Self::named(algorithm, graph, options, ColumnNames::Grust)
    }

    /// A table over `graph` whose columns are reported under `names`. The
    /// schema reported and the batches a scan returns are renamed by the same
    /// function, and `batches` re-checks one against the other.
    pub fn named(
        algorithm: &str,
        graph: String,
        options: &serde_json::Map<String, serde_json::Value>,
        names: ColumnNames,
    ) -> Result<Self> {
        let Some(algorithm) = resolve_algorithm(algorithm) else {
            return plan_err!(
                "nutmeg: unknown algorithm `{algorithm}`; Grust registers {:?}",
                algorithm_names()
            );
        };
        Ok(Self {
            algorithm,
            graph,
            args: Arc::new(validate(algorithm, options)?),
            names,
            schema: output_schema_named(algorithm, names)?,
        })
    }

    pub fn batches(&self) -> Result<Vec<RecordBatch>> {
        let batches = run(self.algorithm, &self.graph, &self.args)?
            .into_iter()
            .map(|batch| self.names.rename_batch(batch))
            .collect::<Result<Vec<_>>>()?;
        if let Some(bad) = batches
            .iter()
            .find(|b| b.schema().fields() != self.schema.fields())
        {
            return exec_err!(
                "nutmeg: `{}` produced {:?}, declared {:?}",
                self.algorithm,
                bad.schema().fields(),
                self.schema.fields()
            );
        }
        Ok(batches)
    }
}

/// The listing of staged graphs.
#[derive(Debug)]
pub struct GraphsTable;

impl GraphsTable {
    pub fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("stagedNodes", DataType::Int64, false),
            Field::new("stagedEdges", DataType::Int64, false),
            Field::new("revision", DataType::Int64, false),
            Field::new("projections", DataType::Int64, false),
        ]))
    }

    pub fn batches() -> Result<Vec<RecordBatch>> {
        let rows: Vec<GraphInfo> = Registry::list()?
            .into_iter()
            .filter(|g| g.name != PROBE)
            .collect();
        let ints = |f: fn(&GraphInfo) -> usize| -> ArrayRef {
            Arc::new(Int64Array::from(
                rows.iter().map(|g| f(g) as i64).collect::<Vec<_>>(),
            ))
        };
        Ok(vec![RecordBatch::try_new(
            Self::arrow_schema(),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|g| g.name.as_str()).collect::<Vec<_>>(),
                )),
                ints(|g| g.staged_nodes),
                ints(|g| g.staged_edges),
                ints(|g| g.revision as usize),
                ints(|g| g.projections),
            ],
        )?])
    }
}

#[async_trait]
impl TableProvider for AlgorithmTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        MemTable::try_new(self.schema.clone(), vec![self.batches()?])?
            .scan(state, projection, filters, limit)
            .await
    }
}

#[async_trait]
impl TableProvider for GraphsTable {
    fn schema(&self) -> SchemaRef {
        Self::arrow_schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        MemTable::try_new(Self::arrow_schema(), vec![Self::batches()?])?
            .scan(state, projection, filters, limit)
            .await
    }
}

fn literal_string(expr: &Expr, position: usize) -> Result<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Ok(s.clone()),
        other => plan_err!("nutmeg: argument {position} must be a string literal, got {other}"),
    }
}

/// Parse a JSON object of options; empty text means none.
pub fn parse_options(json: &str) -> Result<serde_json::Map<String, serde_json::Value>> {
    if json.trim().is_empty() {
        return Ok(Default::default());
    }
    match serde_json::from_str(json) {
        Ok(serde_json::Value::Object(map)) => Ok(map),
        Ok(_) => plan_err!("nutmeg: options must be a JSON object"),
        Err(e) => plan_err!("nutmeg: options: {e}"),
    }
}

/// `nutmeg_<algorithm>('graph'[, '{json configuration}'])`. The configuration
/// is Grust's, plus Nutmeg's `columnNames` (`"grust"` or `"gds"`).
#[derive(Debug)]
struct AlgorithmFunction(&'static str);

impl TableFunctionImpl for AlgorithmFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let (graph, mut options) = match args {
            [g] => (literal_string(g, 1)?, Default::default()),
            [g, o] => (
                literal_string(g, 1)?,
                parse_options(&literal_string(o, 2)?)?,
            ),
            _ => {
                return plan_err!(
                    "nutmeg_{}(graph[, configuration]) takes one or two arguments",
                    snake(self.0)
                );
            }
        };
        let names = ColumnNames::take(&mut options)?;
        Ok(Arc::new(AlgorithmTable::named(
            self.0, graph, &options, names,
        )?))
    }
}

/// `nutmeg_graphs()`.
#[derive(Debug)]
struct GraphsFunction;

impl TableFunctionImpl for GraphsFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if !args.is_empty() {
            return plan_err!("nutmeg_graphs() takes no arguments");
        }
        Ok(Arc::new(GraphsTable))
    }
}

/// One table function per Grust algorithm, plus `nutmeg_graphs`.
pub fn table_functions() -> Vec<(String, Arc<TableFunction>)> {
    let mut out: Vec<(String, Arc<TableFunction>)> = algorithm_names()
        .into_iter()
        .map(|name| {
            let sql = format!("nutmeg_{}", snake(name));
            (
                sql.clone(),
                Arc::new(TableFunction::new(sql, Arc::new(AlgorithmFunction(name)))),
            )
        })
        .collect();
    out.push((
        "nutmeg_graphs".into(),
        Arc::new(TableFunction::new(
            "nutmeg_graphs".into(),
            Arc::new(GraphsFunction),
        )),
    ));
    out
}

/// Register every table function in a DataFusion session.
pub fn register(ctx: &SessionContext) {
    for (name, function) in table_functions() {
        ctx.register_udtf(&name, function.function().clone());
    }
}

#[cfg(test)]
mod tests;
