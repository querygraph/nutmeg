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

use arrow::array::{Array, ArrayRef, Int64Array, StringArray, new_null_array};
use arrow::compute::{cast, is_not_null};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunction, TableFunctionImpl, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{Expr, TableType};
use grust_algorithms::{
    ArrowResultCursor, CsrEstimate, GraphProjection, ProjectionOptions, WeightSelection,
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

// Grust's registry keeps names case-folded. These are the spellings Grust
// registers them under, used for display and to derive SQL names; a name
// not listed here is shown as the registry has it.
const SPELLINGS: [&str; 5] = [
    "estimateCsr",
    "multiSourceBfs",
    "projectionStats",
    "shortestPaths",
    "topologicalSort",
];

fn spelled(registered: &'static str) -> &'static str {
    SPELLINGS
        .iter()
        .copied()
        .find(|s| s.eq_ignore_ascii_case(registered))
        .unwrap_or(registered)
}

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

/// Rename node rows into the grust-arrow layout: `node_id`, `label`.
pub fn normalize_nodes(batch: &RecordBatch, mapping: &ColumnMapping) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let (id, _) = pick(batch, &mapping.id, &["node_id", "id"], "node id", true)?.expect("required");
    let label = match pick(batch, &mapping.label, &["label"], "node label", false)? {
        Some((column, _)) => utf8(column)?,
        None => constant("", rows),
    };
    Ok(RecordBatch::try_new(node_schema(), vec![utf8(id)?, label])?)
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

/// Run one Grust algorithm on the named graph.
pub fn run(
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
        // new registration is served here with no code of Nutmeg's own.
        _ => grust_algorithm_procedures::run_on_projection(algorithm, g, args).map_err(err)?,
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
    validate(algorithm, &options)
}

const PROBE: &str = "nutmeg.schema-probe";

static SCHEMAS: Lazy<RwLock<HashMap<String, SchemaRef>>> = Lazy::new(Default::default);

/// The Arrow schema an algorithm's result has: the kernel's own, observed by
/// running it once on a three-node graph, not a transcription of it.
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
    }
    // A kernel defined on undirected graphs refuses the default directed
    // projection, and says so; probe it on an undirected one instead.
    let batches = match run(algorithm, PROBE, &probe_args(algorithm, None)?) {
        Err(error) if error.to_string().contains("undirected") => run(
            algorithm,
            PROBE,
            &probe_args(algorithm, Some("undirected"))?,
        )?,
        other => other?,
    };
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
    schema: SchemaRef,
}

impl AlgorithmTable {
    pub fn new(
        algorithm: &str,
        graph: String,
        options: &serde_json::Map<String, serde_json::Value>,
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
            schema: output_schema(algorithm)?,
        })
    }

    pub fn batches(&self) -> Result<Vec<RecordBatch>> {
        let batches = run(self.algorithm, &self.graph, &self.args)?;
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

/// `nutmeg_<algorithm>('graph'[, '{json configuration}'])`.
#[derive(Debug)]
struct AlgorithmFunction(&'static str);

impl TableFunctionImpl for AlgorithmFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let (graph, options) = match args {
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
        Ok(Arc::new(AlgorithmTable::new(self.0, graph, &options)?))
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
