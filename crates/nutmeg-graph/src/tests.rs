use super::*;
use arrow::array::{Float64Array, Int32Array};

fn edges(source: &[&str], target: &[&str], weight: Option<&[f64]>) -> RecordBatch {
    let mut fields = vec![
        Field::new("src", DataType::Utf8, false),
        Field::new("dst", DataType::Utf8, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(source.to_vec())),
        Arc::new(StringArray::from(target.to_vec())),
    ];
    if let Some(w) = weight {
        fields.push(Field::new("w", DataType::Float64, false));
        columns.push(Arc::new(Float64Array::from(w.to_vec())));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn no_options() -> serde_json::Map<String, serde_json::Value> {
    Default::default()
}

#[test]
fn every_algorithm_grust_registers_is_served_with_its_declared_columns() {
    let names = algorithm_names();
    assert!(names.len() >= 12, "{names:?}");
    for definition in definitions() {
        let name = short(definition);
        let schema = output_schema(name).unwrap_or_else(|e| panic!("{name}: {e}"));
        let produced: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let declared: Vec<&str> = definition.outputs.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(produced, declared, "{name}");
    }
}

#[test]
fn names_resolve_in_either_spelling() {
    assert_eq!(resolve_algorithm("shortest_paths"), Some("shortestPaths"));
    assert_eq!(resolve_algorithm("shortestPaths"), Some("shortestPaths"));
    assert_eq!(resolve_algorithm("PAGERANK"), Some("pagerank"));
    // Louvain stood here as the unknown name until Grust registered it.
    assert_eq!(resolve_algorithm("louvain"), Some("louvain"));
    assert_eq!(resolve_algorithm("not_an_algorithm"), None);
    assert_eq!(snake("multiSourceBfs"), "multi_source_bfs");
}

#[test]
fn grust_sail_table_columns_are_recognized_without_a_mapping() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("src_id", DataType::Utf8, false),
            Field::new("dst_id", DataType::Utf8, false),
            Field::new("edge_type", DataType::Utf8, false),
            Field::new("props", DataType::Utf8, false),
            Field::new("hops", DataType::Int32, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["e1"])),
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(StringArray::from(vec!["b"])),
            Arc::new(StringArray::from(vec!["KNOWS"])),
            Arc::new(StringArray::from(vec!["{}"])),
            Arc::new(Int32Array::from(vec![Some(3)])),
        ],
    )
    .unwrap();
    let out = normalize_edges(&batch, &ColumnMapping::default()).unwrap();
    let names: Vec<_> = out
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        names,
        [
            "source",
            "target",
            "label",
            "edge_id",
            "property.hops",
            "present.hops"
        ]
    );
    assert_eq!(out.column(4).data_type(), &DataType::Int64);
}

#[test]
fn unknown_options_are_rejected_by_grusts_validator() {
    let mut options = no_options();
    options.insert("dampng".into(), serde_json::json!(0.5));
    let error = validate("pagerank", &options).unwrap_err().to_string();
    assert!(error.contains("dampng"), "{error}");
    let error = validate("bfs", &no_options()).unwrap_err().to_string();
    assert!(error.contains("needs `source`"), "{error}");
}

#[test]
fn string_options_take_the_declared_type_and_spelling() {
    let options = options_from_strings(
        "pagerank",
        [
            ("maxiterations".to_string(), "20".to_string()),
            ("damping".to_string(), "0.9".to_string()),
            ("relationshiptypes".to_string(), "[\"KNOWS\"]".to_string()),
        ],
    )
    .unwrap();
    assert_eq!(options["maxIterations"], serde_json::json!(20));
    assert_eq!(options["damping"], serde_json::json!(0.9));
    assert_eq!(options["relationshipTypes"], serde_json::json!(["KNOWS"]));
    validate("pagerank", &options).unwrap();
    // A numeric-looking node id stays a string.
    let options = options_from_strings("bfs", [("source".to_string(), "42".to_string())]).unwrap();
    assert_eq!(options["source"], serde_json::json!("42"));
}

#[test]
fn projections_are_cached_per_option_set_and_dropped_on_restage() {
    let name = "cache-test";
    let mapping = ColumnMapping::default();
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&["a", "b"], &["b", "c"], None)],
        &mapping,
        true,
    )
    .unwrap();
    let directed = validate("wcc", &no_options()).unwrap();
    let mut undirected = no_options();
    undirected.insert("orientation".into(), serde_json::json!("undirected"));
    let undirected = validate("wcc", &undirected).unwrap();
    Registry::projection(name, &directed).unwrap();
    Registry::projection(name, &directed).unwrap();
    Registry::projection(name, &undirected).unwrap();
    let info = |n: &str| {
        Registry::list()
            .unwrap()
            .into_iter()
            .find(|g| g.name == n)
            .unwrap()
    };
    assert_eq!(info(name).projections, 2);
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&["c"], &["d"], None)],
        &mapping,
        false,
    )
    .unwrap();
    assert_eq!(info(name).projections, 0);
    assert_eq!(info(name).staged_edges, 3);
    assert!(Registry::drop(name).unwrap());
}

#[test]
fn explicit_nodes_make_unknown_endpoints_an_error() {
    let name = "explicit-nodes";
    let nodes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )
    .unwrap();
    let mapping = ColumnMapping::default();
    Registry::stage(name, Part::Nodes, &[nodes], &mapping, true).unwrap();
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&["1"], &["9"], None)],
        &mapping,
        true,
    )
    .unwrap();
    let error = Registry::projection(name, &validate("degree", &no_options()).unwrap())
        .err()
        .expect("unknown endpoint")
        .to_string();
    assert!(error.contains("missing target"), "{error}");
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&["1"], &["2"], None)],
        &mapping,
        true,
    )
    .unwrap();
    Registry::projection(name, &validate("degree", &no_options()).unwrap()).unwrap();
}

#[tokio::test]
async fn every_algorithm_runs_through_sql() -> Result<()> {
    Registry::stage(
        "sql",
        Part::Edges,
        &[edges(
            &["a", "b", "c", "a"],
            &["b", "c", "a", "c"],
            Some(&[1.0, 2.0, 3.0, 10.0]),
        )],
        &ColumnMapping::default(),
        true,
    )?;
    let ctx = SessionContext::new();
    register(&ctx);
    let count = |sql: &str| {
        let ctx = ctx.clone();
        let sql = sql.to_string();
        async move {
            let batches = ctx.sql(&sql).await?.collect().await?;
            Ok::<usize, DataFusionError>(batches.iter().map(|b| b.num_rows()).sum())
        }
    };
    assert_eq!(count("SELECT * FROM nutmeg_degree('sql')").await?, 3);
    assert_eq!(
        count("SELECT * FROM nutmeg_pagerank('sql', '{\"damping\": 0.9}')").await?,
        3
    );
    assert_eq!(
        count("SELECT * FROM nutmeg_bfs('sql', '{\"source\": \"a\"}')").await?,
        3
    );
    assert_eq!(
        count("SELECT * FROM nutmeg_multi_source_bfs('sql', '{\"sources\": [\"a\", \"b\"]}')")
            .await?,
        3
    );
    assert_eq!(
        count("SELECT * FROM nutmeg_dfs('sql', '{\"source\": \"a\"}')").await?,
        3
    );
    assert_eq!(count("SELECT * FROM nutmeg_wcc('sql')").await?, 3);
    assert_eq!(count("SELECT * FROM nutmeg_scc('sql')").await?, 3);
    assert_eq!(
        count("SELECT * FROM nutmeg_topological_sort('sql')").await?,
        1
    );
    assert_eq!(
        count("SELECT * FROM nutmeg_projection_stats('sql')").await?,
        1
    );
    assert_eq!(count("SELECT * FROM nutmeg_estimate_csr('sql')").await?, 1);
    assert!(count("SELECT * FROM nutmeg_graphs()").await? >= 1);
    assert!(
        count("SELECT * FROM nutmeg_shortest_paths('sql', '{\"source\": \"a\", \"weightProperty\": \"w\"}')").await? >= 2
    );

    // Values, not only shapes: weighted Dijkstra from a; a→c directly costs 10, via b costs 3.
    let batches = ctx
        .sql(
            "SELECT \"nodeId\", distance FROM nutmeg_dijkstra('sql', \
             '{\"source\": \"a\", \"weightProperty\": \"w\"}') ORDER BY \"nodeId\"",
        )
        .await?
        .collect()
        .await?;
    let ids = cast(batches[0].column(0), &DataType::Utf8)?;
    let ids = ids.as_any().downcast_ref::<StringArray>().unwrap();
    let dist = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let got: Vec<(&str, f64)> = (0..ids.len())
        .map(|i| (ids.value(i), dist.value(i)))
        .collect();
    assert_eq!(got, [("a", 0.0), ("b", 1.0), ("c", 3.0)]);

    // PageRank scores form a probability distribution.
    let batches = ctx
        .sql("SELECT score FROM nutmeg_pagerank('sql')")
        .await?
        .collect()
        .await?;
    let scores = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let total: f64 = scores.iter().flatten().sum();
    assert!((total - 1.0).abs() < 1e-6, "{total}");

    // The graph composes with ordinary SQL.
    let joined = count(
        "SELECT d.\"nodeId\" FROM nutmeg_degree('sql') d JOIN nutmeg_wcc('sql') w ON d.\"nodeId\" = w.\"nodeId\"",
    )
    .await?;
    assert_eq!(joined, 3);

    let error = ctx
        .sql("SELECT * FROM nutmeg_degree('nope')")
        .await?
        .collect()
        .await;
    assert!(format!("{:?}", error.err()).contains("no graph named"));
    Ok(())
}

/// Node properties arrive the way a Spark DataFrame hands them over: raw column
/// names, narrower types than a kernel reads, and rows in no particular order.
/// Until staging kept them, every node column but the id and label was dropped,
/// so no kernel that reads node properties could run on a staged graph.
///
/// The check is a value, not a presence. Two triangles joined by one edge,
/// partitioned into the triangles, have modularity 5/14 by hand: m = 7 edges,
/// each triangle has 3 internal edges and degree total 7, so
/// Q = 2 * (3/7 - (7/14)^2) = 5/14. Nodes are staged in reverse, so a row
/// misalignment between the staged columns and the projection would scramble
/// the partition and change Q, not merely fail to find it. Both failures were
/// checked by breaking the fix on purpose: dropping node columns, and
/// misassigning communities to rows, each fail this test.
#[test]
fn staged_node_columns_reach_the_kernels_that_read_them() {
    let name = "node-properties";
    let nodes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            // Int32 and Float32: narrower than the kernels read, so staging casts.
            Field::new("community", DataType::Int32, false),
            Field::new("lat", DataType::Float32, false),
            Field::new("lon", DataType::Float32, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["f", "e", "d", "c", "b", "a"])),
            Arc::new(Int32Array::from(vec![1, 1, 1, 0, 0, 0])),
            // One point for every node: the heuristic is zero, so A* is
            // Dijkstra and its answer is the shortest hop count.
            Arc::new(arrow::array::Float32Array::from(vec![10.0; 6])),
            Arc::new(arrow::array::Float32Array::from(vec![20.0; 6])),
        ],
    )
    .unwrap();
    let mapping = ColumnMapping::default();
    Registry::stage(name, Part::Nodes, &[nodes], &mapping, true).unwrap();
    Registry::stage(
        name,
        Part::Edges,
        &[edges(
            &["a", "b", "c", "c", "d", "e", "f"],
            &["b", "c", "a", "d", "e", "f", "d"],
            None,
        )],
        &mapping,
        true,
    )
    .unwrap();

    let options: serde_json::Map<String, serde_json::Value> = serde_json::from_value(
        serde_json::json!({ "orientation": "undirected", "communityProperty": "community" }),
    )
    .unwrap();
    let batches = run(
        "modularity",
        name,
        &validate("modularity", &options).unwrap(),
    )
    .unwrap();
    let batch = &batches[0];
    let sizes = batch
        .column_by_name("size")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(batch.num_rows(), 2, "one row per community");
    assert!(sizes.iter().all(|size| size == Some(3)), "{sizes:?}");
    let total = batch
        .column_by_name("totalModularity")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    // A sum of a few terms against a hand-computed quotient: equal to within
    // rounding, not bit for bit.
    assert!((total - 5.0 / 14.0).abs() < 1e-12, "modularity {total}");

    let options: serde_json::Map<String, serde_json::Value> =
        serde_json::from_value(serde_json::json!({
            "source": "a", "target": "f", "orientation": "undirected",
            "latitudeProperty": "lat", "longitudeProperty": "lon",
        }))
        .unwrap();
    let batches = run("astar", name, &validate("astar", &options).unwrap()).unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // a - c - d - f: three hops, four nodes on the path.
    assert_eq!(rows, 4, "{batches:?}");
    let total = batches[0]
        .column_by_name("totalCost")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 3.0);
    assert!(Registry::drop(name).unwrap());
}
