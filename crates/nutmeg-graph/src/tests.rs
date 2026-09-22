use super::*;
use arrow::array::{Float64Array, Int32Array};
use std::collections::BTreeSet;

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
        let produced: Vec<bool> = schema.fields().iter().map(|f| f.is_nullable()).collect();
        let declared: Vec<bool> = definition.outputs.iter().map(|f| f.nullable).collect();
        assert_eq!(produced, declared, "{name}: nullability");
    }
}

/// Whether an Arrow type is, or contains, an unsigned integer.
fn unsigned(data_type: &DataType) -> bool {
    match data_type {
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => true,
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => unsigned(item.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| unsigned(f.data_type())),
        DataType::Dictionary(key, value) => unsigned(key) || unsigned(value),
        _ => false,
    }
}

/// Spark has no unsigned integer types: the Spark Connect client refuses a
/// `uint64` column outright ("uint64 is not supported in conversion to
/// Arrow"), so one unsigned column makes a whole read fail. No column Nutmeg
/// serves may be unsigned, at any depth: every algorithm's result, observed
/// from a real run, and the graph listing.
#[test]
fn no_column_nutmeg_serves_is_unsigned() {
    let mut schemas = vec![("graphs".to_string(), GraphsTable::arrow_schema())];
    for name in algorithm_names() {
        let schema = output_schema(name).unwrap_or_else(|e| panic!("{name}: {e}"));
        schemas.push((name.to_string(), schema));
    }
    assert!(algorithm_names().contains(&"pagerank") && algorithm_names().contains(&"degree"));
    for (name, schema) in &schemas {
        for field in schema.fields() {
            assert!(
                !unsigned(field.data_type()),
                "{name}.{} is {}, which Spark cannot represent",
                field.name(),
                field.data_type()
            );
        }
    }
    // The check itself sees an unsigned type, including inside a list.
    assert!(unsigned(&DataType::UInt64));
    assert!(unsigned(&DataType::new_large_list(DataType::UInt64, true)));
    assert!(!unsigned(&DataType::Int64));
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
        StageOrder::Canonical,
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
        StageOrder::Canonical,
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
    Registry::stage(
        name,
        Part::Nodes,
        &[nodes],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&["1"], &["9"], None)],
        &mapping,
        true,
        StageOrder::Canonical,
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
        StageOrder::Canonical,
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
        StageOrder::Canonical,
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
    Registry::stage(
        name,
        Part::Nodes,
        &[nodes],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
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
        StageOrder::Canonical,
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

/// The alias table is data about Grust's registry, so it is checked against
/// the registry: every Grust name it renames is one some kernel declares, no
/// rename gives two columns of one kernel the same name, and no kernel claims
/// the option that chooses the names.
#[test]
fn gds_aliases_name_declared_columns_and_never_collide() {
    let declared: HashSet<&str> = definitions()
        .into_iter()
        .flat_map(|d| d.outputs.iter().map(|f| f.name.as_str()))
        .collect();
    for (grust, gds) in GDS_COLUMN_ALIASES {
        assert!(declared.contains(grust), "alias for undeclared `{grust}`");
        assert_ne!(grust, gds);
    }
    for definition in definitions() {
        let name = short(definition);
        let schema = Arc::new(Schema::new(
            definition
                .outputs
                .iter()
                .map(|f| Field::new(f.name.as_str(), DataType::Null, true))
                .collect::<Vec<_>>(),
        ));
        ColumnNames::Gds
            .rename_schema(&schema)
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        for field in definition
            .options
            .iter()
            .map(|o| &o.field)
            .chain(definition.arguments.iter().map(|a| &a.field))
        {
            assert!(
                !field.name.eq_ignore_ascii_case(COLUMN_NAMES_OPTION),
                "{name} declares `{}`, which Nutmeg takes for itself",
                field.name
            );
        }
    }
}

/// For every kernel and both namings, the schema a table reports is the one
/// its scan returns: `batches` re-checks every batch against it, and fails if
/// a rename were applied to one and not the other.
#[test]
fn every_algorithm_reports_the_columns_its_scan_returns_under_either_naming() {
    for definition in definitions() {
        let name = short(definition);
        output_schema(name).unwrap_or_else(|e| panic!("{name}: {e}"));
        let (args, _) = probe(name).unwrap_or_else(|e| panic!("{name}: {e}"));
        let args = Arc::new(args);
        for names in [ColumnNames::Grust, ColumnNames::Gds] {
            let table = AlgorithmTable {
                algorithm: name,
                graph: PROBE.to_string(),
                args: args.clone(),
                names,
                schema: output_schema_named(name, names).unwrap(),
            };
            let batches = table
                .batches()
                .unwrap_or_else(|e| panic!("{name} {names:?}: {e}"));
            assert!(!batches.is_empty(), "{name} {names:?}");
            let got: Vec<String> = batches[0]
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            let expected: Vec<&str> = definition
                .outputs
                .iter()
                .map(|f| names.rename(&f.name))
                .collect();
            assert_eq!(got, expected, "{name} {names:?}");
        }
    }
}

#[tokio::test]
async fn gds_names_are_chosen_per_read_and_grust_names_stay_reachable() -> Result<()> {
    Registry::stage(
        "gds-names",
        Part::Edges,
        &[edges(&["a", "b", "a"], &["b", "c", "c"], None)],
        &ColumnMapping::default(),
        true,
        StageOrder::Canonical,
    )?;
    let ctx = SessionContext::new();
    register(&ctx);
    let columns = |sql: &str| {
        let ctx = ctx.clone();
        let sql = sql.to_string();
        async move {
            let frame = ctx.sql(&sql).await?;
            let names: Vec<String> = frame
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            let rows: usize = frame.collect().await?.iter().map(|b| b.num_rows()).sum();
            Ok::<_, DataFusionError>((names, rows))
        }
    };
    let (grust, rows) = columns(
        "SELECT * FROM nutmeg_yens('gds-names', '{\"source\": \"a\", \"target\": \"c\", \"k\": 2}')",
    )
    .await?;
    assert!(grust.contains(&"pathIndex".to_string()), "{grust:?}");
    assert_eq!(rows, 2);
    let (gds, rows) = columns(
        "SELECT * FROM nutmeg_yens('gds-names', \
         '{\"source\": \"a\", \"target\": \"c\", \"k\": 2, \"columnNames\": \"gds\"}')",
    )
    .await?;
    assert!(gds.contains(&"index".to_string()), "{gds:?}");
    assert!(!gds.contains(&"pathIndex".to_string()), "{gds:?}");
    assert_eq!(rows, 2);
    let (gds, _) = columns(
        "SELECT \"nodeId\", \"ranIterations\", \"didConverge\" \
         FROM nutmeg_pagerank('gds-names', '{\"columnNames\": \"GDS\"}')",
    )
    .await?;
    assert_eq!(gds, ["nodeId", "ranIterations", "didConverge"]);
    let error = match ctx
        .sql("SELECT * FROM nutmeg_pagerank('gds-names', '{\"columnNames\": \"neo4j\"}')")
        .await
    {
        Ok(_) => panic!("an unknown naming was accepted"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("columnNames"), "{error}");
    assert!(Registry::drop("gds-names").unwrap());
    Ok(())
}

/// `linkPrediction` reads a node property for `sameCommunity` alone, so the
/// same kernel runs through both of Nutmeg's paths; `allPairsShortestPaths`
/// streams through its own cursor. Values, on a path a - b - c - d whose
/// communities are {a, b, c} and {d}: the distance-two pairs are (a, c) and
/// (b, d), sharing one neighbour each, in one community and in two.
#[test]
fn link_prediction_and_all_pairs_serve_their_values() {
    let name = "link-prediction";
    let nodes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("community", DataType::Int32, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![0, 0, 0, 1])),
        ],
    )
    .unwrap();
    let mapping = ColumnMapping::default();
    Registry::stage(
        name,
        Part::Nodes,
        &[nodes],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&["a", "b", "c"], &["b", "c", "d"], None)],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    let scores = |metric: &str| {
        let options: serde_json::Map<String, serde_json::Value> =
            serde_json::from_value(serde_json::json!({
                "orientation": "undirected", "metric": metric,
                "communityProperty": "community",
            }))
            .unwrap();
        let args = validate("linkPrediction", &options).unwrap();
        let batches = run("linkPrediction", name, &args).unwrap();
        let mut out = Vec::new();
        for batch in &batches {
            let column = |c: &str| cast(batch.column_by_name(c).unwrap(), &DataType::Utf8).unwrap();
            let (first, second) = (column("node1"), column("node2"));
            let first = first.as_any().downcast_ref::<StringArray>().unwrap();
            let second = second.as_any().downcast_ref::<StringArray>().unwrap();
            let score = batch
                .column_by_name("score")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                out.push((
                    first.value(row).to_string(),
                    second.value(row).to_string(),
                    score.value(row),
                ));
            }
        }
        out
    };
    let pair = |a: &str, b: &str, s: f64| (a.to_string(), b.to_string(), s);
    assert_eq!(
        scores("sameCommunity"),
        [pair("a", "c", 1.0), pair("b", "d", 0.0)]
    );
    assert_eq!(
        scores("commonNeighbors"),
        [pair("a", "c", 1.0), pair("b", "d", 1.0)]
    );

    let distances = |options: serde_json::Value| {
        let options: serde_json::Map<String, serde_json::Value> =
            serde_json::from_value(options).unwrap();
        let args = validate("allPairsShortestPaths", &options).unwrap();
        let mut out = BTreeMap::new();
        for batch in run("allPairsShortestPaths", name, &args).unwrap() {
            let column = |c: &str| cast(batch.column_by_name(c).unwrap(), &DataType::Utf8).unwrap();
            let (source, target) = (column("sourceNodeId"), column("targetNodeId"));
            let source = source.as_any().downcast_ref::<StringArray>().unwrap();
            let target = target.as_any().downcast_ref::<StringArray>().unwrap();
            let distance = batch
                .column_by_name("distance")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                out.insert(
                    (source.value(row).to_string(), target.value(row).to_string()),
                    distance.value(row),
                );
            }
        }
        out
    };
    let all = distances(serde_json::json!({}));
    let key = |a: &str, b: &str| (a.to_string(), b.to_string());
    assert_eq!(all.get(&key("a", "b")), Some(&1.0), "{all:?}");
    assert_eq!(all.get(&key("a", "c")), Some(&2.0), "{all:?}");
    assert_eq!(all.get(&key("a", "d")), Some(&3.0), "{all:?}");
    assert_eq!(all.get(&key("b", "d")), Some(&2.0), "{all:?}");
    let from_b = distances(serde_json::json!({ "sourceNodes": ["b"] }));
    assert!(!from_b.is_empty());
    assert!(from_b.keys().all(|(source, _)| source == "b"), "{from_b:?}");
    assert_eq!(from_b.get(&key("b", "d")), Some(&2.0), "{from_b:?}");
    assert!(Registry::drop(name).unwrap());
}

/// Every kernel that declares a nullable output is read on a graph where that
/// column holds a null, and the reported schema says nullable for it and for
/// nothing Grust declares non-nullable.
///
/// The schema is observed on a three-node probe on which every node reaches
/// every other, so most nullable columns are full there. Grust's Arrow cursors
/// set a column's nullable flag from whether that batch holds a null, so the
/// probe used to report `distance` as non-nullable and every read with an
/// unreachable node failed its schema check (`dijkstra` in the Citi Bike
/// example). The reverse failed too: `degree` probes unweighted, where
/// `strength` is all null, so a weighted read, where it is full, was refused.
///
/// The graph: a weighted cycle a → b → c → a, and d, staged as a node with no
/// edges. From a, d is unreachable; the cycle leaves `longestPath` without
/// distances; d has no neighbours for a clustering coefficient; d alone in its
/// community has no volume for a conductance; unweighted, `degree` has no
/// strength.
#[test]
fn declared_nullable_outputs_are_nullable_whatever_the_rows_hold() {
    let name = "nullable-outputs";
    let nodes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("community", DataType::Int32, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![0, 0, 0, 1])),
        ],
    )
    .unwrap();
    let mapping = ColumnMapping::default();
    Registry::stage(
        name,
        Part::Nodes,
        &[nodes],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    Registry::stage(
        name,
        Part::Edges,
        &[edges(
            &["a", "b", "c"],
            &["b", "c", "a"],
            Some(&[1.0, 2.0, 3.0]),
        )],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();

    // Kernel, options, the declared-nullable column that holds a null here.
    let cases: Vec<(&str, serde_json::Value, &str)> = vec![
        ("degree", serde_json::json!({}), "strength"),
        ("bfs", serde_json::json!({ "source": "a" }), "distance"),
        (
            "dijkstra",
            serde_json::json!({ "source": "a", "weightProperty": "w" }),
            "distance",
        ),
        (
            "multiSourceBfs",
            serde_json::json!({ "sources": ["a"] }),
            "distance",
        ),
        ("longestPath", serde_json::json!({}), "distance"),
        (
            "localClusteringCoefficient",
            serde_json::json!({ "orientation": "undirected" }),
            "coefficient",
        ),
        (
            "modularity",
            serde_json::json!({ "orientation": "undirected", "communityProperty": "community" }),
            "conductance",
        ),
        (
            "bellmanFord",
            serde_json::json!({ "source": "a", "weightProperty": "w" }),
            "distance",
        ),
    ];

    // The cases cover exactly the kernels that declare a nullable output, so a
    // new nullable declaration fails here until it is read with a null.
    let declared: BTreeMap<&str, Vec<&str>> = definitions()
        .into_iter()
        .map(|d| {
            let nullable = d
                .outputs
                .iter()
                .filter(|f| f.nullable)
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>();
            (short(d), nullable)
        })
        .filter(|(_, nullable)| !nullable.is_empty())
        .collect();
    let covered: BTreeMap<&str, Vec<&str>> = cases
        .iter()
        .map(|(kernel, _, column)| (*kernel, vec![*column]))
        .collect();
    assert_eq!(covered, declared);

    let read = |graph: &str, kernel: &str, options: &serde_json::Value| {
        let options: serde_json::Map<String, serde_json::Value> =
            serde_json::from_value(options.clone()).unwrap();
        let table = AlgorithmTable::new(kernel, graph.to_string(), &options)
            .unwrap_or_else(|e| panic!("{kernel}: {e}"));
        let batches = table.batches().unwrap_or_else(|e| panic!("{kernel}: {e}"));
        (table.schema(), batches)
    };
    for (kernel, options, column) in &cases {
        let (schema, batches) = read(name, kernel, options);
        let definition = definition_of(kernel).unwrap();
        for (field, declared) in schema.fields().iter().zip(&definition.outputs) {
            assert_eq!(
                field.is_nullable(),
                declared.nullable,
                "{kernel}.{}",
                field.name()
            );
        }
        let nulls: usize = batches
            .iter()
            .map(|b| b.column_by_name(column).unwrap().null_count())
            .sum();
        assert!(
            nulls > 0,
            "{kernel}: `{column}` holds no null on this graph"
        );
        for batch in &batches {
            assert_eq!(batch.schema().fields(), schema.fields(), "{kernel}");
        }
    }

    // The same columns full, on the cycle alone: the flag must not follow the
    // rows the other way either.
    let full = "nullable-outputs-full";
    Registry::stage(
        full,
        Part::Edges,
        &[edges(
            &["a", "b", "c"],
            &["b", "c", "a"],
            Some(&[1.0, 2.0, 3.0]),
        )],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    for (kernel, options, column) in [
        (
            "degree",
            serde_json::json!({ "weightProperty": "w" }),
            "strength",
        ),
        (
            "dijkstra",
            serde_json::json!({ "source": "a", "weightProperty": "w" }),
            "distance",
        ),
    ] {
        let (schema, batches) = read(full, kernel, &options);
        let nulls: usize = batches
            .iter()
            .map(|b| b.column_by_name(column).unwrap().null_count())
            .sum();
        assert_eq!(nulls, 0, "{kernel}: `{column}` should be full here");
        assert!(
            schema.field_with_name(column).unwrap().is_nullable(),
            "{kernel}"
        );
        for batch in &batches {
            assert!(
                batch
                    .schema()
                    .field_with_name(column)
                    .unwrap()
                    .is_nullable(),
                "{kernel}"
            );
        }
    }
    assert!(Registry::drop(name).unwrap());
    assert!(Registry::drop(full).unwrap());
}

// ------------------------------------------------------------ canonical order

/// A small deterministic generator, so a fixture is the same on every run
/// and needs no dependency.
struct Mix(u64);

impl Mix {
    fn next(&mut self) -> u64 {
        // SplitMix64.
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            items.swap(i, self.below(i + 1));
        }
    }
}

/// Three planted communities of ten nodes, dense inside and sparse between.
/// Ids sort differently as text and as numbers (`"10"` before `"9"`), and
/// include the probe's `a`, `b`, `c`, which every kernel's probe arguments
/// name. Every node carries a column for each property option any registered
/// kernel declares ([`probe_key`]). Edges carry a weight `w`, and every
/// seventh pair is joined a second time with a different weight and no
/// `edge_id`, so the tie-break past `source`, `target` is exercised.
struct Fixture {
    /// (id, community) per node.
    nodes: Vec<(String, i64)>,
    /// (source, target, w) per edge.
    edges: Vec<(String, String, f64)>,
}

fn fixture() -> Fixture {
    let mut mix = Mix(7);
    let mut ids: Vec<String> = ["a", "b", "c"].map(String::from).to_vec();
    ids.extend((1..=27).map(|i| i.to_string()));
    let nodes: Vec<(String, i64)> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.clone(), (i / 10) as i64))
        .collect();
    let mut edges = Vec::new();
    for (i, (u, cu)) in nodes.iter().enumerate() {
        for (j, (v, cv)) in nodes.iter().enumerate() {
            let chance = if cu == cv { 30 } else { 3 };
            if i != j && mix.below(100) < chance {
                edges.push((u.clone(), v.clone(), (1 + mix.below(5)) as f64));
            }
        }
    }
    let parallel: Vec<_> = edges.iter().step_by(7).cloned().collect();
    for (u, v, w) in parallel {
        edges.push((u, v, w + 0.5));
    }
    Fixture { nodes, edges }
}

/// Split `order` into consecutive runs of the given sizes.
fn runs<'a>(order: &'a [usize], sizes: &[usize]) -> Vec<&'a [usize]> {
    let mut out = Vec::new();
    let mut start = 0;
    for &size in sizes {
        out.push(&order[start..start + size]);
        start += size;
    }
    out
}

impl Fixture {
    /// Node rows in `order`, as batches of the given sizes.
    fn node_batches(&self, order: &[usize], sizes: &[usize]) -> Vec<RecordBatch> {
        runs(order, sizes)
            .into_iter()
            .map(|rows| self.node_batch(rows))
            .collect()
    }

    fn node_batch(&self, rows: &[usize]) -> RecordBatch {
        let community = |i: &usize| self.nodes[*i].1;
        let mut fields = vec![Field::new("id", DataType::Utf8, false)];
        let mut columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(
            rows.iter()
                .map(|i| self.nodes[*i].0.as_str())
                .collect::<Vec<_>>(),
        ))];
        let mut seen = HashSet::new();
        for name in grust_algorithm_procedures::projection_kernel_names() {
            for declared in grust_algorithm_procedures::node_property_options(name).unwrap() {
                let key = probe_key(declared.option, declared.kind);
                if !seen.insert(key.clone()) {
                    continue;
                }
                let values: ArrayRef =
                    match declared.kind {
                        PropertyKind::Number => Arc::new(Float64Array::from(
                            rows.iter()
                                .map(|i| (*i as f64 * 7.0) % 90.0)
                                .collect::<Vec<_>>(),
                        )),
                        PropertyKind::Integer => Arc::new(Int64Array::from(
                            rows.iter().map(community).collect::<Vec<_>>(),
                        )),
                        PropertyKind::Category => Arc::new(StringArray::from(
                            rows.iter()
                                .map(|i| ["x", "y", "z"][community(i) as usize])
                                .collect::<Vec<_>>(),
                        )),
                        PropertyKind::Vector => Arc::new(
                            FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                                rows.iter().map(|i| {
                                    let c = community(i) as f32;
                                    Some(vec![Some(1.0 + c), Some(1.0 + (*i % 3) as f32)])
                                }),
                                2,
                            ),
                        ),
                    };
                fields.push(Field::new(&key, values.data_type().clone(), true));
                columns.push(values);
            }
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    /// Edge rows in `order`, as batches of the given sizes.
    fn edge_batches(&self, order: &[usize], sizes: &[usize]) -> Vec<RecordBatch> {
        runs(order, sizes)
            .into_iter()
            .map(|rows| {
                let source: Vec<&str> = rows.iter().map(|i| self.edges[*i].0.as_str()).collect();
                let target: Vec<&str> = rows.iter().map(|i| self.edges[*i].1.as_str()).collect();
                let w: Vec<f64> = rows.iter().map(|i| self.edges[*i].2).collect();
                edges(&source, &target, Some(&w))
            })
            .collect()
    }
}

/// `n` positions shuffled by `seed`, and batch sizes, also drawn from `seed`,
/// that cover them.
fn arrival(n: usize, seed: u64) -> (Vec<usize>, Vec<usize>) {
    let mut mix = Mix(seed);
    let mut order: Vec<usize> = (0..n).collect();
    mix.shuffle(&mut order);
    let mut sizes = Vec::new();
    let mut left = n;
    while left > 0 {
        let size = (1 + mix.below(n / 3 + 1)).min(left);
        sizes.push(size);
        left -= size;
    }
    (order, sizes)
}

/// Every cell of every result row as text, rows in the order they came: two
/// runs are identical exactly when these are equal. Floats print at full
/// round-trip precision, so a difference in the last bit shows. A failed run
/// is compared by its message.
fn rendered(result: Result<Vec<RecordBatch>>) -> Vec<String> {
    let batches = match result {
        Ok(batches) => batches,
        Err(error) => return vec![format!("error: {error}")],
    };
    let options = arrow::util::display::FormatOptions::default().with_null("null");
    let mut rows = Vec::new();
    for batch in &batches {
        let formatters: Vec<_> = batch
            .columns()
            .iter()
            .map(|c| arrow::util::display::ArrayFormatter::try_new(c.as_ref(), &options).unwrap())
            .collect();
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = formatters
                .iter()
                .map(|f| f.value(row).to_string())
                .collect();
            rows.push(cells.join(" | "));
        }
    }
    rows
}

/// Every registered kernel with its probe arguments (which name nodes `a`,
/// `b`, `c` and the fixture's property columns), on the outgoing and the
/// undirected projection, unweighted and weighted by `w`. A combination
/// Grust's validator refuses is left out; one that fails when run is kept,
/// and compared by its error.
fn calls() -> Vec<(String, &'static str, ValidatedArguments)> {
    let mut out = Vec::new();
    for algorithm in algorithm_names() {
        for orientation in ["outgoing", "undirected"] {
            for weighted in [false, true] {
                let mut options = serde_json::Map::new();
                options.insert("orientation".into(), serde_json::json!(orientation));
                if weighted {
                    options.insert("weightProperty".into(), serde_json::json!("w"));
                }
                if let Ok(args) = probe_args_with(algorithm, options) {
                    let weight = if weighted { "weighted" } else { "unweighted" };
                    out.push((
                        format!("{algorithm} {orientation} {weight}"),
                        algorithm,
                        args,
                    ));
                }
            }
        }
    }
    out
}

/// Stage the fixture under `name` in the arrival order drawn from `seed`: its
/// nodes (unless `edges_only`) in one write, and its edges in one write per
/// batch when `appends`, the first replacing and the rest appending.
fn stage_fixture(
    name: &str,
    fixture: &Fixture,
    seed: u64,
    edges_only: bool,
    appends: bool,
    order: StageOrder,
) {
    let mapping = ColumnMapping::default();
    if edges_only {
        Registry::stage(name, Part::Nodes, &[], &mapping, true, order).unwrap();
    } else {
        let (rows, sizes) = arrival(fixture.nodes.len(), seed);
        let batches = fixture.node_batches(&rows, &sizes);
        Registry::stage(name, Part::Nodes, &batches, &mapping, true, order).unwrap();
    }
    let (rows, sizes) = arrival(fixture.edges.len(), seed.wrapping_mul(31));
    let batches = fixture.edge_batches(&rows, &sizes);
    if appends {
        for (i, batch) in batches.iter().enumerate() {
            Registry::stage(
                name,
                Part::Edges,
                std::slice::from_ref(batch),
                &mapping,
                i == 0,
                order,
            )
            .unwrap();
        }
    } else {
        Registry::stage(name, Part::Edges, &batches, &mapping, true, order).unwrap();
    }
}

type Calls = [(String, &'static str, ValidatedArguments)];

/// Each call's rendered result on `name`.
fn results(name: &str, calls: &Calls) -> Vec<Vec<String>> {
    calls
        .iter()
        .map(|(_, algorithm, args)| rendered(run(algorithm, name, args)))
        .collect()
}

/// Stage the fixture under a fresh name, collect every call's result, drop it.
fn staged_results(
    calls: &Calls,
    seed: u64,
    edges_only: bool,
    appends: bool,
    order: StageOrder,
) -> Vec<Vec<String>> {
    let name = format!("order-{order:?}-{seed}-{edges_only}-{appends}");
    stage_fixture(&name, &fixture(), seed, edges_only, appends, order);
    let out = results(&name, calls);
    assert!(Registry::drop(&name).unwrap());
    out
}

/// The kernels with a call whose values differ between two stagings, compared
/// as multisets of rows: a different row order alone does not count, only
/// different values.
fn differing(calls: &Calls, left: &[Vec<String>], right: &[Vec<String>]) -> BTreeSet<&'static str> {
    let sorted = |rows: &Vec<String>| {
        let mut rows = rows.clone();
        rows.sort();
        rows
    };
    calls
        .iter()
        .zip(left.iter().zip(right))
        .filter(|(_, (l, r))| sorted(l) != sorted(r))
        .map(|((_, algorithm, _), _)| *algorithm)
        .collect()
}

/// The modularity `algorithm` (Leiden or Louvain) reaches on the fixture staged
/// in the arrival order drawn from `seed`: a measure of the partition found,
/// so a difference in it is a different answer, not a relabelling.
fn modularity_reached(
    algorithm: &str,
    seed: u64,
    edges_only: bool,
    appends: bool,
    order: StageOrder,
) -> f64 {
    let name = format!("modularity-{algorithm}-{order:?}-{seed}-{edges_only}-{appends}");
    stage_fixture(&name, &fixture(), seed, edges_only, appends, order);
    let mut options = serde_json::Map::new();
    options.insert("orientation".into(), serde_json::json!("undirected"));
    let batches = run(algorithm, &name, &validate(algorithm, &options).unwrap()).unwrap();
    assert!(Registry::drop(&name).unwrap());
    batches[0]
        .column_by_name("modularity")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0)
}

/// The same graph staged in different orders gives identical results, row for
/// row and bit for bit, for every kernel Grust registers, under canonical
/// order; and as staged, it does not.
///
/// Each staging shuffles the fixture's node and edge rows and splits them
/// into batches of random sizes. Every kernel runs with its probe arguments,
/// outgoing and undirected, unweighted and weighted: 164 calls on the Grust
/// this was written against, each compared in full, including the edge
/// ordinals path kernels report and the text of any error. It runs twice:
/// with the nodes staged, and with edges alone, where the nodes are derived
/// from the edges, as the Citi Bike example stages its trips.
///
/// The second half is what shows the sort is doing the work. Staged as they
/// arrived, the same three orders change Leiden's, Louvain's and label
/// propagation's answers, and Leiden and Louvain reach a different modularity
/// in four orders (0.5026 in some, 0.5248 in others): a different partition,
/// not the same one relabelled. Canonically staged, the same four orders all
/// reach one. Without the sort, the first half fails the same way.
#[test]
fn canonical_order_makes_every_kernel_independent_of_arrival_order() {
    let calls = calls();
    assert!(calls.len() >= 100, "{} calls", calls.len());
    for edges_only in [false, true] {
        let canonical: Vec<_> = [1, 2, 3]
            .map(|seed| staged_results(&calls, seed, edges_only, false, StageOrder::Canonical))
            .into();
        let succeeded = canonical[0]
            .iter()
            .filter(|rows| !rows.iter().any(|row| row.starts_with("error:")))
            .count();
        assert!(
            succeeded * 2 > calls.len(),
            "only {succeeded} of {} calls ran; the comparison would be of errors",
            calls.len()
        );
        for (i, (label, _, _)) in calls.iter().enumerate() {
            for other in &canonical[1..] {
                assert_eq!(
                    canonical[0][i], other[i],
                    "{label} (edges only: {edges_only})"
                );
            }
        }

        let as_staged: Vec<_> = [1, 2, 3]
            .map(|seed| staged_results(&calls, seed, edges_only, false, StageOrder::AsStaged))
            .into();
        let mut sensitive = BTreeSet::new();
        for other in &as_staged[1..] {
            sensitive.extend(differing(&calls, &as_staged[0], other));
        }
        for kernel in ["leiden", "louvain", "labelPropagation"] {
            assert!(
                sensitive.contains(kernel),
                "{kernel} gave the same values in every arrival order as staged, so this \
                 fixture does not show what the sort is for (edges only: {edges_only}); \
                 sensitive: {sensitive:?}"
            );
        }
    }
    for algorithm in ["leiden", "louvain"] {
        for edges_only in [false, true] {
            let reached = |order| -> BTreeSet<u64> {
                [1, 2, 3, 4]
                    .map(|seed| {
                        modularity_reached(algorithm, seed, edges_only, false, order).to_bits()
                    })
                    .into()
            };
            let context = format!("{algorithm} (edges only: {edges_only})");
            assert_eq!(reached(StageOrder::Canonical).len(), 1, "{context}");
            assert!(reached(StageOrder::AsStaged).len() > 1, "{context}");
        }
    }
}

/// Canonical order covers the whole part, not each write: the edges staged in
/// one write, or appended a batch at a time in two different orders, give the
/// same results for every call. As staged, the appends give different
/// answers. A canonical append after an as-staged write sorts the rows that
/// write left too.
#[test]
fn canonical_order_covers_the_whole_part_across_appends() {
    let calls = calls();
    let whole = staged_results(&calls, 1, true, false, StageOrder::Canonical);
    for seed in [4, 5] {
        let appended = staged_results(&calls, seed, true, true, StageOrder::Canonical);
        for (i, (label, _, _)) in calls.iter().enumerate() {
            assert_eq!(whole[i], appended[i], "{label} (appends from seed {seed})");
        }
    }
    let left = staged_results(&calls, 4, true, true, StageOrder::AsStaged);
    let right = staged_results(&calls, 5, true, true, StageOrder::AsStaged);
    let sensitive = differing(&calls, &left, &right);
    assert!(
        sensitive.contains("leiden") && sensitive.contains("louvain"),
        "{sensitive:?}"
    );
    let reached: BTreeSet<u64> = [4, 5, 6]
        .map(|seed| modularity_reached("leiden", seed, true, true, StageOrder::AsStaged).to_bits())
        .into();
    assert!(
        reached.len() > 1,
        "as staged, appends reached one modularity"
    );

    // An as-staged write, then a canonical append: the whole part is sorted.
    let name = "order-mixed";
    let fixture = fixture();
    let mapping = ColumnMapping::default();
    let (rows, sizes) = arrival(fixture.edges.len(), 9);
    let batches = fixture.edge_batches(&rows, &sizes);
    let (first, rest) = batches.split_at(batches.len() / 2);
    Registry::stage(name, Part::Nodes, &[], &mapping, true, StageOrder::AsStaged).unwrap();
    Registry::stage(
        name,
        Part::Edges,
        first,
        &mapping,
        true,
        StageOrder::AsStaged,
    )
    .unwrap();
    Registry::stage(
        name,
        Part::Edges,
        rest,
        &mapping,
        false,
        StageOrder::Canonical,
    )
    .unwrap();
    let mixed = results(name, &calls);
    assert!(Registry::drop(name).unwrap());
    for (i, (label, _, _)) in calls.iter().enumerate() {
        assert_eq!(
            whole[i], mixed[i],
            "{label} (as staged, then a canonical append)"
        );
    }
}

fn staged(name: &str, part: Part) -> Vec<RecordBatch> {
    let entry = Registry::entry(name, false).unwrap().unwrap();
    let e = entry.read().unwrap();
    match part {
        Part::Nodes => e.nodes.clone(),
        Part::Edges => e.edges.clone(),
    }
}

fn strings_of(batches: &[RecordBatch], column: &str) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            let c = b
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            (0..c.len()).map(move |i| c.value(i).to_string())
        })
        .collect()
}

/// Ids are Utf8 once staged, so canonical order is text order: `"10"` sorts
/// before `"9"`, for staged nodes and for nodes derived from edges alike.
/// Parallel edges with no `edge_id` are ordered by their weight.
#[test]
fn canonical_order_is_text_order_on_ids_and_breaks_ties_by_the_other_columns() {
    let name = "text-order";
    let mapping = ColumnMapping::default();
    let nodes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![9, 10, 2]))],
    )
    .unwrap();
    Registry::stage(
        name,
        Part::Nodes,
        &[nodes],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    assert_eq!(
        strings_of(&staged(name, Part::Nodes), "node_id"),
        ["10", "2", "9"]
    );
    let batch = edges(
        &["9", "9", "10", "9"],
        &["2", "2", "9", "10"],
        Some(&[3.0, 1.0, 5.0, 2.0]),
    );
    Registry::stage(
        name,
        Part::Edges,
        std::slice::from_ref(&batch),
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    let sorted = staged(name, Part::Edges);
    assert_eq!(strings_of(&sorted, "source"), ["10", "9", "9", "9"]);
    assert_eq!(strings_of(&sorted, "target"), ["9", "10", "2", "2"]);
    let w = sorted[0]
        .column_by_name("property.w")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(w.values().to_vec(), [5.0, 2.0, 1.0, 3.0]);

    Registry::stage(
        name,
        Part::Nodes,
        &[],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    assert_eq!(
        strings_of(&Registry::node_batches(name).unwrap(), "node_id"),
        ["10", "2", "9"]
    );
    // As staged, rows keep their arrival order, and derived nodes the order
    // their ids first appear.
    Registry::stage(
        name,
        Part::Edges,
        &[batch],
        &mapping,
        true,
        StageOrder::AsStaged,
    )
    .unwrap();
    assert_eq!(
        strings_of(&staged(name, Part::Edges), "source"),
        ["9", "9", "10", "9"]
    );
    assert_eq!(
        strings_of(&Registry::node_batches(name).unwrap(), "node_id"),
        ["9", "2", "10"]
    );
    assert!(Registry::drop(name).unwrap());
}

/// Appends of differently shaped rows are sorted as one part: a batch without
/// the weight column is filled as Grust reads a missing column (absent), so
/// the two appends in either order stage the same rows. A column that is two
/// types in two appends has no single sorted form and is refused, naming the
/// opt-out, which accepts it.
#[test]
fn appends_of_different_shapes_are_sorted_as_one_part() {
    let mapping = ColumnMapping::default();
    let weighted = edges(&["b", "a"], &["c", "b"], Some(&[2.0, 1.0]));
    let bare = edges(&["a", "c"], &["c", "a"], None);
    let mut seen = Vec::new();
    for (name, batches) in [
        ("shapes-1", [&weighted, &bare]),
        ("shapes-2", [&bare, &weighted]),
    ] {
        Registry::stage(
            name,
            Part::Edges,
            &[batches[0].clone()],
            &mapping,
            true,
            StageOrder::Canonical,
        )
        .unwrap();
        Registry::stage(
            name,
            Part::Edges,
            &[batches[1].clone()],
            &mapping,
            false,
            StageOrder::Canonical,
        )
        .unwrap();
        let mut options = no_options();
        options.insert("weightProperty".into(), serde_json::json!("w"));
        options.insert("defaultWeight".into(), serde_json::json!(7.0));
        let degree = rendered(run("degree", name, &validate("degree", &options).unwrap()));
        seen.push((staged(name, Part::Edges), degree));
        assert!(Registry::drop(name).unwrap());
    }
    assert_eq!(seen[0], seen[1]);
    let present = seen[0].0[0]
        .column_by_name("present.w")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect::<Vec<_>>();
    // a→b (w 1), a→c (none), b→c (w 2), c→a (none).
    assert_eq!(present, [Some(true), Some(false), Some(true), Some(false)]);

    let name = "shapes-conflict";
    let integer = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("src", DataType::Utf8, false),
            Field::new("dst", DataType::Utf8, false),
            Field::new("w", DataType::Int32, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(StringArray::from(vec!["b"])),
            Arc::new(Int32Array::from(vec![1])),
        ],
    )
    .unwrap();
    Registry::stage(
        name,
        Part::Edges,
        &[weighted],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    let error = Registry::stage(
        name,
        Part::Edges,
        std::slice::from_ref(&integer),
        &mapping,
        false,
        StageOrder::Canonical,
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("property.w") && error.contains("asStaged"),
        "{error}"
    );
    // The refused write left the rows staged before it.
    assert_eq!(strings_of(&staged(name, Part::Edges), "source"), ["a", "b"]);
    let staged_rows = Registry::stage(
        name,
        Part::Edges,
        &[integer],
        &mapping,
        false,
        StageOrder::AsStaged,
    )
    .unwrap();
    assert_eq!(staged_rows, 3);
    assert!(Registry::drop(name).unwrap());
}

#[test]
fn the_order_option_parses_in_either_case() {
    assert_eq!(
        StageOrder::parse("canonical").unwrap(),
        StageOrder::Canonical
    );
    assert_eq!(StageOrder::parse("asStaged").unwrap(), StageOrder::AsStaged);
    assert_eq!(
        StageOrder::parse(" ASSTAGED ").unwrap(),
        StageOrder::AsStaged
    );
    assert_eq!(StageOrder::default(), StageOrder::Canonical);
    let error = StageOrder::parse("sorted").unwrap_err().to_string();
    assert!(
        error.contains(ORDER_OPTION) && error.contains("asStaged"),
        "{error}"
    );
}
