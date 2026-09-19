### session-factory-hook

#### Commit Information

| Commit | Revision | Branch |
| :--- | :--- | :--- |
| **After** | `991d50c` | `session-factory-hook-branch` |
| **Before** | `20f4de4` | `session-factory-hook-base` |

#### Test Summary

| Suite | Commit | Failed | Passed | Skipped | Warnings | Time (s) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `doctest-catalog` | **After** | 10 | 14 | 1 | 138 | 5.05 |
|  | **Before** | 10 | 14 | 1 | 138 | 5.07 |
| `doctest-column` | **After** |  | 33 |  | 140 | 5.05 |
|  | **Before** |  | 33 |  | 140 | 4.91 |
| `doctest-dataframe` | **After** | 14 | 83 | 10 | 135 | 5.50 |
|  | **Before** | 14 | 83 | 10 | 135 | 6.11 |
| `doctest-functions` | **After** | 17 | 383 | 9 | 171 | 8.64 |
|  | **Before** | 17 | 383 | 9 | 171 | 9.12 |
| `test-connect` | **After** | 496 | 506 | 170 | 3947 | 49.86 |
|  | **Before** | 496 | 506 | 170 | 3951 | 53.26 |


#### Test Details

<details>
<summary>Error Counts</summary>

```text
          537 Total
(+1)       86 Total Unique
-------- ---- ----------------------------------------------------------------------------------------------------------
          394 IllegalArgumentException: invalid argument: Python version used to compile the UDF (3.11) does not match the Python version at runtime (3.13.5 (main, Aug 10 2026, 12:06:59) [GCC 14.2.0])
           11 PySparkAssertionError: [DIFFERENT_PANDAS_DATAFRAME] DataFrames are not almost equal:
           10 handle add artifacts
            9 DocTestFailure
            6 UnsupportedOperationException: PlanNode::CacheTable
            4 UnsupportedOperationException: function: input_file_name
            3 AnalysisException: Temporary View not found: v
            3 ValueError: Converting to Python dictionary is not supported when duplicate field names are present
            2 AnalysisException: Could not find config namespace "spark"
            2 AnalysisException: Internal error: Function 'approx_percentile_cont' failed to match any signature, errors: Error during planning: Function 'approx_percentile_cont' expects 2 arguments but received 3,...
            2 AnalysisException: No data source found for: orc
            2 AnalysisException: not supported: function exists
            2 AssertionError: "Exception thrown when converting pandas.Series" does not match "invalid argument: Python version used to compile the UDF (3.11) does not match the Python version at runtime (3.13.5 (m...
            2 AssertionError: "Invalid.*type" does not match "invalid argument: Python version used to compile the UDF (3.11) does not match the Python version at runtime (3.13.5 (main, Aug 10 2026, 12:06:59) [GCC ...
            2 AssertionError: 0 not greater than or equal to 1
(-1)        2 AssertionError: False is not true
            2 IllegalArgumentException: invalid argument: found FUNCTION at 7:15 expected 'DATABASE', 'SCHEMA', 'NAMESPACE', 'OR', 'TEMP', 'TEMPORARY', 'EXTERNAL', 'TABLE', 'GLOBAL', or 'VIEW'
            2 IllegalArgumentException: invalid argument: found RESET at 0:5 expected something else, ';', statement, or end of input
            2 PySparkNotImplementedError: [NOT_IMPLEMENTED] rdd() is not implemented.
            2 UnsupportedOperationException: approx quantile
            2 UnsupportedOperationException: collect metrics
            2 UnsupportedOperationException: freq items
            2 UnsupportedOperationException: function: session_window
            2 UnsupportedOperationException: handle analyze same semantics
            2 UnsupportedOperationException: user defined data type should only exist in a field
            2 UnsupportedOperationException: with watermark
            2 handle artifact statuses
            1 AnalysisException: Table already exists: tbl1
(+1)        1 AnalysisException: Temporary View already exists: view_1
            1 AnalysisException: Temporary View not found: tab2
            1 AnalysisException: UNION queries have different number of columns: left has 2 columns whereas right has 3 columns
            1 AnalysisException: not supported: qualified function name
            1 AssertionError
(+1)        1 AssertionError: "Database 'memory:c333115a-cda4-43c2-96b8-3ddf86a5979d' dropped." does not match "No data source found for: jdbc. The JDBC data source is provided by pysail and must be registered befo...
(+1)        1 AssertionError: "Database 'memory:f8a1b244-bb23-449b-be77-f24340b199ac' dropped." does not match "No data source found for: jdbc. The JDBC data source is provided by pysail and must be registered befo...
            1 AssertionError: "My error" does not match "invalid argument: Python version used to compile the UDF (3.11) does not match the Python version at runtime (3.13.5 (main, Aug 10 2026, 12:06:59) [GCC 14.2....
            1 AssertionError: "PickleException" does not match "invalid argument: Python version used to compile the UDF (3.11) does not match the Python version at runtime (3.13.5 (main, Aug 10 2026, 12:06:59) [GC...
            1 AssertionError: "Result vector from pandas_udf was not the required length" does not match "invalid argument: Python version used to compile the UDF (3.11) does not match the Python version at runtime...
            1 AssertionError: "Return.*type.*Series" does not match "invalid argument: Python version used to compile the UDF (3.11) does not match the Python version at runtime (3.13.5 (main, Aug 10 2026, 12:06:59...
            1 AssertionError: "division( or modulo)? by zero" does not match "invalid argument: Python version used to compile the UDF (3.11) does not match the Python version at runtime (3.13.5 (main, Aug 10 2026,...
            1 AssertionError: "reached finally block" does not match "invalid argument: Python version used to compile the UDF (3.11) does not match the Python version at runtime (3.13.5 (main, Aug 10 2026, 12:06:5...
            1 AssertionError: 1 != 0
            1 AssertionError: 4 != 10
            1 AssertionError: AnalysisException not raised
            1 AssertionError: AnalysisException not raised by <lambda>
            1 AssertionError: Exception not raised by <lambda>
            1 AssertionError: Lists differ: [Row([178 chars]on='<<'), Row(function='<='), Row(function='<=[14985 chars]'~')] != [Row([178 chars]on='<='), Row(function='<=>'), Row(function='<[11284 chars]'~')]
            1 AssertionError: Lists differ: [Row(id=90, name='90'), Row(id=91, name='91'), Ro[176 chars]99')] != [Row(id=15, name='15'), Row(id=16, name='16'), Ro[176 chars]24')]
            1 AssertionError: Lists differ: [Row(ln(id)=0.0, ln(id)=0.0, struct(id, name)=Row(id=[1232 chars]0'))] != [Row(ln(id)=4.31748811353631, ln(id)=4.31748811353631[1312 chars]4'))]
            1 AssertionError: Lists differ: [Row(name='Andy', age=30), Row(name='Andy', ag[374 chars]one)] != [Row(age=19, name='Justin'), Row(age=19, name=[374 chars]el')]
            1 AssertionError: Lists differ: [Row(name='Andy', age=30), Row(name='Justin', [34 chars]one)] != [Row(_corrupt_record=' "age":19}\n', name=None[104 chars]el')]
            1 AssertionError: Row(point='[1.0, 2.0]', pypoint='[3.0, 4.0]') != Row(point='(1.0, 2.0)', pypoint='[3.0, 4.0]')
            1 AssertionError: StorageLevel(False, True, True, False, 1) != StorageLevel(False, False, False, False, 1)
            1 AssertionError: Struc[15 chars]eld('a', NullType(), True), StructField('b', L[51 chars]ue)]) != Struc[15 chars]eld('b', LongType(), True), StructField('c', S[15 chars]ue)])
            1 AssertionError: Struc[40 chars]ue), StructField('val', ArrayType(DoubleType(), False), True)]) != Struc[40 chars]ue), StructField('val', PythonOnlyUDT(), True)])
            1 AssertionError: StructType([StructField('interval', DayTimeIntervalType(0, 2), False)]) != StructType([StructField('interval', DayTimeIntervalType(0, 3), False)])
            1 AssertionError: YearMonthIntervalType(0, 1) != YearMonthIntervalType(0, 0)
            1 AssertionError: [1.0, 2.0] != ExamplePoint(1.0,2.0)
            1 AssertionError: dtype('<M8[us]') != 'datetime64[ns]'
            1 IllegalArgumentException: invalid argument: field not found in input schema: col1
            1 IllegalArgumentException: invalid argument: table does not exist: ObjectName([Identifier("test_table")])
            1 PySparkNotImplementedError: [NOT_IMPLEMENTED] toJSON() is not implemented.
            1 SparkRuntimeException: Cast error: Cannot cast string '1997/02/28 10:30:00' to value of Date32 type
            1 SparkRuntimeException: Invalid argument error: 83.140 is too large to store in a Decimal128 of precision 4. Max is 9.999
            1 SparkRuntimeException: Json error: Not valid JSON: EOF while parsing a list at line 1 column 1
            1 SparkRuntimeException: Json error: Not valid JSON: expected value at line 1 column 2
            1 SparkRuntimeException: Parser error: Error while parsing value '0
            1 SparkRuntimeException: This feature is not implemented: Unsupported CAST from Map("entries": non-null Struct("key": non-null Int32, "value": non-null Int32), unsorted) to Null
            1 UnsupportedOperationException: PlanNode::ClearCache
            1 UnsupportedOperationException: PlanNode::IsCached
            1 UnsupportedOperationException: PlanNode::RecoverPartitions
            1 UnsupportedOperationException: Support for 'approx_distinct' for data type Float64 is not implemented
            1 UnsupportedOperationException: apply in pandas with state
            1 UnsupportedOperationException: bucketing for writing listing data source
            1 UnsupportedOperationException: deduplicate within watermark
            1 UnsupportedOperationException: function: input_file_block_length
            1 UnsupportedOperationException: function: input_file_block_start
            1 UnsupportedOperationException: function: map_filter
            1 UnsupportedOperationException: function: map_zip_with
            1 UnsupportedOperationException: function: transform_keys
            1 UnsupportedOperationException: function: transform_values
            1 UnsupportedOperationException: function: zip_with
            1 UnsupportedOperationException: handle analyze semantic hash
            1 UnsupportedOperationException: unknown function: distributed_sequence_id
            1 ValueError: The column label 'id' is not unique.
            1 ValueError: The column label 'struct' is not unique.
(-1)        0 AssertionError: "Database 'memory:0b8a2c97-bb5b-4455-981a-07e3e5f08b10' dropped." does not match "No data source found for: jdbc. The JDBC data source is provided by pysail and must be registered befo...
(-1)        0 AssertionError: "Database 'memory:3c0db28e-23aa-4a1d-bd86-2b1f7a048020' dropped." does not match "No data source found for: jdbc. The JDBC data source is provided by pysail and must be registered befo...
```

</details>

<details>
<summary>Passed Tests Diff</summary>

(empty)

</details>
<details>
<summary>Failed Tests</summary>

```text
pyspark/sql/catalog.py::pyspark.sql.catalog.Catalog.cacheTable
pyspark/sql/catalog.py::pyspark.sql.catalog.Catalog.clearCache
pyspark/sql/catalog.py::pyspark.sql.catalog.Catalog.createTable
pyspark/sql/catalog.py::pyspark.sql.catalog.Catalog.functionExists
pyspark/sql/catalog.py::pyspark.sql.catalog.Catalog.getFunction
pyspark/sql/catalog.py::pyspark.sql.catalog.Catalog.isCached
pyspark/sql/catalog.py::pyspark.sql.catalog.Catalog.recoverPartitions
pyspark/sql/catalog.py::pyspark.sql.catalog.Catalog.refreshByPath
pyspark/sql/catalog.py::pyspark.sql.catalog.Catalog.refreshTable
pyspark/sql/catalog.py::pyspark.sql.catalog.Catalog.uncacheTable
pyspark/sql/dataframe.py::pyspark.sql.dataframe.DataFrame.colRegex
pyspark/sql/dataframe.py::pyspark.sql.dataframe.DataFrame.dropDuplicatesWithinWatermark
pyspark/sql/dataframe.py::pyspark.sql.dataframe.DataFrame.explain
pyspark/sql/dataframe.py::pyspark.sql.dataframe.DataFrame.hint
pyspark/sql/dataframe.py::pyspark.sql.dataframe.DataFrame.observe
pyspark/sql/dataframe.py::pyspark.sql.dataframe.DataFrame.randomSplit
pyspark/sql/dataframe.py::pyspark.sql.dataframe.DataFrame.repartition
pyspark/sql/dataframe.py::pyspark.sql.dataframe.DataFrame.repartitionByRange
pyspark/sql/dataframe.py::pyspark.sql.dataframe.DataFrame.sameSemantics
pyspark/sql/dataframe.py::pyspark.sql.dataframe.DataFrame.sampleBy
pyspark/sql/dataframe.py::pyspark.sql.dataframe.DataFrame.storageLevel
pyspark/sql/dataframe.py::pyspark.sql.dataframe.DataFrame.toJSON
pyspark/sql/dataframe.py::pyspark.sql.dataframe.DataFrame.withWatermark
pyspark/sql/dataframe.py::pyspark.sql.dataframe.DataFrameStatFunctions.sampleBy
pyspark/sql/functions.py::pyspark.sql.functions.approx_percentile
pyspark/sql/functions.py::pyspark.sql.functions.call_function
pyspark/sql/functions.py::pyspark.sql.functions.call_udf
pyspark/sql/functions.py::pyspark.sql.functions.input_file_block_length
pyspark/sql/functions.py::pyspark.sql.functions.input_file_block_start
pyspark/sql/functions.py::pyspark.sql.functions.input_file_name
pyspark/sql/functions.py::pyspark.sql.functions.map_entries
pyspark/sql/functions.py::pyspark.sql.functions.map_filter
pyspark/sql/functions.py::pyspark.sql.functions.map_zip_with
pyspark/sql/functions.py::pyspark.sql.functions.percentile_approx
pyspark/sql/functions.py::pyspark.sql.functions.regexp_instr
pyspark/sql/functions.py::pyspark.sql.functions.session_window
pyspark/sql/functions.py::pyspark.sql.functions.transform_keys
pyspark/sql/functions.py::pyspark.sql.functions.transform_values
pyspark/sql/functions.py::pyspark.sql.functions.udf
pyspark/sql/functions.py::pyspark.sql.functions.udtf
pyspark/sql/functions.py::pyspark.sql.functions.zip_with
pyspark/sql/tests/connect/client/test_artifact.py::ArtifactTests::test_add_archive
pyspark/sql/tests/connect/client/test_artifact.py::ArtifactTests::test_add_file
pyspark/sql/tests/connect/client/test_artifact.py::ArtifactTests::test_add_pyfile
pyspark/sql/tests/connect/client/test_artifact.py::ArtifactTests::test_add_zipped_package
pyspark/sql/tests/connect/client/test_artifact.py::ArtifactTests::test_basic_requests
pyspark/sql/tests/connect/client/test_artifact.py::ArtifactTests::test_cache_artifact
pyspark/sql/tests/connect/client/test_artifact.py::ArtifactTests::test_copy_from_local_to_fs
pyspark/sql/tests/connect/client/test_artifact.py::LocalClusterArtifactTests::test_add_archive
pyspark/sql/tests/connect/client/test_artifact.py::LocalClusterArtifactTests::test_add_file
pyspark/sql/tests/connect/client/test_artifact.py::LocalClusterArtifactTests::test_add_pyfile
pyspark/sql/tests/connect/client/test_artifact.py::LocalClusterArtifactTests::test_add_zipped_package
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_collect
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_create_global_temp_view
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_deduplicate_within_watermark_in_batch
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_describe
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_hint
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_join_hint
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_json
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_multi_paths
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_observe
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_orc
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_random_split
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_same_semantics
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_schema
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_semantic_hash
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_simple_udt_from_read
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_sql_with_command
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_stat_approx_quantile
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_stat_freq_items
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_stat_sample_by
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_streaming_local_relation
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_tail
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_to
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectBasicTests::test_write_operations
pyspark/sql/tests/connect/test_connect_basic.py::SparkConnectSessionTests::test_error_stack_trace
pyspark/sql/tests/connect/test_connect_column.py::SparkConnectColumnTests::test_column_arithmetic_ops
pyspark/sql/tests/connect/test_connect_column.py::SparkConnectColumnTests::test_decimal
pyspark/sql/tests/connect/test_connect_column.py::SparkConnectColumnTests::test_distributed_sequence_id
pyspark/sql/tests/connect/test_connect_function.py::SparkConnectFunctionTests::test_aggregation_functions
pyspark/sql/tests/connect/test_connect_function.py::SparkConnectFunctionTests::test_collection_functions
pyspark/sql/tests/connect/test_connect_function.py::SparkConnectFunctionTests::test_date_ts_functions
pyspark/sql/tests/connect/test_connect_function.py::SparkConnectFunctionTests::test_generator_functions
pyspark/sql/tests/connect/test_connect_function.py::SparkConnectFunctionTests::test_lambda_functions
pyspark/sql/tests/connect/test_connect_function.py::SparkConnectFunctionTests::test_math_functions
pyspark/sql/tests/connect/test_connect_function.py::SparkConnectFunctionTests::test_normal_functions
pyspark/sql/tests/connect/test_connect_function.py::SparkConnectFunctionTests::test_string_functions_multi_args
pyspark/sql/tests/connect/test_connect_function.py::SparkConnectFunctionTests::test_time_window_functions
pyspark/sql/tests/connect/test_connect_function.py::SparkConnectFunctionTests::test_udf
pyspark/sql/tests/connect/test_connect_function.py::SparkConnectFunctionTests::test_udtf
pyspark/sql/tests/connect/test_connect_function.py::SparkConnectFunctionTests::test_window_functions
pyspark/sql/tests/connect/test_parity_arrow.py::ArrowParityTests::test_createDataFrame_duplicate_field_names
pyspark/sql/tests/connect/test_parity_arrow.py::ArrowParityTests::test_pandas_self_destruct
pyspark/sql/tests/connect/test_parity_arrow.py::ArrowParityTests::test_propagates_spark_exception
pyspark/sql/tests/connect/test_parity_arrow.py::ArrowParityTests::test_toPandas_duplicate_field_names
pyspark/sql/tests/connect/test_parity_arrow_map.py::ArrowMapParityTests::test_chain_map_in_arrow
pyspark/sql/tests/connect/test_parity_arrow_map.py::ArrowMapParityTests::test_different_output_length
pyspark/sql/tests/connect/test_parity_arrow_map.py::ArrowMapParityTests::test_empty_iterator
pyspark/sql/tests/connect/test_parity_arrow_map.py::ArrowMapParityTests::test_empty_rows
pyspark/sql/tests/connect/test_parity_arrow_map.py::ArrowMapParityTests::test_large_variable_width_types
pyspark/sql/tests/connect/test_parity_arrow_map.py::ArrowMapParityTests::test_map_in_arrow
pyspark/sql/tests/connect/test_parity_arrow_map.py::ArrowMapParityTests::test_multiple_columns
pyspark/sql/tests/connect/test_parity_arrow_map.py::ArrowMapParityTests::test_other_than_recordbatch_iter
pyspark/sql/tests/connect/test_parity_arrow_map.py::ArrowMapParityTests::test_self_join
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_chained_udf
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_complex_input_types
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_complex_return_types
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_file_dsv2_with_udf_filter
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_multiple_udfs
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_nested_array
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_nested_array_input
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_nested_map
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_nested_struct
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_nondeterministic_udf
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_nondeterministic_udf2
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_nondeterministic_udf_in_aggregate
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_nonparam_udf_with_aggregate
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_raise_stop_iteration
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_register
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_single_udf_with_repeated_argument
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_type_coercion_string_to_numeric
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf2
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf3
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_and_common_filter_in_join_condition
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_as_join_condition
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_daytime_interval
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_globals_not_overwritten
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_in_filter_on_top_of_join
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_in_filter_on_top_of_outer_join
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_in_generate
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_in_join_condition
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_in_left_outer_join_condition
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_in_subquery
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_input_serialization_valuecompare_disabled
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_not_supported_in_join_condition
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_registration_return_type_none
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_registration_returns_udf
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_with_256_args
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_with_aggregate_function
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_with_array_type
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_with_callable
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_with_column_vector
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_with_decorator
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_with_filter_function
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_with_input_file_name
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_with_order_by_and_limit
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_with_partial_function
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_with_rand
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_with_string_return_type
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_without_arguments
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_use_arrow
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_chained_udf
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_complex_return_types
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_file_dsv2_with_udf_filter
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_multiple_udfs
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_nested_array
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_nested_map
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_nested_struct
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_nondeterministic_udf
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_nondeterministic_udf2
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_nondeterministic_udf_in_aggregate
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_nonparam_udf_with_aggregate
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_raise_stop_iteration
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_single_udf_with_repeated_argument
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf2
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf3
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_and_common_filter_in_join_condition
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_as_join_condition
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_daytime_interval
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_globals_not_overwritten
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_in_filter_on_top_of_join
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_in_filter_on_top_of_outer_join
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_in_generate
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_in_join_condition
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_in_left_outer_join_condition
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_in_subquery
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_input_serialization_valuecompare_disabled
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_not_supported_in_join_condition
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_registration_return_type_none
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_registration_returns_udf
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_with_256_args
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_with_aggregate_function
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_with_array_type
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_with_callable
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_with_column_vector
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_with_decorator
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_with_filter_function
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_with_input_file_name
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_with_order_by_and_limit
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_with_partial_function
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_with_rand
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_with_string_return_type
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_without_arguments
pyspark/sql/tests/connect/test_parity_catalog.py::CatalogParityTests::test_function_exists
pyspark/sql/tests/connect/test_parity_catalog.py::CatalogParityTests::test_get_function
pyspark/sql/tests/connect/test_parity_catalog.py::CatalogParityTests::test_list_functions
pyspark/sql/tests/connect/test_parity_catalog.py::CatalogParityTests::test_refresh_table
pyspark/sql/tests/connect/test_parity_catalog.py::CatalogParityTests::test_table_cache
pyspark/sql/tests/connect/test_parity_dataframe.py::DataFrameParityTests::test_cache_dataframe
pyspark/sql/tests/connect/test_parity_dataframe.py::DataFrameParityTests::test_cache_table
pyspark/sql/tests/connect/test_parity_dataframe.py::DataFrameParityTests::test_duplicate_field_names
pyspark/sql/tests/connect/test_parity_dataframe.py::DataFrameParityTests::test_extended_hint_types
pyspark/sql/tests/connect/test_parity_dataframe.py::DataFrameParityTests::test_freqItems
pyspark/sql/tests/connect/test_parity_dataframe.py::DataFrameParityTests::test_generic_hints
pyspark/sql/tests/connect/test_parity_dataframe.py::DataFrameParityTests::test_input_files
pyspark/sql/tests/connect/test_parity_dataframe.py::DataFrameParityTests::test_to
pyspark/sql/tests/connect/test_parity_dataframe.py::DataFrameParityTests::test_to_pandas
pyspark/sql/tests/connect/test_parity_datasources.py::DataSourcesParityTests::test_checking_csv_header
pyspark/sql/tests/connect/test_parity_datasources.py::DataSourcesParityTests::test_encoding_json
pyspark/sql/tests/connect/test_parity_datasources.py::DataSourcesParityTests::test_ignore_column_of_all_nulls
pyspark/sql/tests/connect/test_parity_datasources.py::DataSourcesParityTests::test_jdbc
pyspark/sql/tests/connect/test_parity_datasources.py::DataSourcesParityTests::test_jdbc_format
pyspark/sql/tests/connect/test_parity_datasources.py::DataSourcesParityTests::test_linesep_json
pyspark/sql/tests/connect/test_parity_datasources.py::DataSourcesParityTests::test_multiline_json
pyspark/sql/tests/connect/test_parity_datasources.py::DataSourcesParityTests::test_read_multiple_orc_file
pyspark/sql/tests/connect/test_parity_functions.py::FunctionsParityTests::test_approxQuantile
pyspark/sql/tests/connect/test_parity_functions.py::FunctionsParityTests::test_functions_broadcast
pyspark/sql/tests/connect/test_parity_functions.py::FunctionsParityTests::test_input_file_name_udf
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_apply_in_pandas_not_returning_pandas_dataframe
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_apply_in_pandas_returning_column_names
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_apply_in_pandas_returning_column_names_sometimes
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_apply_in_pandas_returning_empty_dataframe
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_apply_in_pandas_returning_no_column_names
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_case_insensitive_grouping_column
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_complex_group_by
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_different_keys
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_different_schemas
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_empty_group_by
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_left_group_empty
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_mixed_scalar_udfs_followed_by_cogrouby_apply
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_right_group_empty
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_self_join
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_simple
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_with_key_complex
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_with_key_left
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_with_key_left_group_empty
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_with_key_right
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_with_key_right_group_empty
pyspark/sql/tests/connect/test_parity_pandas_cogrouped_map.py::CogroupedApplyInPandasTests::test_with_window_function
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_apply_in_pandas_not_returning_pandas_dataframe
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_apply_in_pandas_returning_column_names
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_apply_in_pandas_returning_column_names_sometimes
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_apply_in_pandas_returning_empty_dataframe
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_apply_in_pandas_returning_no_column_names
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_apply_in_pandas_returning_no_column_names_and_wrong_amount
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_apply_in_pandas_returning_wrong_column_names
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_array_type_correct
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_case_insensitive_grouping_column
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_coerce
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_column_order
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_complex_groupby
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_datatype_string
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_decorator
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_empty_groupby
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_grouped_over_window
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_grouped_over_window_with_key
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_mixed_scalar_udfs_followed_by_groupby_apply
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_positional_assignment_conf
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_self_join_with_pandas
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_timestamp_dst
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_udf_with_key
pyspark/sql/tests/connect/test_parity_pandas_grouped_map_with_state.py::GroupedApplyInPandasWithStateTests::test_apply_in_pandas_with_state_python_worker_random_failure
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_chain_map_partitions_in_pandas
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_dataframes_with_duplicate_column_names
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_dataframes_with_less_columns
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_dataframes_with_more_columns
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_dataframes_with_other_column_names
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_different_output_length
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_empty_dataframes
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_empty_dataframes_with_less_columns
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_empty_dataframes_with_more_columns
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_empty_dataframes_with_other_columns
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_empty_dataframes_without_columns
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_empty_iterator
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_large_variable_types
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_map_in_pandas
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_map_in_pandas_with_column_vector
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_multiple_columns
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_no_column_names
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_other_than_dataframe_iter
pyspark/sql/tests/connect/test_parity_pandas_map.py::MapInPandasParityTests::test_self_join
pyspark/sql/tests/connect/test_parity_pandas_udf.py::PandasUDFParityTests::test_pandas_udf_arrow_overflow
pyspark/sql/tests/connect/test_parity_pandas_udf.py::PandasUDFParityTests::test_pandas_udf_day_time_interval_type
pyspark/sql/tests/connect/test_parity_pandas_udf.py::PandasUDFParityTests::test_pandas_udf_detect_unsafe_type_conversion
pyspark/sql/tests/connect/test_parity_pandas_udf.py::PandasUDFParityTests::test_pandas_udf_timestamp_ntz
pyspark/sql/tests/connect/test_parity_pandas_udf.py::PandasUDFParityTests::test_stopiteration_in_grouped_agg
pyspark/sql/tests/connect/test_parity_pandas_udf.py::PandasUDFParityTests::test_stopiteration_in_grouped_map
pyspark/sql/tests/connect/test_parity_pandas_udf.py::PandasUDFParityTests::test_stopiteration_in_udf
pyspark/sql/tests/connect/test_parity_pandas_udf_grouped_agg.py::PandasUDFGroupedAggParityTests::test_alias
pyspark/sql/tests/connect/test_parity_pandas_udf_grouped_agg.py::PandasUDFGroupedAggParityTests::test_array_type
pyspark/sql/tests/connect/test_parity_pandas_udf_grouped_agg.py::PandasUDFGroupedAggParityTests::test_basic
pyspark/sql/tests/connect/test_parity_pandas_udf_grouped_agg.py::PandasUDFGroupedAggParityTests::test_complex_expressions
pyspark/sql/tests/connect/test_parity_pandas_udf_grouped_agg.py::PandasUDFGroupedAggParityTests::test_complex_groupby
pyspark/sql/tests/connect/test_parity_pandas_udf_grouped_agg.py::PandasUDFGroupedAggParityTests::test_grouped_without_group_by_clause
pyspark/sql/tests/connect/test_parity_pandas_udf_grouped_agg.py::PandasUDFGroupedAggParityTests::test_invalid_args
pyspark/sql/tests/connect/test_parity_pandas_udf_grouped_agg.py::PandasUDFGroupedAggParityTests::test_mixed_sql
pyspark/sql/tests/connect/test_parity_pandas_udf_grouped_agg.py::PandasUDFGroupedAggParityTests::test_mixed_udfs
pyspark/sql/tests/connect/test_parity_pandas_udf_grouped_agg.py::PandasUDFGroupedAggParityTests::test_multiple_udfs
pyspark/sql/tests/connect/test_parity_pandas_udf_grouped_agg.py::PandasUDFGroupedAggParityTests::test_no_predicate_pushdown_through
pyspark/sql/tests/connect/test_parity_pandas_udf_grouped_agg.py::PandasUDFGroupedAggParityTests::test_register_vectorized_udf_basic
pyspark/sql/tests/connect/test_parity_pandas_udf_grouped_agg.py::PandasUDFGroupedAggParityTests::test_retain_group_columns
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_input_nested_arrays
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_input_nested_maps
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_input_nested_structs
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_mixed_udf
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_mixed_udf_and_sql
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_nondeterministic_vectorized_udf
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_nondeterministic_vectorized_udf_in_aggregate
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_pandas_array_struct
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_pandas_udf_nested_arrays
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_pandas_udf_tokenize
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_pandas_udf_with_column_vector
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_register_nondeterministic_vectorized_udf_basic
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_register_vectorized_udf_basic
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_scalar_iter_udf_close
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_scalar_iter_udf_init
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_timestamp_dst
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_type_annotation
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_udf_category_type
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_array_type
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_basic
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_chained
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_chained_struct_type
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_check_config
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_complex
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_datatype_string
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_dates
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_decorator
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_exception
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_invalid_length
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_map_type
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_nested_struct
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_null_array
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_null_binary
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_null_boolean
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_null_byte
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_null_decimal
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_null_double
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_null_float
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_null_int
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_null_long
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_null_short
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_null_string
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_return_scalar
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_return_timestamp_tz
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_string_in_udf
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_struct_complex
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_struct_empty
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_struct_type
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_timestamps
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_timestamps_respect_session_timezone
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_varargs
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_array_type
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_bounded_mixed
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_bounded_simple
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_growing_window
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_mixed_sql
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_mixed_sql_and_udf
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_mixed_udf
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_multiple_udfs
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_replace_existing
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_shrinking_window
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_simple
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_sliding_window
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_without_partitionBy
pyspark/sql/tests/connect/test_parity_readwriter.py::ReadwriterParityTests::test_bucketed_write
pyspark/sql/tests/connect/test_parity_readwriter.py::ReadwriterParityTests::test_save_and_load
pyspark/sql/tests/connect/test_parity_readwriter.py::ReadwriterParityTests::test_save_and_load_builder
pyspark/sql/tests/connect/test_parity_readwriter.py::ReadwriterV2ParityTests::test_table_overwrite
pyspark/sql/tests/connect/test_parity_types.py::TypesParityTests::test_cast_to_string_with_udt
pyspark/sql/tests/connect/test_parity_types.py::TypesParityTests::test_cast_to_udt_with_udt
pyspark/sql/tests/connect/test_parity_types.py::TypesParityTests::test_complex_nested_udt_in_df
pyspark/sql/tests/connect/test_parity_types.py::TypesParityTests::test_negative_decimal
pyspark/sql/tests/connect/test_parity_types.py::TypesParityTests::test_parquet_with_udt
pyspark/sql/tests/connect/test_parity_types.py::TypesParityTests::test_udf_with_udt
pyspark/sql/tests/connect/test_parity_types.py::TypesParityTests::test_udt_with_none
pyspark/sql/tests/connect/test_parity_types.py::TypesParityTests::test_yearmonth_interval_type
pyspark/sql/tests/connect/test_parity_udf.py::UDFParityTests::test_chained_udf
pyspark/sql/tests/connect/test_parity_udf.py::UDFParityTests::test_complex_return_types
pyspark/sql/tests/connect/test_parity_udf.py::UDFParityTests::test_file_dsv2_with_udf_filter
pyspark/sql/tests/connect/test_parity_udf.py::UDFParityTests::test_multiple_udfs
pyspark/sql/tests/connect/test_parity_udf.py::UDFParityTests::test_nested_array
pyspark/sql/tests/connect/test_parity_udf.py::UDFParityTests::test_nested_map
pyspark/sql/tests/connect/test_parity_udf.py::UDFParityTests::test_nested_struct
pyspark/sql/tests/connect/test_parity_udf.py::UDFParityTests::test_nondeterministic_udf
pyspark/sql/tests/connect/test_parity_udf.py::UDFParityTests::test_nondeterministic_udf2
p
```

(truncated)

</details>

