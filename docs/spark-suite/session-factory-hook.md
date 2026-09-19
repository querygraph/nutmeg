<!-- Run on quegee, 2026-09-19. Base binary: ~/opt/sail311-base-20f4de4f,
branch binary: ~/opt/sail311-hook-991d50ca, both release builds with
PYO3_PYTHON pointing at the test environment's Python 3.11. Each suite's
server was verified to be the one this gate started. -->

### session-factory-hook

#### Commit Information

| Commit | Revision | Branch |
| :--- | :--- | :--- |
| **After** | `991d50c` | `session-factory-hook-branch` |
| **Before** | `20f4de4` | `session-factory-hook-base` |

#### Test Summary

| Suite | Commit | Failed | Passed | Skipped | Warnings | Time (s) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `doctest-catalog` | **After** | 10 | 14 | 1 | 138 | 5.10 |
|  | **Before** | 10 | 14 | 1 | 138 | 5.09 |
| `doctest-column` | **After** |  | 33 |  | 140 | 5.07 |
|  | **Before** |  | 33 |  | 140 | 5.13 |
| `doctest-dataframe` | **After** | 14 | 83 | 10 | 135 | 5.64 |
|  | **Before** | 14 | 83 | 10 | 135 | 5.59 |
| `doctest-functions` | **After** | 13 | 387 | 9 | 175 | 8.72 |
|  | **Before** | 13 | 387 | 9 | 175 | 8.71 |
| `test-connect` | **After** | 106 | 896 | 170 | 4756 | 51.25 |
|  | **Before** | 106 | 896 | 170 | 4752 | 50.48 |


#### Test Details

<details>
<summary>Error Counts</summary>

```text
          143 Total
           83 Total Unique
-------- ---- ----------------------------------------------------------------------------------------------------------
           11 PySparkAssertionError: [DIFFERENT_PANDAS_DATAFRAME] DataFrames are not almost equal:
           10 handle add artifacts
            9 DocTestFailure
            6 UnsupportedOperationException: PlanNode::CacheTable
            5 UnsupportedOperationException: function: input_file_name
            4 AssertionError: False is not true
            3 ValueError: Converting to Python dictionary is not supported when duplicate field names are present
            2 AnalysisException: Could not find config namespace "spark"
            2 AnalysisException: Internal error: Function 'approx_percentile_cont' failed to match any signature, errors: Error during planning: Function 'approx_percentile_cont' expects 2 arguments but received 3,...
            2 AnalysisException: No data source found for: orc
            2 AnalysisException: explode should be rewritten during logical plan analysis
            2 AnalysisException: not supported: function exists
            2 AssertionError
            2 AssertionError: 0 not greater than or equal to 1
            2 IllegalArgumentException: invalid argument: found FUNCTION at 7:15 expected 'DATABASE', 'SCHEMA', 'NAMESPACE', 'OR', 'TEMP', 'TEMPORARY', 'EXTERNAL', 'TABLE', 'GLOBAL', or 'VIEW'
            2 IllegalArgumentException: invalid argument: found RESET at 0:5 expected something else, ';', statement, or end of input
            2 PySparkNotImplementedError: [NOT_IMPLEMENTED] rdd() is not implemented.
            2 UnsupportedOperationException: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: avg@khwf9yq01gq0jba04bt8182z(#9) PARTITION BY [#8] ORDER BY [#9 ASC NULLS ...
            2 UnsupportedOperationException: approx quantile
            2 UnsupportedOperationException: collect metrics
            2 UnsupportedOperationException: freq items
            2 UnsupportedOperationException: function: session_window
            2 UnsupportedOperationException: handle analyze same semantics
            2 UnsupportedOperationException: user defined data type should only exist in a field
            2 UnsupportedOperationException: with watermark
            2 handle artifact statuses
            1 AnalysisException: Table already exists: tbl1
            1 AnalysisException: Temporary View not found: tab2
            1 AnalysisException: UNION queries have different number of columns: left has 3 columns whereas right has 2 columns
            1 AnalysisException: not supported: qualified function name
(+1)        1 AssertionError: "Database 'memory:929512da-737c-45a5-b73c-09d001ad80d5' dropped." does not match "No data source found for: jdbc. The JDBC data source is provided by pysail and must be registered befo...
(+1)        1 AssertionError: "Database 'memory:a683da26-594b-44c1-a739-568cd806aea0' dropped." does not match "No data source found for: jdbc. The JDBC data source is provided by pysail and must be registered befo...
            1 AssertionError: 1 != 0
            1 AssertionError: 4 != 10
            1 AssertionError: AnalysisException not raised
            1 AssertionError: AnalysisException not raised by <lambda>
            1 AssertionError: Exception not raised
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
            1 PythonException:  AttributeError: 'NoneType' object has no attribute 'partitionId'
            1 PythonException:  AttributeError: 'list' object has no attribute 'x'
            1 PythonException:  AttributeError: 'list' object has no attribute 'y'
            1 SparkRuntimeException: Cast error: Cannot cast string '1997/02/28 10:30:00' to value of Date32 type
            1 SparkRuntimeException: Invalid argument error: 83.140 is too large to store in a Decimal128 of precision 4. Max is 9.999
            1 SparkRuntimeException: Json error: Not valid JSON: EOF while parsing a list at line 1 column 1
            1 SparkRuntimeException: Json error: Not valid JSON: expected value at line 1 column 2
            1 SparkRuntimeException: Parser error: Error while parsing value '0
            1 SparkRuntimeException: This feature is not implemented: Unsupported CAST from Map("entries": non-null Struct("key": non-null Int32, "value": non-null Int32), unsorted) to Null
            1 UnsupportedOperationException: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: avg@khwf9yq01gq0jba04bt8182z(#9) PARTITION BY [#8] ORDER BY [#9 ASC NULLS ...
            1 UnsupportedOperationException: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: avg@khwf9yq01gq0jba04bt8182z(plus_one@9vknmx3ofwm6psl9zm3y4xhkb(#9)) PARTI...
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
(-1)        0 AssertionError: "Database 'memory:1288ef64-573f-460f-bfc8-70521aa773bc' dropped." does not match "No data source found for: jdbc. The JDBC data source is provided by pysail and must be registered befo...
(-1)        0 AssertionError: "Database 'memory:3543791b-9c24-4d62-add3-6c4e540df96d' dropped." does not match "No data source found for: jdbc. The JDBC data source is provided by pysail and must be registered befo...
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
pyspark/sql/tests/connect/test_parity_arrow.py::ArrowParityTests::test_toPandas_duplicate_field_names
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::ArrowPythonUDFParityTests::test_udf_with_input_file_name
pyspark/sql/tests/connect/test_parity_arrow_python_udf.py::UDFParityTests::test_udf_with_input_file_name
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
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_grouped_over_window
pyspark/sql/tests/connect/test_parity_pandas_grouped_map.py::GroupedApplyInPandasTests::test_grouped_over_window_with_key
pyspark/sql/tests/connect/test_parity_pandas_grouped_map_with_state.py::GroupedApplyInPandasWithStateTests::test_apply_in_pandas_with_state_python_worker_random_failure
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_scalar_iter_udf_init
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_check_config
pyspark/sql/tests/connect/test_parity_pandas_udf_scalar.py::PandasUDFScalarParityTests::test_vectorized_udf_invalid_length
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_bounded_mixed
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_bounded_simple
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_shrinking_window
pyspark/sql/tests/connect/test_parity_pandas_udf_window.py::PandasUDFWindowParityTests::test_sliding_window
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
pyspark/sql/tests/connect/test_parity_udf.py::UDFParityTests::test_udf_with_input_file_name
pyspark/sql/tests/connect/test_parity_udtf.py::ArrowUDTFParityTests::test_udtf_arrow_sql_conf
pyspark/sql/tests/connect/test_utils.py::ConnectUtilsTests::test_assert_approx_equal_decimaltype_custom_rtol_pass
pyspark/sql/tests/connect/test_utils.py::ConnectUtilsTests::test_assert_equal_nested_struct_str_duplicate
```

</details>

