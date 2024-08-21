# TileDB-Spark
![TileDB-Spark CI](https://github.com/TileDB-Inc/TileDB-Spark/actions/workflows/github_actions.yml/badge.svg)

Currently works for Spark (v3.2.0).

We also provide a stable version for Spark (v2.4.4) in [this branch.](https://github.com/TileDB-Inc/TileDB-Spark/tree/Spark-2.4.4-Stable) 

## Build / Test

To build and install

```
    git clone git@github.com:TileDB-Inc/TileDB-Spark.git
    cd TileDB-Spark
    ./gradlew assemble
    ./gradlew shadowJar
    ./gradlew test
```

This will create a `build/libs/TileDB-Spark-X.X.X-SNAPSHOT.jar` JAR as well as build a TileDB-Java Jar that

### Amazon-Linux / EMR

## Spark Shell

To load the TileDB Spark Datasource reader, 
you need to specify the path to built project jar with `--jars` to upload to the Spark cluster.

    $ spark-shell --jars build/libs/TileDB-Spark-X.X.X-SNAPSHOT.jar,/path/to/TileDB-Java-0.X.X.jar

To read TileDB data to a dataframe in the TileDB format, specify the `format` and `uri` option.
Optionally include the `read_buffer_size` to set the off heap tiledb buffer sizes per attribute (include coordinates).
 
    scala> val sampleDF = spark.read
                               .format("io.tiledb.spark")
                               .option("read_buffer_size", 100*1024*1024)
                               .load("file:///path/to/tiledb/array")

To write to TileDB from an existing dataframe, you need to specify a URI and the column(s) which map to sparse array dimensions.  For now only sparse array writes are supported.

    scala > val sampleDF.write
                        .format("io.tiledb.spark")
                        .option("schema.dim.0.name", "rows")
                        .option("schema.dim.1.name", "cols")
                        .option("write_buffer_size", 100*1024*1024)
                        .mode(SaveMode.ErrorIfExists)
                        .save("file:///path/to/tiledb/array_new");

## Metrics

Reporting metrics are supported via dropwizard and the default spark
metrics setup. Metrics can be enabled by adding the following lines to your
`metric.properties` file:

```
driver.source.io.tiledb.spark.class=org.apache.spark.metrics.TileDBMetricsSource
executor.source.io.tiledb.spark.class=org.apache.spark.metrics.TileDBMetricsSource
```

### Using Metrics on Executor/Worker Nodes

When loading an application jar (i.e. via `--jar` cli flag use by
spark-submit/pyspark/sparkR) the metrics are available to the master node
and the `driver` metrics will report. However the executors will error
about class not found. This is because on each worker node a jar containing
the `org.apache.spark.metrics.TileDBMetricsSource` must be provided in the
class path.

A dedicated jar `tiledb-spark-metrics-$version.jar` is built by default to
allow a user to place this in the class path. Typically this jar can be copied
to `$SPARK_HOME/jars/`.

## Options

### Read/Write options
* `uri` (legacy): URI to TileDB sparse or dense array. URI should be a parameter of load()/save() instead.
* `tiledb.` (optional): Set a TileDB config option, ex: `option("tiledb.vfs.num_threads", 4)`.  Multiple tiledb config options can be
   specified.  See the
  [full list of configuration options](https://docs.tiledb.io/en/latest/tutorials/config.html?highlight=config#summary-of-parameters).

### Read options
* `order` (optional): Result layout order `"row-major"`/ `"TILEDB_ROW_MAJOR"`, `"col-major"` / `"TILEDB_COL_MAJOR"`, or `"unordered"`/ `"TILEDB_UNORDERED"` (default `"unordered"`).
* `read_buffer_size` (optional): Set the TileDB read buffer size in bytes per attribute/coordinates. Defaults to 10MB
* `allow_read_buffer_realloc` (optional): If the read buffer size is too small allow reallocation. Default: True
* `timestamp_start`(optional): The start timestamp at which to open the array
* `timestamp_end`(optional): The end timestamp at which to open the array
* `print_array_metadata`(optional): Prints the array metadata to the console
* `tiledb_filtering`(optional): If false, disables the use of TileDB's Query Condition API and uses Spark filtering

### Write options
* `write_buffer_size` (optional): Set the TileDB read buffer size in bytes per attribute/coordinates. Defaults to 10MB
* `schema.dim.<N>.name` (requried): Specify which of the spark dataframe columns names are dimensions
* `schema.dim.<N>.min` (optional): Specify the lower bound for the TileDB array schema
* `schema.dim.<N>.max` (optional): Specify the upper bound for the TileDB array schema
* `schema.dim.<N>.extent` (optional): Specify the schema dimension domain extent (tile size)
* `schema.attr.<NAME>.filter_list` (optional): Specfify filter list for attribute NAME.  Filter list is a tuple of the form `(name, option)`, ex: `"(byteshuffle, -1), (gzip, 9)"`
* `schema.set_allows_dups` (optional): Specify whether to allow multiple cells with the same coordinates to exist. (Sparse arrays only)
* `schema.capacity` (optional): Specify the sparse array tile capacity
* `schema.cell_order` (optional): Specify the cell order. Filter list is a tuple of the form `(name, option)`, ex: `"(byteshuffle, -1), (gzip, 9)"`
* `schema.tile_order` (optional): Specify the tile order. Filter list is a tuple of the form `(name, option)`, ex: `"(byteshuffle, -1), (gzip, 9)"`
* `schema.coords_filter_list` (optional): Specify the coordinate filter list
* `schema.offsets_filter_list` (optional): Specify the offsets filter list
* `schema.<ATT_NAME>.filter_list` (optional): Specify the attribute filter list
* `metadata_value.<KEY>` (optional): The metadata value for a given key
* `metadata_type.<KEY>` (optional): The metadata datatype for a given key
  <br /><br /> Note: Filters are passed as options as shown [here](https://github.com/TileDB-Inc/TileDB-Spark/blob/abcbce41950d105a609aeed5d2d498f64945f2bc/src/test/java/io/tiledb/spark/NullableAttributesTest.java#L519). Each filter needs to have at least one parameter. Use -1 if the filter in use does not take any parameters (e.g (byteshuffle, -1))
## Semantics

### Type Mapping

TileDB-Spark does not support all of TileDB's datatypes.  

* Currently, Integer, Float / Double, all TILEDB_DATETIME types, and ASCII / UTF-8 strings are supported.
* Because integers are upcasted to the next largest signed datatype expressible in Java (ex. `TILEDB_UINT8` -> Java `Short`),
except for `TILEDB_UINT64` which is not expressible as a numeric primitive in Java.
* TileDB `UINT64` values are casted to Java `Long` integers.  Java provides limited functionality for re-interpreting `Long` values as unsigned `Long`.

### Correctness / Validation

* TileDB-Spark doesn't validate UTF-8 data and is assumed that the written TileDB UTF-8 array data is correctly encoded on write.
