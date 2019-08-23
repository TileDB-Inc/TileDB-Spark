package io.tiledb.spark;

import java.net.URI;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class TileDBDataReaderPartition implements InputPartition<ColumnarBatch> {

  private final Filter[] pushedFilters;
  private URI uri;
  private TileDBReadSchema tileDBReadSchema;
  private TileDBDataSourceOptions tiledbOptions;

  public TileDBDataReaderPartition(
      URI uri, TileDBReadSchema schema, TileDBDataSourceOptions options, Filter[] pushedFilters) {
    this.uri = uri;
    this.tileDBReadSchema = schema;
    this.tiledbOptions = options;
    this.pushedFilters = pushedFilters;
  }

  @Override
  public InputPartitionReader<ColumnarBatch> createPartitionReader() {
    return new TileDBDataReaderPartitionScan(uri, tileDBReadSchema, tiledbOptions, pushedFilters);
  }
}
