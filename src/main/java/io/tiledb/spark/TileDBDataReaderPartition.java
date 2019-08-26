package io.tiledb.spark;

import java.net.URI;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class TileDBDataReaderPartition implements InputPartition<ColumnarBatch> {

  private final URI uri;
  private final TileDBReadSchema tileDBReadSchema;
  private final TileDBDataSourceOptions tiledbOptions;
  private DomainDimRange[] dimRanges;

  public TileDBDataReaderPartition(
      URI uri,
      TileDBReadSchema schema,
      TileDBDataSourceOptions options,
      DomainDimRange[] dimRanges) {
    this.uri = uri;
    this.tileDBReadSchema = schema;
    this.tiledbOptions = options;
    this.dimRanges = dimRanges;
  }

  @Override
  public InputPartitionReader<ColumnarBatch> createPartitionReader() {
    return new TileDBDataReaderPartitionScan(uri, tileDBReadSchema, tiledbOptions, dimRanges);
  }
}
