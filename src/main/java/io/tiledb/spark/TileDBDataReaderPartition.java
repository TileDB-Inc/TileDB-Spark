package io.tiledb.spark;

import io.tiledb.java.api.Pair;
import java.net.URI;
import java.util.List;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class TileDBDataReaderPartition implements InputPartition<ColumnarBatch> {

  private final List<List<Pair>> pushedRanges;
  private URI uri;
  private TileDBReadSchema tileDBReadSchema;
  private TileDBDataSourceOptions tiledbOptions;

  public TileDBDataReaderPartition(
      URI uri,
      TileDBReadSchema schema,
      TileDBDataSourceOptions options,
      List<List<Pair>> pushedRanges) {
    this.uri = uri;
    this.tileDBReadSchema = schema;
    this.tiledbOptions = options;
    this.pushedRanges = pushedRanges;
  }

  @Override
  public InputPartitionReader<ColumnarBatch> createPartitionReader() {
    return new TileDBDataReaderPartitionScan(uri, tileDBReadSchema, tiledbOptions, pushedRanges);
  }
}
