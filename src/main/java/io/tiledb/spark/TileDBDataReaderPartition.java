package io.tiledb.spark;

import java.net.URI;
import java.util.List;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class TileDBDataReaderPartition implements InputPartition<ColumnarBatch> {

  private final List<List<Range>> dimensionRanges;
  private final List<List<Range>> attributeRanges;
  private URI uri;
  private TileDBReadSchema tileDBReadSchema;
  private TileDBDataSourceOptions tiledbOptions;

  public TileDBDataReaderPartition(
      URI uri,
      TileDBReadSchema schema,
      TileDBDataSourceOptions options,
      List<List<Range>> dimensionRanges,
      List<List<Range>> attributeRanges) {
    this.uri = uri;
    this.tileDBReadSchema = schema;
    this.tiledbOptions = options;
    this.dimensionRanges = dimensionRanges;
    this.attributeRanges = attributeRanges;
  }

  @Override
  public InputPartitionReader<ColumnarBatch> createPartitionReader() {
    return new TileDBDataReaderPartitionScan(
        uri, tileDBReadSchema, tiledbOptions, dimensionRanges, attributeRanges);
  }
}
