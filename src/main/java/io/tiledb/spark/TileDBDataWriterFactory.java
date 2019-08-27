package io.tiledb.spark;

import java.net.URI;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

public class TileDBDataWriterFactory implements DataWriterFactory<InternalRow> {

  private URI uri;
  private StructType sparkSchema;
  private TileDBDataSourceOptions options;

  public TileDBDataWriterFactory(URI uri, StructType sparkSchema, TileDBDataSourceOptions options) {
    this.uri = uri;
    this.sparkSchema = sparkSchema;
    this.options = options;
  }

  @Override
  public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
    return new TileDBDataWriter(uri, sparkSchema, options);
  }
}
