package io.tiledb.spark;

import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

public class TileDBWriterFactory implements DataWriterFactory {

  private final String jobId;
  private final StructType schema;
  private final TileDBOptions options;

  public TileDBWriterFactory(String jobId, StructType schema, TileDBOptions options) {
    this.jobId = jobId;
    this.schema = schema;
    this.options = options;
  }

  @Override
  public DataWriter createDataWriter(int partitionId, int attemptNumber) {
    return new TileDBDataWriter(schema, options);
  }
}
