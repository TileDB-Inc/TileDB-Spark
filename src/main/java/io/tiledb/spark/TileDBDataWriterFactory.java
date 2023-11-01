package io.tiledb.spark;

import io.tiledb.java.api.TileDBError;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

public class TileDBDataWriterFactory implements DataWriterFactory {

  private String uri;
  private StructType sparkSchema;
  private TileDBDataSourceOptions options;

  public TileDBDataWriterFactory(
      String uri, StructType sparkSchema, TileDBDataSourceOptions options) {
    this.uri = uri;
    this.sparkSchema = sparkSchema;
    this.options = options;
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    try {
      return new TileDBDataWriter(uri, sparkSchema, options);
    } catch (TileDBError e) {
      throw new RuntimeException(e);
    }
  }
}
