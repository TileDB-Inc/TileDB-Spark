package io.tiledb.spark;

import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;

public class TileDBArrayWriterFactory implements DataWriterFactory {

  @Override
  public DataWriter createDataWriter(int partitionId, long taskId, long epochId) {
    return null;
  }
}
