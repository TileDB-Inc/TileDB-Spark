package io.tiledb.spark;

import java.util.Map;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.WriteBuilder;

public class TileDBWriteBuilder implements WriteBuilder, SupportsTruncate {
  private final Map<String, String> properties;
  private final LogicalWriteInfo info;
  private boolean toTruncate;

  public TileDBWriteBuilder(Map<String, String> properties, LogicalWriteInfo info) {
    this.properties = properties;
    this.info = info;
    this.toTruncate = false;
  }

  @Override
  public BatchWrite buildForBatch() { // todo check deprecation
    if (toTruncate) return new TileDBBatchWrite(properties, info, SaveMode.Overwrite);
    return new TileDBBatchWrite(properties, info, SaveMode.Append);
  }

  @Override
  public WriteBuilder truncate() {
    this.toTruncate = true;
    return this;
  }
}
