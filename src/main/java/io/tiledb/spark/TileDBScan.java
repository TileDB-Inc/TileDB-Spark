package io.tiledb.spark;

import java.util.Map;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

public class TileDBScan implements Scan {
  private final TileDBReadSchema tileDBReadSchema;
  private final Map<String, String> properties;
  private final TileDBDataSourceOptions options;
  private final Filter[] pushedFilters;

  public TileDBScan(
      TileDBReadSchema tileDBReadSchema,
      Map<String, String> properties,
      TileDBDataSourceOptions options,
      Filter[] pushedFilters) {
    this.tileDBReadSchema = tileDBReadSchema;
    this.properties = properties;
    this.options = options;
    this.pushedFilters = pushedFilters;
  }

  @Override
  public StructType readSchema() {
    //    metricsUpdater.startTimer(dataSourceReadSchemaTimerName);
    //    log.trace("Reading schema for " + uri);
    //    StructType schema = tileDBReadSchema.getSparkSchema();
    //    log.trace("Read schema for " + uri + ": " + schema);
    //    metricsUpdater.finish(dataSourceReadSchemaTimerName); TODO add
    return tileDBReadSchema.getSparkSchema();
  }

  @Override
  public String description() {
    return Scan.super.description();
  }

  @Override
  public Batch toBatch() {
    return new TileDBBatch(tileDBReadSchema, properties, options, pushedFilters);
  }

  @Override
  public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    return Scan.super.toMicroBatchStream(checkpointLocation);
  }

  @Override
  public ContinuousStream toContinuousStream(String checkpointLocation) {
    return Scan.super.toContinuousStream(checkpointLocation);
  }

  @Override
  public CustomMetric[] supportedCustomMetrics() {
    return Scan.super.supportedCustomMetrics();
  }
}
