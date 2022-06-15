package io.tiledb.spark;

import static org.apache.spark.metrics.TileDBMetricsSource.dataSourceReadSchemaTimerName;

import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.metrics.TileDBReadMetricsUpdater;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

public class TileDBScan implements Scan {
  private final TileDBReadSchema tileDBReadSchema;
  private final TileDBDataSourceOptions options;
  private final Filter[] pushedFilters;
  private final String uri;

  private final TileDBReadMetricsUpdater metricsUpdater;
  static Logger log = Logger.getLogger(TileDBScan.class.getName());

  public TileDBScan(
      TileDBReadSchema tileDBReadSchema, TileDBDataSourceOptions options, Filter[] pushedFilters) {
    this.tileDBReadSchema = tileDBReadSchema;
    this.options = options;
    this.pushedFilters = pushedFilters;
    this.metricsUpdater = new TileDBReadMetricsUpdater(TaskContext.get());
    this.uri = util.tryGetArrayURI(options);
  }

  @Override
  public StructType readSchema() {
    metricsUpdater.startTimer(dataSourceReadSchemaTimerName);
    log.trace("Reading schema for " + uri);
    StructType schema = tileDBReadSchema.getSparkSchema();
    log.trace("Read schema for " + uri + ": " + schema);
    metricsUpdater.finish(dataSourceReadSchemaTimerName);
    return tileDBReadSchema.getSparkSchema();
  }

  @Override
  public String description() {
    return Scan.super.description();
  }

  @Override
  public Batch toBatch() {
    return new TileDBBatch(tileDBReadSchema, options, pushedFilters);
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
