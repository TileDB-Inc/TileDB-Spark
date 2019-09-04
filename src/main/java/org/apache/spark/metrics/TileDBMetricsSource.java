package org.apache.spark.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.log4j.Logger;
import org.apache.spark.metrics.source.Source;

public class TileDBMetricsSource implements Source {
  public static final String sourceName = "tiledb";

  public static final String queryReadTimerName = "query-read-task";
  public static final String tileDBReadQuerySubmitTimerName = "tiledb-read-query-submit";
  public static final String queryInitTimerName = "query-init";
  public static final String queryAllocBufferTimerName = "query-alloc-buffers";
  public static final String queryGetScalarAttributeTimerName = "query-get-scalar-attribute";
  public static final String queryGetVariableLengthAttributeTimerName =
      "query-get-variable-length-attribute";
  public static final String queryGetDimensionTimerName = "query-get-dimension";
  public static final String queryCloseNativeArraysTimerName = "query-close-native-arrays";
  public static final String queryNextTimerName = "query-next";
  public static final String queryGetTimerName = "query-get";

  // Data source metrics
  public static final String dataSourceReadSchemaTimerName = "data-source-read-schema";
  public static final String dataSourcePruneColumnsTimerName = "data-source-prune-columns";
  public static final String dataSourcePushFiltersTimerName = "data-source-push-filters";
  public static final String dataSourcePlanBatchInputPartitionsTimerName =
      "data-source-plan-batch-input-partitions";
  public static final String dataSourceBuildRangeFromFilterTimerName =
      "data-source-build-range-from-filter";
  public static final String dataSourceCheckAndMergeRangesTimerName =
      "data-source-check-and-merge-ranges";
  public static final String dataSourceComputeNeededSplitsToReduceToMedianVolumeTimerName =
      "data-source-computer-needed-splits";
  private MetricRegistry metricRegistry;

  static Logger log = Logger.getLogger(TileDBMetricsSource.class.getName());

  public TileDBMetricsSource() {
    log.info("Creating TileDBMetricsSource");
    metricRegistry = new MetricRegistry();
    metricRegistry.timer(queryReadTimerName);
    metricRegistry.timer(tileDBReadQuerySubmitTimerName);
    metricRegistry.timer(queryInitTimerName);
    metricRegistry.timer(queryAllocBufferTimerName);
    metricRegistry.timer(queryGetScalarAttributeTimerName);
    metricRegistry.timer(queryGetVariableLengthAttributeTimerName);
    metricRegistry.timer(queryGetDimensionTimerName);
    metricRegistry.timer(queryCloseNativeArraysTimerName);
    metricRegistry.timer(queryNextTimerName);
    metricRegistry.timer(queryGetTimerName);

    // Data source metrics
    metricRegistry.timer(dataSourceReadSchemaTimerName);
    metricRegistry.timer(dataSourcePruneColumnsTimerName);
    metricRegistry.timer(dataSourcePushFiltersTimerName);
    metricRegistry.timer(dataSourcePlanBatchInputPartitionsTimerName);
    metricRegistry.timer(dataSourceBuildRangeFromFilterTimerName);
    metricRegistry.timer(dataSourceCheckAndMergeRangesTimerName);
    metricRegistry.timer(dataSourceComputeNeededSplitsToReduceToMedianVolumeTimerName);
  }

  @Override
  public String sourceName() {
    return sourceName;
  }

  public Timer registerTimer(String timerName) {
    return metricRegistry.timer(timerName);
  }

  @Override
  public MetricRegistry metricRegistry() {
    return metricRegistry;
  }
}
