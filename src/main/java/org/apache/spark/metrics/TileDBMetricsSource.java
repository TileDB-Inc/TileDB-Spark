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

  protected final Timer queryReadTimer;
  protected final Timer tileDBReadQuerySubmitTimer;
  protected final Timer queryInitTimer;
  protected final Timer queryAllocBufferTimer;
  protected final Timer queryGetScalarAttributeTimer;
  protected final Timer queryGetVariableLengthAttributeTimer;
  protected final Timer queryGetDimensionTimer;
  protected final Timer queryCloseNativeArraysTimer;
  protected final Timer queryNextTimer;
  protected final Timer queryGetTimer;

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

  protected final Timer dataSourceReadSchemaTimer;
  protected final Timer dataSourcePruneColumnsTimer;
  protected final Timer dataSourcePushFiltersTimer;
  protected final Timer dataSourcePlanBatchInputPartitionsTimer;
  protected final Timer dataSourceBuildRangeFromFilterTimer;
  protected final Timer dataSourceCheckAndMergeRangesTimer;
  protected final Timer dataSourceComputeNeededSplitsToReduceToMedianVolumeTimer;
  private MetricRegistry metricRegistry;

  static Logger log = Logger.getLogger(TileDBMetricsSource.class.getName());

  public TileDBMetricsSource() {
    log.info("Creating TileDBMetricsSource");
    metricRegistry = new MetricRegistry();
    queryReadTimer = metricRegistry.timer(queryReadTimerName);
    tileDBReadQuerySubmitTimer = metricRegistry.timer(tileDBReadQuerySubmitTimerName);
    queryInitTimer = metricRegistry.timer(queryInitTimerName);
    queryAllocBufferTimer = metricRegistry.timer(queryAllocBufferTimerName);
    queryGetScalarAttributeTimer = metricRegistry.timer(queryGetScalarAttributeTimerName);
    queryGetVariableLengthAttributeTimer =
        metricRegistry.timer(queryGetVariableLengthAttributeTimerName);
    queryGetDimensionTimer = metricRegistry.timer(queryGetDimensionTimerName);
    queryCloseNativeArraysTimer = metricRegistry.timer(queryCloseNativeArraysTimerName);
    queryNextTimer = metricRegistry.timer(queryNextTimerName);
    queryGetTimer = metricRegistry.timer(queryGetTimerName);

    // Data source metrics
    dataSourceReadSchemaTimer = metricRegistry.timer(dataSourceReadSchemaTimerName);
    dataSourcePruneColumnsTimer = metricRegistry.timer(dataSourcePruneColumnsTimerName);
    dataSourcePushFiltersTimer = metricRegistry.timer(dataSourcePushFiltersTimerName);
    dataSourcePlanBatchInputPartitionsTimer =
        metricRegistry.timer(dataSourcePlanBatchInputPartitionsTimerName);
    dataSourceBuildRangeFromFilterTimer =
        metricRegistry.timer(dataSourceBuildRangeFromFilterTimerName);
    dataSourceCheckAndMergeRangesTimer =
        metricRegistry.timer(dataSourceCheckAndMergeRangesTimerName);
    dataSourceComputeNeededSplitsToReduceToMedianVolumeTimer =
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
