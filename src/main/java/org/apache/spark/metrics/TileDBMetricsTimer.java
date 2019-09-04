package org.apache.spark.metrics;

import static org.apache.spark.metrics.TileDBMetricsSource.dataSourceBuildRangeFromFilterTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.dataSourceCheckAndMergeRangesTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.dataSourceComputeNeededSplitsToReduceToMedianVolumeTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.dataSourcePlanBatchInputPartitionsTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.dataSourcePruneColumnsTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.dataSourcePushFiltersTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.dataSourceReadSchemaTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryAllocBufferTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryCloseNativeArraysTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryGetDimensionTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryGetScalarAttributeTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryGetTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryGetVariableLengthAttributeTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryInitTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryNextTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryReadTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.tileDBReadQuerySubmitTimerName;

import com.codahale.metrics.Timer.Context;
import org.apache.spark.SparkEnv;

public class TileDBMetricsTimer implements Timer {

  private Context timer;

  TileDBMetricsTimer(TileDBMetricsSource source, String timerName) {
    switch (timerName) {
      case queryReadTimerName:
        timer = source.queryReadTimer.time();
        break;
      case tileDBReadQuerySubmitTimerName:
        timer = source.tileDBReadQuerySubmitTimer.time();
        break;
      case queryInitTimerName:
        timer = source.queryInitTimer.time();
        break;

      case queryAllocBufferTimerName:
        timer = source.queryAllocBufferTimer.time();
        break;
      case queryGetScalarAttributeTimerName:
        timer = source.queryGetScalarAttributeTimer.time();
        break;
      case queryGetVariableLengthAttributeTimerName:
        timer = source.queryGetVariableLengthAttributeTimer.time();
        break;
      case queryGetDimensionTimerName:
        timer = source.queryGetDimensionTimer.time();
        break;
      case queryCloseNativeArraysTimerName:
        timer = source.queryCloseNativeArraysTimer.time();
        break;
      case queryNextTimerName:
        timer = source.queryNextTimer.time();
        break;
      case queryGetTimerName:
        timer = source.queryGetTimer.time();
        break;

      case dataSourceReadSchemaTimerName:
        timer = source.dataSourceReadSchemaTimer.time();
        break;
      case dataSourcePruneColumnsTimerName:
        timer = source.dataSourcePruneColumnsTimer.time();
        break;
      case dataSourcePushFiltersTimerName:
        timer = source.dataSourcePushFiltersTimer.time();
        break;
      case dataSourcePlanBatchInputPartitionsTimerName:
        timer = source.dataSourcePlanBatchInputPartitionsTimer.time();
        break;
      case dataSourceBuildRangeFromFilterTimerName:
        timer = source.dataSourceBuildRangeFromFilterTimer.time();
        break;
      case dataSourceCheckAndMergeRangesTimerName:
        timer = source.dataSourceCheckAndMergeRangesTimer.time();
        break;
      case dataSourceComputeNeededSplitsToReduceToMedianVolumeTimerName:
        timer = source.dataSourceComputeNeededSplitsToReduceToMedianVolumeTimer.time();
        break;
      default:
        timer = source.registerTimer(timerName).time();
    }
    /* MetricRegistry metricRegistry = source.metricRegistry();
    SortedMap<String, com.codahale.metrics.Timer> timers = metricRegistry.getTimers();
    if (timers.containsKey(timerName)) {
      timer = timers.get(timerName).time();
    } else {
      timer = source.registerTimer(timerName).time();
    }*/
  }

  public Long stopTimer() {
    Long t = timer.stop();

    SparkEnv.get().metricsSystem().report();
    return t;
  }
}
