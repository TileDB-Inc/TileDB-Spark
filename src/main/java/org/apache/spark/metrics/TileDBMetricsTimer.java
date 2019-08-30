package org.apache.spark.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer.Context;
import java.util.SortedMap;
import org.apache.spark.SparkEnv;

public class TileDBMetricsTimer implements Timer {

  private Context timer;

  TileDBMetricsTimer(TileDBMetricsSource source, String timerName) {
    MetricRegistry metricRegistry = source.metricRegistry();
    SortedMap<String, com.codahale.metrics.Timer> timers = metricRegistry.getTimers();
    if (timers.containsKey(timerName)) {
      timer = timers.get(timerName).time();
    } else {
      timer = source.registerTimer(timerName).time();
    }
  }

  public Long stopTimer() {
    Long t = timer.stop();

    SparkEnv.get().metricsSystem().report();
    return t;
  }
}
