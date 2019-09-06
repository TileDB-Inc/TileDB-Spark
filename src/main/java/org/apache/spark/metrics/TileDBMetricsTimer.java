package org.apache.spark.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer.Context;
import java.util.SortedMap;
import org.apache.spark.SparkEnv;

/** Timer that will report results to spark when it is stopped */
public class TileDBMetricsTimer implements Timer {

  private Context timer;

  /**
   * Constructor for starting timer
   *
   * @param source source to register with
   * @param timerName name of timer
   */
  TileDBMetricsTimer(TileDBMetricsSource source, String timerName) {
    MetricRegistry metricRegistry = source.metricRegistry();
    SortedMap<String, com.codahale.metrics.Timer> timers = metricRegistry.getTimers();
    if (timers.containsKey(timerName)) {
      timer = timers.get(timerName).time();
    } else {
      timer = source.registerTimer(timerName).time();
    }
  }

  /**
   * Stop timer, report duration and return duration
   *
   * @return duration
   */
  public Long stopTimer() {
    Long t = timer.stop();

    SparkEnv.get().metricsSystem().report();
    return t;
  }
}
