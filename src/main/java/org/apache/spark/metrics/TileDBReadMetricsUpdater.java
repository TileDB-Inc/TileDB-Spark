package org.apache.spark.metrics;

import static org.apache.spark.metrics.TileDBMetricsSource.sourceName;

import java.util.HashMap;
import java.util.Optional;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.InputMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.metrics.source.Source;
import scala.collection.Seq;

/** Handler read related metrics */
public class TileDBReadMetricsUpdater extends MetricsUpdater {
  private InputMetrics inputMetrics = null;
  private TileDBMetricsSource source = null;
  private HashMap<String, Timer> timers;

  /**
   * Get source and set inputMetrics if inside a task
   *
   * @param task optional task
   */
  public TileDBReadMetricsUpdater(TaskContext task) {

    // Try to find the source metric
    Optional<TileDBMetricsSource> tmp = getSource(task);
    if (tmp.isPresent()) {
      source = tmp.get();
    } else {
      SparkEnv env = SparkEnv.get();
      Seq<Source> sources = env.metricsSystem().getSourcesByName(sourceName);
      if (sources.length() > 1) source = (TileDBMetricsSource) sources.head();
    }
    timers = new HashMap<>();

    if (task != null) {
      TaskMetrics tm = task.taskMetrics();
      inputMetrics = tm.inputMetrics();
    }
  }

  /**
   * Start a timer by creating a wrapper timer class around the name Store the resulting timer on a
   * map so we can stop it later
   *
   * @param timerName timer to start
   */
  public synchronized void startTimer(String timerName) {
    if (source != null) {
      timers.put(timerName, new TileDBMetricsTimer(source, timerName));
    } else {
      timers.put(timerName, new SimpleTimer());
    }
  }

  /**
   * Publish record count and record sizes to spark input metrics
   *
   * @param recordCount number of records from read
   * @param sizeInBytes size of results to add
   */
  public void updateTaskMetrics(long recordCount, long sizeInBytes) {
    if (inputMetrics != null) {
      inputMetrics.incBytesRead(sizeInBytes);
      inputMetrics.incRecordsRead(recordCount);
    }
  }

  /**
   * Stop a timer
   *
   * @param timerName timer to stop
   * @return duration
   */
  @Override
  public Long finish(String timerName) {
    if (timers.containsKey(timerName)) {
      return timers.get(timerName).stopTimer();
    }
    return null;
  }
}
