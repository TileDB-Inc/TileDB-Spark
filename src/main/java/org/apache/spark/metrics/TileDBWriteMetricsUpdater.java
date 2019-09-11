package org.apache.spark.metrics;

import static org.apache.spark.metrics.TileDBMetricsSource.sourceName;

import java.util.HashMap;
import java.util.Optional;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.OutputMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.metrics.source.Source;
import scala.collection.Seq;

/** Handler read related metrics */
public class TileDBWriteMetricsUpdater extends MetricsUpdater {
  private OutputMetrics outputMetrics = null;
  private TileDBMetricsSource source = null;
  private HashMap<String, Timer> timers;

  /**
   * Get source and set outputMetrics if inside a task
   *
   * @param task optional task
   */
  public TileDBWriteMetricsUpdater(TaskContext task) {

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
      outputMetrics = tm.outputMetrics();
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
   * Publish record count and record sizes to spark output metrics
   *
   * @param recordCount number of records written
   * @param sizeInBytes size of records written
   */
  public void appendTaskMetrics(long recordCount, long sizeInBytes) {
    if (outputMetrics != null) {
      outputMetrics.setBytesWritten(sizeInBytes + outputMetrics.bytesWritten());
      outputMetrics.setRecordsWritten(recordCount + outputMetrics.recordsWritten());
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
