package org.apache.spark.metrics;

import io.tiledb.spark.TileDBDataSourceOptions;
import io.tiledb.spark.TileDBDataSourceReader;
import java.util.HashMap;
import java.util.Optional;
import org.apache.log4j.Logger;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.InputMetrics;
import org.apache.spark.executor.TaskMetrics;

public class TileDBReadMetricsUpdater extends MetricsUpdater {
  static Logger log = Logger.getLogger(TileDBDataSourceReader.class.getName());
  //  private Timer timer;
  private InputMetrics inputMetrics = null;
  private TileDBMetricsSource source = null;
  private HashMap<String, Timer> timers;

  public TileDBReadMetricsUpdater(TaskContext task, TileDBDataSourceOptions options) {

    Optional<TileDBMetricsSource> tmp = getSource(task);
    if (tmp.isPresent()) {
      source = tmp.get();
    }
    timers = new HashMap<>();

    if (options.getTaskMetricsEnabled() && task != null) {
      TaskMetrics tm = task.taskMetrics();
      inputMetrics = tm.inputMetrics();
    }
  }

  public synchronized void startTimer(String timerName) {
    if (source != null) {
      log.info("Source defined for " + timerName + " all is well");
      timers.put(timerName, new TileDBMetricsTimer(source, timerName));
    } else {
      log.error(
          "Source was null or not defined for "
              + timerName
              + "!! Timer will not report metrics properly!");
      timers.put(timerName, new SimpleTimer());
    }
  }

  public void updateTaskMetrics(long recordCount, long sizeInBytes) {
    if (inputMetrics != null) {
      inputMetrics.incBytesRead(sizeInBytes);
      inputMetrics.incRecordsRead(recordCount);
    }
  }

  @Override
  public Long finish(String timerName) {
    if (timers.containsKey(timerName)) {
      return timers.get(timerName).stopTimer();
    }

    SparkEnv.get().metricsSystem().report();
    ;
    return null;
  }
}
