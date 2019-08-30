package org.apache.spark.metrics;

import io.tiledb.spark.TileDBDataSourceOptions;
import java.util.HashMap;
import java.util.Optional;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.InputMetrics;
import org.apache.spark.executor.TaskMetrics;

public class TileDBReadMetricsUpdater extends MetricsUpdater {
  //  private Timer timer;
  private InputMetrics inputMetrics = null;
  private Optional<TileDBMetricsSource> source;
  private HashMap<String, Timer> timers;

  public TileDBReadMetricsUpdater(TaskContext task, TileDBDataSourceOptions options) {

    source = getSource(task);
    timers = new HashMap<>();

    if (options.getTaskMetricsEnabled() && task != null) {
      TaskMetrics tm = task.taskMetrics();
      inputMetrics = tm.inputMetrics();
    }
  }

  public synchronized void startTimer(String timerName) {
    if (source.isPresent()) {
      timers.put(timerName, new TileDBMetricsTimer(source.get(), timerName));
    } else {
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
    return null;
  }
}
