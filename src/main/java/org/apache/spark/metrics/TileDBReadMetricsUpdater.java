package org.apache.spark.metrics;

import static org.apache.spark.metrics.TileDBMetricsSource.sourceName;

import io.tiledb.spark.TileDBDataSourceOptions;
import java.util.HashMap;
import java.util.Optional;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.InputMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.metrics.source.Source;
import scala.collection.Seq;

public class TileDBReadMetricsUpdater extends MetricsUpdater {
  //  private Timer timer;
  private InputMetrics inputMetrics = null;
  private TileDBMetricsSource source = null;
  private HashMap<String, Timer> timers;

  public TileDBReadMetricsUpdater(TaskContext task, TileDBDataSourceOptions options) {

    Optional<TileDBMetricsSource> tmp = getSource(task);
    if (tmp.isPresent()) {
      source = tmp.get();
    } else {
      SparkEnv env = SparkEnv.get();
      Seq<Source> sources = env.metricsSystem().getSourcesByName(sourceName);
      if (sources.length() > 1) source = (TileDBMetricsSource) sources.head();
    }
    timers = new HashMap<>();

    if (options.getTaskMetricsEnabled() && task != null) {
      TaskMetrics tm = task.taskMetrics();
      inputMetrics = tm.inputMetrics();
    }
  }

  public synchronized void startTimer(String timerName) {
    if (source != null) {
      timers.put(timerName, new TileDBMetricsTimer(source, timerName));
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
