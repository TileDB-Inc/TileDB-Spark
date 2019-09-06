package org.apache.spark.metrics;

import static org.apache.spark.metrics.TileDBMetricsSource.sourceName;

import java.util.Optional;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.metrics.source.Source;
import scala.Option;
import scala.collection.Seq;

/**
 * Abstract class for handling metrics. Main function is the getSource which fetches the
 * TileDBMetricSource from the task or env contexts
 */
public abstract class MetricsUpdater implements MetricsUpdate {

  /**
   * Fetch a TileDBMetricSource from the task or evn context. If we don't find a TileDBMetricSource
   * this means that tiledb metrics are not enabled
   *
   * @param task task or null if not executor
   * @return TileDBMetricSource if found
   */
  Optional<TileDBMetricsSource> getSource(TaskContext task) {
    if (task != null) {
      Option<Source> source = task.getMetricsSources(sourceName).headOption();
      if (source.isDefined()) {
        return Optional.of((TileDBMetricsSource) source.get());
      }
    } else {
      SparkEnv env = SparkEnv.get();
      Seq<Source> sources = env.metricsSystem().getSourcesByName(sourceName);
      if (sources.length() > 0) return Optional.of((TileDBMetricsSource) sources.head());
    }
    return Optional.empty();
  }

  @Override
  public Long finish(String timerName) {
    return null;
  }
}
