package org.apache.spark.metrics;

import static org.apache.spark.metrics.TileDBMetricsSource.sourceName;

import java.util.Optional;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.metrics.source.Source;
import scala.Option;
import scala.collection.Seq;

public abstract class MetricsUpdater implements MetricsUpdate {

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
