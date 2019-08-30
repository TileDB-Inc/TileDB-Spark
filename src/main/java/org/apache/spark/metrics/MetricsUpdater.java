package org.apache.spark.metrics;

import java.util.Optional;
import org.apache.spark.TaskContext;
import org.apache.spark.metrics.source.Source;
import scala.Option;

public abstract class MetricsUpdater implements MetricsUpdate {
  private String sourceName = "tiledb";

  Optional<TileDBMetricsSource> getSource(TaskContext task) {
    if (task != null) {
      Option<Source> source = task.getMetricsSources(sourceName).headOption();
      if (source.isDefined()) {
        return Optional.of((TileDBMetricsSource) source.get());
      }
    }
    return Optional.empty();
  }

  @Override
  public Long finish(String timerName) {
    return null;
  }
}
