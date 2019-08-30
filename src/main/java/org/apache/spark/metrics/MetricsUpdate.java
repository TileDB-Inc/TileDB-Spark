package org.apache.spark.metrics;

public interface MetricsUpdate {
  Long finish(String name);
}
