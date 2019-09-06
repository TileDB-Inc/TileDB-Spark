package org.apache.spark.metrics;

/**
 * This is an interface so we can have multiple implementation of metrics that "finish". Currently
 * we just use read metrics but eventually will support write metrics also.
 */
public interface MetricsUpdate {
  Long finish(String name);
}
