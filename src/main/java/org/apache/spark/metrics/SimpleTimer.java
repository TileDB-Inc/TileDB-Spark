package org.apache.spark.metrics;

/** Simple timer class for use when metrics are not loaded */
public class SimpleTimer implements Timer {
  private long startTime = System.nanoTime();

  /**
   * End the timer
   *
   * @return duration
   */
  public Long stopTimer() {
    return System.nanoTime() - startTime;
  }
}
