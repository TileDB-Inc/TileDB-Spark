package org.apache.spark.metrics;

public class SimpleTimer implements Timer {
  private long startTime = System.nanoTime();

  public Long stopTimer() {
    return System.nanoTime() - startTime;
  }
}
