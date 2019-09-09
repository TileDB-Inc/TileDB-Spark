package io.tiledb.spark;

import io.tiledb.libtiledb.NativeLibLoader;
import org.apache.spark.sql.SparkSession;

public class SharedJavaSparkSession extends SharedJavaSparkContext {

  static transient SparkSession sparkSession;

  @Override
  public void runBeforeHook() {
    super.runBeforeHook();
    sparkSession = SparkSession.builder().config(conf()).getOrCreate();
    new NativeLibLoader();
  }

  public SparkSession session() {
    return sparkSession;
  }
}
