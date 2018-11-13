package io.tiledb.spark;

import org.junit.Assert;
import org.junit.Test;

public class SimpleSparkSessionTest extends SharedJavaSparkSession {

  @Test
  public void testDatabaseDefault() {
    Assert.assertEquals(session().catalog().currentDatabase(), "default");
  }
}
