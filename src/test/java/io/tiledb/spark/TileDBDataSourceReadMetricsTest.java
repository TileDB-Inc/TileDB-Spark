package io.tiledb.spark;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

public class TileDBDataSourceReadMetricsTest extends SharedJavaSparkSession {

  private String metricConfigFile() {
    return Paths.get("src", "test", "resources", "metrics.properties").toString();
  }

  private String testArrayURIString(String arrayName) {
    Path arraysPath = Paths.get("src", "test", "resources", "data", "1.6", arrayName);
    return "file://".concat(arraysPath.toAbsolutePath().toString());
  }

  @Test
  public void testQuickStartSparseMetrics() {

    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("uri", testArrayURIString("quickstart_sparse_array"))
            .option("spark.metrics.conf", metricConfigFile())
            .load();
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();
    Assert.assertEquals(3, rows.size());
    // A[1, 1] == 1
    Row row = rows.get(0);
    Assert.assertEquals(1, row.getInt(0));
    Assert.assertEquals(1, row.getInt(1));
    Assert.assertEquals(1, row.getInt(2));
    // A[2, 3] == 3
    row = rows.get(1);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertEquals(3, row.getInt(1));
    Assert.assertEquals(3, row.getInt(2));
    // A[2, 4] == 2
    row = rows.get(2);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertEquals(4, row.getInt(1));
    Assert.assertEquals(2, row.getInt(2));
    return;
  }
}
