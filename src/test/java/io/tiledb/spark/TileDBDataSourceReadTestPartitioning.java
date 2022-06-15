package io.tiledb.spark;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

public class TileDBDataSourceReadTestPartitioning extends SharedJavaSparkSession {

  private String testArrayURIString(String arrayName) {
    Path arraysPath = Paths.get("src", "test", "resources", "data", "1.6", arrayName);
    return "file://".concat(arraysPath.toAbsolutePath().toString());
  }

  @Test
  /** */
  public void testQuickStartSparseLargePartitioning() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("partition_count", 11)
            .load(testArrayURIString("sparse_large_dimension_1_4000"));
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp ORDER BY rows, cols").collectAsList();
    Assert.assertEquals(8, rows.size());
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
    // A[100, 10] == 4
    row = rows.get(3);
    Assert.assertEquals(100, row.getInt(0));
    Assert.assertEquals(10, row.getInt(1));
    Assert.assertEquals(4, row.getInt(2));
    // A[110, 2100] == 5
    row = rows.get(4);
    Assert.assertEquals(110, row.getInt(0));
    Assert.assertEquals(2000, row.getInt(1));
    Assert.assertEquals(5, row.getInt(2));
    // A[1000, 2100] == 6
    row = rows.get(5);
    Assert.assertEquals(1000, row.getInt(0));
    Assert.assertEquals(2100, row.getInt(1));
    Assert.assertEquals(6, row.getInt(2));
    // A[3000, 3300] == 7
    row = rows.get(6);
    Assert.assertEquals(3000, row.getInt(0));
    Assert.assertEquals(3300, row.getInt(1));
    Assert.assertEquals(7, row.getInt(2));
    // A[3500, 1300] == 8
    row = rows.get(7);
    Assert.assertEquals(3500, row.getInt(0));
    Assert.assertEquals(1300, row.getInt(1));
    Assert.assertEquals(8, row.getInt(2));
    return;
  }

  @Test
  /** */
  public void testQuickStartSparseLargePartitioningMultipleRanges() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("partition_count", 11)
            .load(testArrayURIString("sparse_large_dimension_1_4000"));
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows =
        session()
            .sql(
                "SELECT * FROM tmp WHERE rows between 1 and 100 OR rows between 105 and 111 OR rows between 350 and 3900 OR rows between 200 and 325 ORDER BY rows, cols")
            .collectAsList();
    Assert.assertEquals(8, rows.size());
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
    // A[100, 10] == 4
    row = rows.get(3);
    Assert.assertEquals(100, row.getInt(0));
    Assert.assertEquals(10, row.getInt(1));
    Assert.assertEquals(4, row.getInt(2));
    // A[110, 2100] == 5
    row = rows.get(4);
    Assert.assertEquals(110, row.getInt(0));
    Assert.assertEquals(2000, row.getInt(1));
    Assert.assertEquals(5, row.getInt(2));
    // A[1000, 2100] == 6
    row = rows.get(5);
    Assert.assertEquals(1000, row.getInt(0));
    Assert.assertEquals(2100, row.getInt(1));
    Assert.assertEquals(6, row.getInt(2));
    // A[3000, 3300] == 7
    row = rows.get(6);
    Assert.assertEquals(3000, row.getInt(0));
    Assert.assertEquals(3300, row.getInt(1));
    Assert.assertEquals(7, row.getInt(2));
    // A[3500, 1300] == 8
    row = rows.get(7);
    Assert.assertEquals(3500, row.getInt(0));
    Assert.assertEquals(1300, row.getInt(1));
    Assert.assertEquals(8, row.getInt(2));
    return;
  }

  @Test
  /** */
  public void testQuickStartSparseSinglePartition() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("partition_count", 1)
            .load(testArrayURIString("sparse_large_dimension_1_4000"));
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();
    Assert.assertEquals(8, rows.size());
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
    // A[100, 10] == 4
    row = rows.get(3);
    Assert.assertEquals(100, row.getInt(0));
    Assert.assertEquals(10, row.getInt(1));
    Assert.assertEquals(4, row.getInt(2));
    // A[110, 2100] == 5
    row = rows.get(4);
    Assert.assertEquals(110, row.getInt(0));
    Assert.assertEquals(2000, row.getInt(1));
    Assert.assertEquals(5, row.getInt(2));
    // A[1000, 2100] == 6
    row = rows.get(5);
    Assert.assertEquals(1000, row.getInt(0));
    Assert.assertEquals(2100, row.getInt(1));
    Assert.assertEquals(6, row.getInt(2));
    // A[3000, 3300] == 7
    row = rows.get(6);
    Assert.assertEquals(3000, row.getInt(0));
    Assert.assertEquals(3300, row.getInt(1));
    Assert.assertEquals(7, row.getInt(2));
    // A[3500, 1300] == 8
    row = rows.get(7);
    Assert.assertEquals(3500, row.getInt(0));
    Assert.assertEquals(1300, row.getInt(1));
    Assert.assertEquals(8, row.getInt(2));
    return;
  }
}
