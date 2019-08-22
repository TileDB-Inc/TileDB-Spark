package io.tiledb.spark;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

public class TileDBDataSourceReadTest extends SharedJavaSparkSession {

  private String testArrayURIString(String arrayName) {
    Path arraysPath = Paths.get("src", "test", "resources", "data", arrayName);
    return "file://".concat(arraysPath.toAbsolutePath().toString());
  }

  @Test
  public void testQuickStartSparse() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("uri", testArrayURIString("quickstart_sparse_array"))
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

  @Test
  public void testQuickStartDenseRowMajor() {
    for (String order : new String[] {"row-major", "TILEDB_ROW_MAJOR"}) {
      Dataset<Row> dfRead =
          session()
              .read()
              .format("io.tiledb.spark")
              .option("uri", testArrayURIString("writing_dense_global_array"))
              .option("order", order)
              .load();
      dfRead.createOrReplaceTempView("tmp");
      List<Row> rows = dfRead.sqlContext().sql("SELECT * FROM tmp").collectAsList();
      int[] expectedRows = new int[] {1, 1, 2, 2, 3, 3, 4, 4};
      Assert.assertEquals(expectedRows.length, rows.size());
      for (int i = 0; i < rows.size(); i++) {
        Assert.assertEquals(expectedRows[i], rows.get(i).getInt(0));
      }
      int[] expectedCols = new int[] {1, 2, 1, 2, 1, 2, 1, 2};
      Assert.assertEquals(expectedCols.length, rows.size());
      for (int i = 0; i < rows.size(); i++) {
        Assert.assertEquals(expectedCols[i], rows.get(i).getInt(1));
      }
      int[] expectedVals = new int[] {1, 2, 3, 4, 5, 6, 7, 8};
      for (int i = 0; i < rows.size(); i++) {
        Assert.assertEquals(expectedVals[i], rows.get(i).getInt(2));
      }
    }
  }

  @Test
  public void testQuickStartDenseColMajor() {
    for (String order : new String[] {"col-major", "TILEDB_COL_MAJOR"}) {
      Dataset<Row> dfRead =
          session()
              .read()
              .format("io.tiledb.spark")
              .option("uri", testArrayURIString("writing_dense_global_array"))
              .option("order", order)
              .load();
      dfRead.createOrReplaceTempView("tmp");
      List<Row> rows = dfRead.sqlContext().sql("SELECT * FROM tmp").collectAsList();
      int[] expectedRows = new int[] {1, 2, 3, 4, 1, 2, 3, 4};
      Assert.assertEquals(expectedRows.length, rows.size());
      for (int i = 0; i < rows.size(); i++) {
        Assert.assertEquals(expectedRows[i], rows.get(i).getInt(0));
      }
      int[] expectedCols = new int[] {1, 1, 1, 1, 2, 2, 2, 2};
      Assert.assertEquals(expectedCols.length, rows.size());
      for (int i = 0; i < rows.size(); i++) {
        Assert.assertEquals(expectedCols[i], rows.get(i).getInt(1));
      }
      int[] expectedVals = new int[] {1, 3, 5, 7, 2, 4, 6, 8};
      for (int i = 0; i < rows.size(); i++) {
        Assert.assertEquals(expectedVals[i], rows.get(i).getInt(2));
      }
    }
  }

  @Test
  public void testQuickStartSparseFilterEqual() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("uri", testArrayURIString("quickstart_sparse_array"))
            .load();
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp WHERE rows = 1 and cols = 1").collectAsList();
    Assert.assertEquals(1, rows.size());
    // A[1, 1] == 1
    Row row = rows.get(0);
    Assert.assertEquals(1, row.getInt(0));
    Assert.assertEquals(1, row.getInt(1));
    Assert.assertEquals(1, row.getInt(2));
    return;
  }

  @Test
  public void testQuickStartSparseFilterGreaterThan() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("uri", testArrayURIString("quickstart_sparse_array"))
            .load();
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp WHERE rows > 1").collectAsList();
    Assert.assertEquals(2, rows.size());
    // A[2, 3] == 3
    Row row = rows.get(0);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertEquals(3, row.getInt(1));
    Assert.assertEquals(3, row.getInt(2));
    // A[2, 4] == 2
    row = rows.get(1);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertEquals(4, row.getInt(1));
    Assert.assertEquals(2, row.getInt(2));
    return;
  }

  @Test
  public void testQuickStartSparseFilterGreaterThanEqual() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("uri", testArrayURIString("quickstart_sparse_array"))
            .load();
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp WHERE rows >= 2").collectAsList();
    Assert.assertEquals(2, rows.size());
    // A[2, 3] == 3
    Row row = rows.get(0);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertEquals(3, row.getInt(1));
    Assert.assertEquals(3, row.getInt(2));
    // A[2, 4] == 2
    row = rows.get(1);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertEquals(4, row.getInt(1));
    Assert.assertEquals(2, row.getInt(2));
    return;
  }

  @Test
  public void testQuickStartSparseFilterLessThan() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("uri", testArrayURIString("quickstart_sparse_array"))
            .load();
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp WHERE rows < 2").collectAsList();
    Assert.assertEquals(1, rows.size());
    // A[1, 1] == 1
    Row row = rows.get(0);
    Assert.assertEquals(1, row.getInt(0));
    Assert.assertEquals(1, row.getInt(1));
    Assert.assertEquals(1, row.getInt(2));
    return;
  }

  @Test
  public void testQuickStartSparseFilterLessThanEqual() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("uri", testArrayURIString("quickstart_sparse_array"))
            .load();
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows =
        session().sql("SELECT * FROM tmp WHERE rows <= 2 AND cols <= 3").collectAsList();
    Assert.assertEquals(2, rows.size());
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
    return;
  }

  @Test
  public void testQuickStartSparseFilterIn() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("uri", testArrayURIString("quickstart_sparse_array"))
            .load();
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows =
        session().sql("SELECT * FROM tmp WHERE rows IN (1, 2) AND cols IN (1,3)").collectAsList();
    Assert.assertEquals(2, rows.size());
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
    return;
  }
}
