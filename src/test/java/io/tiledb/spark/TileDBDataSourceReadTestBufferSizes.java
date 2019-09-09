package io.tiledb.spark;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import io.tiledb.libtiledb.NativeLibLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TileDBDataSourceReadTestBufferSizes extends SharedJavaSparkSession {

  @Before
  public void before() {
    new NativeLibLoader();
  }

  private String testArrayURIString(String arrayName) {
    Path arraysPath = Paths.get("src", "test", "resources", "data", "1.6", arrayName);
    return "file://".concat(arraysPath.toAbsolutePath().toString());
  }

  /**
   * This test uses an 8 byte buffer which is enough for 1 cell (2 coordinates at 4 byte a piece) It
   * validates that incomplete queries are working
   */
  @Test
  public void testQuickStartSparseWith8ByteBuffer() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("uri", testArrayURIString("quickstart_sparse_array"))
            .option("read_buffer_size", 8)
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

  /**
   * This test uses an 4 byte buffer which is too small to hold any results, buffer reallocation is
   * disabled so we should get an exception The test is currently disabled because catching the
   * exception is not working as expected because its a different thread throwing the exception
   * Neither try/catch nor having the test marked for expected exception
   */
  /* @Test
  public void testQuickStartSparseWith4ByteBufferIncompleteReallocDisabled() {
    try {
      Dataset<Row> dfRead =
          session()
              .read()
              .format("io.tiledb.spark")
              .option("uri", testArrayURIString("quickstart_sparse_array"))
              .option("read_buffer_size", 4)
              .option("allow_read_buffer_realloc", false)
              .load();
      dfRead.createOrReplaceTempView("tmp");
      List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();
    } catch (RuntimeException e) {
      Assert.assertThat(
          e.getMessage(),
          Is.is(
              "Incomplete query with no more records means the buffers are too small but allow_read_buffer_realloc is set to false!"));
    }
  }*/

  /**
   * This test uses an 4 byte buffer which is too small to hold any results, buffer reallocation is
   * disabled so we should get an exception
   */
  @Test
  public void testQuickStartSparseWith4ByteBufferIncomplete() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("uri", testArrayURIString("quickstart_sparse_array"))
            .option("read_buffer_size", 4)
            .option("allow_read_buffer_realloc", true)
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
