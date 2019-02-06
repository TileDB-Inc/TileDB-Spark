package io.tiledb.spark;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
    session().sql("SELECT * FROM tmp").show();
    return;
  }

  @Test
  public void testQuickStartDense() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("uri", testArrayURIString("writing_dense_global_array"))
            .load();
    dfRead.createOrReplaceTempView("tmp");
    session().sql("SELECT rows, cols FROM tmp").show();
    return;
  }
}
