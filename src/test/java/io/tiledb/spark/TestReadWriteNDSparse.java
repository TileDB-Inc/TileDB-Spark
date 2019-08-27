package io.tiledb.spark;

import io.tiledb.java.api.Array;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.TileDBObject;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.*;

public class TestReadWriteNDSparse extends SharedJavaSparkSession {

  private Context ctx;
  private String arrayURI = "read_write";

  @Before
  public void setup() throws Exception {
    ctx = new Context();
    if (Array.exists(ctx, arrayURI)) {
      TileDBObject.remove(ctx, arrayURI);
    }
  }

  @After
  public void teardown() throws Exception {
    if (Array.exists(ctx, arrayURI)) {
      TileDBObject.remove(ctx, arrayURI);
    }
    ctx.close();
  }

  public String writeArrayURI() {
    return arrayURI;
  }

  private String testArrayURIString(String arrayName) {
    Path arraysPath = Paths.get("src", "test", "resources", "data", arrayName);
    return "file://".concat(arraysPath.toAbsolutePath().toString());
  }

  @Test(expected = RuntimeException.class)
  public void testReadWriteExistingErrors() {
    String writeArrayURI = writeArrayURI();
    Dataset<Row> dfReadFirst =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("uri", testArrayURIString("quickstart_sparse_array"))
            .load();
    dfReadFirst.show();
    dfReadFirst
        .write()
        .format("io.tiledb.spark")
        .option("uri", testArrayURIString("quickstart_sparse_array"))
        .option("schema.dim.0", "rows")
        .option("schema.dim.1", "cols")
        .mode(SaveMode.ErrorIfExists)
        .save();
  }

  @Test
  public void testReadWriteQuickStartSparse() {
    String writeArrayURI = writeArrayURI();
    Dataset<Row> dfReadFirst =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("uri", testArrayURIString("quickstart_sparse_array"))
            .load();
    dfReadFirst.show();
    dfReadFirst
        .write()
        .format("io.tiledb.spark")
        .option("uri", writeArrayURI)
        .option("schema.dim.0", "rows")
        .option("schema.dim.1", "cols")
        .mode(SaveMode.ErrorIfExists)
        .save();

    Dataset<Row> dfRead =
        session().read().format("io.tiledb.spark").option("uri", writeArrayURI).load();

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
