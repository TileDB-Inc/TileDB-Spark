package io.tiledb.spark;

import io.tiledb.java.api.Array;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.TileDBError;
import io.tiledb.java.api.TileDBObject;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.spark.sql.*;
import org.junit.*;

public class TestReadWriteNDDense extends SharedJavaSparkSession {

  private Context ctx;
  private String arrayURI = "read_write_dense";

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
    Path arraysPath = Paths.get("src", "test", "resources", "data", "1.6", arrayName);
    return "file://".concat(arraysPath.toAbsolutePath().toString());
  }

  @Test
  public void testReadWriteDense() throws TileDBError {
    String writeArrayURI = writeArrayURI();
    Dataset<Row> dfReadFirst =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("uri", testArrayURIString("writing_dense_global_array"))
            .load();
    // Array will be written as Sparse. TileDB-Spark does not support dense writes.
    dfReadFirst
        .write()
        .format("io.tiledb.spark")
        .option("uri", arrayURI)
        .option("schema.dim.0.name", "rows")
        .option("schema.dim.0.min", 1)
        .option("schema.dim.0.max", 4)
        .option("schema.dim.0.extent", 2)
        .option("schema.dim.1.name", "cols")
        .option("schema.dim.1.min", 1)
        .option("schema.dim.1.max", 2)
        .option("schema.dim.1.extent", 2)
        .option("schema.attr.a.filter_list", "(byteshuffle, -1), (gzip, -10)")
        .option("schema.cell_order", "row-major")
        .option("schema.tile_order", "row-major")
        .option("schema.capacity", 3)
        .mode(SaveMode.ErrorIfExists)
        .save();

    Dataset<Row> dfRead = session().read().format("io.tiledb.spark").option("uri", arrayURI).load();

    // check values
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();
    Assert.assertEquals(8, rows.size());

    Row row = rows.get(0);
    Assert.assertEquals(1, row.getInt(0));
    Assert.assertEquals(1, row.getInt(1));
    Assert.assertEquals(1, row.getInt(2));

    row = rows.get(1);
    Assert.assertEquals(1, row.getInt(0));
    Assert.assertEquals(2, row.getInt(1));
    Assert.assertEquals(2, row.getInt(2));

    row = rows.get(2);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertEquals(1, row.getInt(1));
    Assert.assertEquals(3, row.getInt(2));

    row = rows.get(3);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertEquals(2, row.getInt(1));
    Assert.assertEquals(4, row.getInt(2));

    row = rows.get(4);
    Assert.assertEquals(3, row.getInt(0));
    Assert.assertEquals(1, row.getInt(1));
    Assert.assertEquals(5, row.getInt(2));

    row = rows.get(5);
    Assert.assertEquals(3, row.getInt(0));
    Assert.assertEquals(2, row.getInt(1));
    Assert.assertEquals(6, row.getInt(2));

    row = rows.get(6);
    Assert.assertEquals(4, row.getInt(0));
    Assert.assertEquals(1, row.getInt(1));
    Assert.assertEquals(7, row.getInt(2));

    row = rows.get(7);
    Assert.assertEquals(4, row.getInt(0));
    Assert.assertEquals(2, row.getInt(1));
    Assert.assertEquals(8, row.getInt(2));
  }
}
