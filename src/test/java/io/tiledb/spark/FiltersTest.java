package io.tiledb.spark;

import io.tiledb.java.api.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FiltersTest extends SharedJavaSparkSession {
  private static Context ctx;
  private String[] filters;
  private String sparseURI;

  private String writeArrayURI;

  @Before
  public void setup() throws Exception {
    ctx = new Context();
    sparseURI = "sparse_array";
    writeArrayURI = "temp";
    filters =
        new String[] {
          "NONE",
          "GZIP, -1",
          "ZSTD, -1",
          "LZ4, -1",
          "RLE, -1",
          "BZIP2, -1",
          "DOUBLE_DELTA, -1",
          "BIT_WIDTH_REDUCTION, -1",
          "BITSHUFFLE, -1",
          "BYTESHUFFLE, -1",
          "POSITIVE_DELTA, -1",
          "SCALE_FLOAT, 1.0, 1.0, 8"
        };
    deleteArrays();
  }

  @After
  public void delete() throws Exception {
    deleteArrays();
    ctx.close();
  }

  private void deleteArrays() throws TileDBError {
    if (Files.exists(Paths.get(sparseURI))) {
      TileDBObject.remove(ctx, sparseURI);
    }
    if (Files.exists(Paths.get(writeArrayURI))) {
      TileDBObject.remove(ctx, writeArrayURI);
    }
  }

  @Test
  public void filtersTest() {
    for (String filter : filters) {
      Dataset<Row> dfReadFirst = TestUtil.createSparseDataset(session());
      // dfReadFirst.show();
      dfReadFirst
          .write()
          .format("io.tiledb.spark")
          .option("schema.dim.0.name", "d1")
          .option("schema.dim.0.min", 1)
          .option("schema.dim.0.max", 8)
          .option("schema.dim.0.extent", 2)
          .option("schema.attr.a1.filter_list", filter)
          .option("schema.cell_order", "row-major")
          .option("schema.tile_order", "row-major")
          .option("schema.capacity", 3)
          .mode("overwrite")
          .save(writeArrayURI);

      Dataset<Row> dfRead = session().read().format("io.tiledb.spark").load(writeArrayURI);
      dfRead.createOrReplaceTempView("tmp");
      List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();
      Assert.assertEquals(5, rows.size());

      Row row = rows.get(0);
      Assert.assertEquals(1, row.get(0));
      Assert.assertNull(row.get(1));
      Assert.assertEquals("a", row.get(2));

      row = rows.get(1);
      Assert.assertEquals(2, row.get(0));
      Assert.assertNull(row.get(1));
      Assert.assertEquals("b", row.get(2));

      row = rows.get(2);
      Assert.assertEquals(3, row.get(0));
      Assert.assertNull(row.get(1));
      Assert.assertEquals("c", row.get(2));

      row = rows.get(3);
      Assert.assertEquals(4, row.get(0));
      Assert.assertEquals(4.0f, row.get(1));
      Assert.assertNull(row.get(2));

      row = rows.get(4);
      Assert.assertEquals(5, row.get(0));
      Assert.assertEquals(5.0f, row.get(1));
      Assert.assertNull(row.get(2));
    }
  }
}
