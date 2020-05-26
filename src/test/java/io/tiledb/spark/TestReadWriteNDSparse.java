package io.tiledb.spark;

import io.tiledb.java.api.Array;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.TileDBObject;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
    Path arraysPath = Paths.get("src", "test", "resources", "data", "1.6", arrayName);
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
        .option("schema.dim.0.name", "rows")
        .option("schema.dim.0.min", 1)
        .option("schema.dim.0.max", 2)
        .option("schema.dim.0.extent", 2)
        .option("schema.dim.1.name", "cols")
        .option("schema.dim.1.min", 1)
        .option("schema.dim.1.max", 4)
        .option("schema.dim.1.extent", 4)
        .option("schema.attr.a.filter_list", "(byteshuffle, -1), (gzip, -10)")
        .option("schema.cell_order", "row-major")
        .option("schema.tile_order", "row-major")
        .option("schema.capacity", 3)
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

  @Test
  public void testWriteDupsLong() {
    StructField[] structFields =
        new StructField[] {
          new StructField("d1", DataTypes.LongType, false, Metadata.empty()),
        };

    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1l));
    rows.add(RowFactory.create(2l));
    rows.add(RowFactory.create(3l));
    rows.add(RowFactory.create(3l));
    StructType structType = new StructType(structFields);
    Dataset<Row> df =
        session()
            .createDataFrame(rows, structType)
            .withColumn("id", functions.monotonically_increasing_id())
            .repartition(1);

    df.show();

    DataFrameWriter<Row> writer =
        df.write()
            .format("io.tiledb.spark")
            .option("uri", arrayURI)
            .option("schema.dim.0.name", "d1")
            .option("schema.dim.0.extent", 2)
            .option("schema.cell_order", "row-major")
            .option("schema.tile_order", "row-major")
            .option("schema.capacity", 3)
            .mode(SaveMode.ErrorIfExists);

    try {
      writer.save();
      Assert.fail(
          "Duplicate dimensions should not be allowed if set_allows_dups parameter is not set.");
    } catch (Exception e) {
    }

    writer.option("schema.set_allows_dups", true).save();
  }

  @Test
  public void testWriteDupsDouble() {
    StructField[] structFields =
        new StructField[] {
          new StructField("d1", DataTypes.DoubleType, false, Metadata.empty()),
        };

    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1d));
    rows.add(RowFactory.create(2d));
    rows.add(RowFactory.create(3d));
    rows.add(RowFactory.create(3d));
    StructType structType = new StructType(structFields);
    Dataset<Row> df =
        session()
            .createDataFrame(rows, structType)
            .withColumn("id", functions.monotonically_increasing_id())
            .repartition(1);

    df.show();

    DataFrameWriter<Row> writer =
        df.write()
            .format("io.tiledb.spark")
            .option("uri", arrayURI)
            .option("schema.dim.0.name", "d1")
            .option("schema.dim.0.extent", 2)
            .option("schema.cell_order", "row-major")
            .option("schema.tile_order", "row-major")
            .option("schema.capacity", 3)
            .mode(SaveMode.ErrorIfExists);

    try {
      writer.save();
      Assert.fail(
          "Duplicate dimensions should not be allowed if set_allows_dups parameter is not set.");
    } catch (Exception e) {
    }

    writer.option("schema.set_allows_dups", true).save();
  }
}
