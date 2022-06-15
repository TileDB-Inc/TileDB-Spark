package io.tiledb.spark;

import static io.tiledb.java.api.ArrayType.TILEDB_DENSE;
import static io.tiledb.java.api.Layout.TILEDB_ROW_MAJOR;
import static io.tiledb.java.api.QueryType.TILEDB_WRITE;

import io.tiledb.java.api.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.spark.sql.*;
import org.junit.*;

public class TimeTravellingTest extends SharedJavaSparkSession {

  private static Context ctx;
  private String arrayURI;

  @Before
  public void setup() throws Exception {
    ctx = new Context();
    arrayURI = "time_travelling_test_array";
    deleteArrays();
  }

  @After
  public void delete() throws Exception {
    deleteArrays();
    ctx.close();
  }

  private void deleteArrays() throws TileDBError {
    if (Files.exists(Paths.get(arrayURI))) {
      TileDBObject.remove(ctx, arrayURI);
    }
  }

  @Test
  public void testTimeTravelling() throws Exception {
    // create array
    arrayCreate();
    // updates
    arrayWrite(10, new int[] {1, 2, 3, 4});
    arrayWrite(20, new int[] {101, 102, 103, 104});
    arrayWrite(30, new int[] {201, 202, 203, 204});
    // consolidate
    Array.consolidate(ctx, arrayURI);
    // verify consolidation
    arrayRead();
    // check if vacuuming breaks time travelling
    Array.vacuum(ctx, arrayURI);
    arrayReadAfterVacuuming();
  }

  private void arrayReadAfterVacuuming() {
    // Test first time interval. The values should be the fill values because time travelling is not
    // possible after vacuuming.
    Dataset<Row> dfRead = load("0", "10");
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();

    // 1st row
    Row row = rows.get(0);
    Assert.assertEquals(1, row.getInt(0));
    Assert.assertEquals(-2147483648, row.getInt(1));

    // 2nd row
    row = rows.get(1);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertEquals(-2147483648, row.getInt(1));

    // 3nd row
    row = rows.get(2);
    Assert.assertEquals(3, row.getInt(0));
    Assert.assertEquals(-2147483648, row.getInt(1));

    // 4th row
    row = rows.get(3);
    Assert.assertEquals(4, row.getInt(0));
    Assert.assertEquals(-2147483648, row.getInt(1));
  }

  private void arrayRead() throws Exception {
    // Test first time interval
    Dataset<Row> dfRead = load("0", "10");
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();

    // 1st row
    Row row = rows.get(0);
    Assert.assertEquals(1, row.getInt(0));
    Assert.assertEquals(1, row.getInt(1));

    // 2nd row
    row = rows.get(1);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertEquals(2, row.getInt(1));

    // 3nd row
    row = rows.get(2);
    Assert.assertEquals(3, row.getInt(0));
    Assert.assertEquals(3, row.getInt(1));

    // 4th row
    row = rows.get(3);
    Assert.assertEquals(4, row.getInt(0));
    Assert.assertEquals(4, row.getInt(1));

    // Test second time interval
    dfRead = load("10", "20");
    dfRead.createOrReplaceTempView("tmp");
    rows = session().sql("SELECT * FROM tmp").collectAsList();

    // 1st row
    row = rows.get(0);
    Assert.assertEquals(1, row.getInt(0));
    Assert.assertEquals(101, row.getInt(1));

    // 2nd row
    row = rows.get(1);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertEquals(102, row.getInt(1));

    // 3nd row
    row = rows.get(2);
    Assert.assertEquals(3, row.getInt(0));
    Assert.assertEquals(103, row.getInt(1));

    // 4th row
    row = rows.get(3);
    Assert.assertEquals(4, row.getInt(0));
    Assert.assertEquals(104, row.getInt(1));

    // Test third time interval
    dfRead = load("20", "30");
    dfRead.createOrReplaceTempView("tmp");
    rows = session().sql("SELECT * FROM tmp").collectAsList();

    // 1st row
    row = rows.get(0);
    Assert.assertEquals(1, row.getInt(0));
    Assert.assertEquals(201, row.getInt(1));

    // 2nd row
    row = rows.get(1);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertEquals(202, row.getInt(1));

    // 3nd row
    row = rows.get(2);
    Assert.assertEquals(3, row.getInt(0));
    Assert.assertEquals(203, row.getInt(1));

    // 4th row
    row = rows.get(3);
    Assert.assertEquals(4, row.getInt(0));
    Assert.assertEquals(204, row.getInt(1));
  }

  private Dataset<Row> load(String timestamp_start, String timestamp_end) {
    return session()
        .read()
        .format("io.tiledb.spark")
        .option("timestamp_start", timestamp_start)
        .option("timestamp_end", timestamp_end)
        .load(arrayURI);
  }

  public void arrayCreate() throws Exception {

    // Create getDimensions
    Dimension<Integer> dim =
        new Dimension<Integer>(ctx, "rows", Integer.class, new Pair<Integer, Integer>(1, 4), 2);

    // Create and set getDomain
    Domain domain = new Domain(ctx);
    domain.addDimension(dim);

    // Create and add getAttributes
    Attribute a = new Attribute(ctx, "a", Integer.class);

    ArraySchema schema = new ArraySchema(ctx, TILEDB_DENSE);
    schema.setTileOrder(TILEDB_ROW_MAJOR);
    schema.setCellOrder(TILEDB_ROW_MAJOR);
    schema.setDomain(domain);
    schema.addAttribute(a);

    Array.create(arrayURI, schema);
  }

  public void arrayWrite(long timestamp, int[] dataArray) throws Exception {
    // Prepare cell buffers
    NativeArray data = new NativeArray(ctx, dataArray, Integer.class);

    NativeArray subarray = new NativeArray(ctx, new int[] {1, 4}, Integer.class);

    // Create query
    Array array = new Array(ctx, arrayURI, TILEDB_WRITE, BigInteger.valueOf(timestamp));
    Query query = new Query(array);
    query.setLayout(TILEDB_ROW_MAJOR);
    query.setBuffer("a", data);
    query.setSubarray(subarray);
    // Submit query
    query.submit();
    query.close();
    array.close();
  }
}
