package io.tiledb.spark;

import static io.tiledb.java.api.ArrayType.TILEDB_DENSE;
import static io.tiledb.java.api.Layout.*;
import static io.tiledb.java.api.QueryType.TILEDB_WRITE;

import io.tiledb.java.api.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TileDBDataSourceReadTest extends SharedJavaSparkSession {
  private Context ctx;
  private String DENSE_ARRAY_URI = "dense";

  @Before
  public void setup() throws Exception {
    ctx = new Context();

    if (Files.exists(Paths.get(DENSE_ARRAY_URI))) TileDBObject.remove(ctx, DENSE_ARRAY_URI);
  }

  @After
  public void tearDown() throws Exception {
    if (Files.exists(Paths.get(DENSE_ARRAY_URI))) TileDBObject.remove(ctx, DENSE_ARRAY_URI);

    ctx.close();
  }

  private String testArrayURIString(String arrayName) {
    Path arraysPath = Paths.get("src", "test", "resources", "data", "1.6", arrayName);
    return "file://".concat(arraysPath.toAbsolutePath().toString());
  }

  public void denseArrayCreate() throws TileDBError {
    // Create getDimensions
    Dimension d1 = new Dimension(ctx, "rows", Integer.class, new Pair(1, 4), 2);
    Dimension d2 = new Dimension(ctx, "cols", Integer.class, new Pair(1, 2), 2);

    // Create and set getDomain
    Domain domain = new Domain(ctx);
    domain.addDimension(d1);
    domain.addDimension(d2);

    // Create and add getAttributes
    Attribute a1 = new Attribute(ctx, "vals", Integer.class);
    a1.setFilterList(new FilterList(ctx).addFilter(new LZ4Filter(ctx)));

    ArraySchema schema = new ArraySchema(ctx, TILEDB_DENSE);
    schema.setTileOrder(TILEDB_ROW_MAJOR);
    schema.setCellOrder(TILEDB_ROW_MAJOR);
    schema.setDomain(domain);
    schema.addAttribute(a1);

    schema.check();

    Array.create(DENSE_ARRAY_URI, schema);
  }

  public void denseArrayWrite() throws TileDBError {
    Array my_dense_array = new Array(ctx, DENSE_ARRAY_URI, TILEDB_WRITE);

    NativeArray vals_data = new NativeArray(ctx, new int[] {1, 3, 5, 7, 2, 4, 6, 8}, Integer.class);

    // Create query
    try (Query query = new Query(my_dense_array, TILEDB_WRITE)) {
      query.setLayout(TILEDB_GLOBAL_ORDER).setBuffer("vals", vals_data);
      query.submit();
      query.finalizeQuery();
    }

    my_dense_array.close();
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
  public void testQuickStartDenseRowMajor() throws TileDBError {
    denseArrayCreate();
    denseArrayWrite();

    for (String order : new String[] {"row-major", "TILEDB_ROW_MAJOR"}) {
      Dataset<Row> dfRead =
          session()
              .read()
              .format("io.tiledb.spark")
              .option("uri", DENSE_ARRAY_URI)
              .option("order", order)
              .option("partition_count", 1)
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
      int[] expectedVals = new int[] {1, 3, 5, 7, 2, 4, 6, 8};
      for (int i = 0; i < rows.size(); i++) {
        Assert.assertEquals(expectedVals[i], rows.get(i).getInt(2));
      }
    }
  }

  @Test
  public void testExampleVarlenArray() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("uri", testArrayURIString("variable_length_array"))
            .option("partition_count", 1)
            .load();
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT a1 FROM tmp").collectAsList();
    String[] expected =
        new String[] {
          "a", "bb", "ccc", "dd", "eee", "f", "g", "hhh", "i", "jjj", "kk", "l", "m", "n", "oo", "p"
        };
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(expected[i], rows.get(i).getString(0));
    }
  }

  @Test
  public void testQuickStartDenseColMajor() throws TileDBError {
    denseArrayCreate();
    denseArrayWrite();

    for (String order : new String[] {"col-major", "TILEDB_COL_MAJOR"}) {
      Dataset<Row> dfRead =
          session()
              .read()
              .format("io.tiledb.spark")
              .option("uri", DENSE_ARRAY_URI)
              .option("order", order)
              .option("partition_count", 1)
              .load();
      dfRead.createOrReplaceTempView("tmp");
      dfRead.show();
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
      int[] expectedVals = new int[] {1, 5, 2, 6, 3, 7, 4, 8};
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

  @Test
  public void testQuickStartSparseFilterBetween() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("uri", testArrayURIString("quickstart_sparse_array"))
            .load();
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows =
        session()
            .sql(
                "SELECT * FROM tmp WHERE rows between 1 and 3 AND (cols = 1 OR cols = 3 OR cols = 4) ORDER BY rows, cols")
            .collectAsList();
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
