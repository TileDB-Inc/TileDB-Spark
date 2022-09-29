package io.tiledb.spark;

import static io.tiledb.java.api.ArrayType.TILEDB_DENSE;
import static io.tiledb.java.api.ArrayType.TILEDB_SPARSE;
import static io.tiledb.java.api.Layout.*;
import static io.tiledb.java.api.QueryType.TILEDB_WRITE;

import io.tiledb.java.api.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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
  private String SPARSE_ARRAY_URI = "sparse";

  @Before
  public void setup() throws Exception {
    ctx = new Context();
    if (Files.exists(Paths.get(DENSE_ARRAY_URI))) TileDBObject.remove(ctx, DENSE_ARRAY_URI);
    if (Files.exists(Paths.get(SPARSE_ARRAY_URI))) TileDBObject.remove(ctx, SPARSE_ARRAY_URI);
  }

  @After
  public void tearDown() throws Exception {
    if (Files.exists(Paths.get(DENSE_ARRAY_URI))) TileDBObject.remove(ctx, DENSE_ARRAY_URI);
    if (Files.exists(Paths.get(SPARSE_ARRAY_URI))) TileDBObject.remove(ctx, SPARSE_ARRAY_URI);

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
    schema.close();
    domain.close();
  }

  public void sparseHeterogeneousArrayCreate(List<Dimension> dimensions) throws TileDBError {
    // Create and set getDomain
    Domain domain = new Domain(ctx);

    for (Dimension dimension : dimensions) domain.addDimension(dimension);

    Attribute a1 = new Attribute(ctx, "a1", Integer.class);

    ArraySchema schema = new ArraySchema(ctx, TILEDB_SPARSE);
    schema.setTileOrder(TILEDB_ROW_MAJOR);
    schema.setCellOrder(TILEDB_ROW_MAJOR);
    schema.setDomain(domain);
    schema.addAttribute(a1);

    Array.create(SPARSE_ARRAY_URI, schema);
    domain.close();
    schema.close();
  }

  public void sparseHeterogeneousArrayCreate2A(List<Dimension> dimensions) throws TileDBError {
    // Create and set getDomain
    Domain domain = new Domain(ctx);

    for (Dimension dimension : dimensions) domain.addDimension(dimension);

    Attribute a1 = new Attribute(ctx, "a1", Integer.class);
    Attribute a2 = new Attribute(ctx, "a2", Integer.class);

    ArraySchema schema = new ArraySchema(ctx, TILEDB_SPARSE);
    schema.setTileOrder(TILEDB_ROW_MAJOR);
    schema.setCellOrder(TILEDB_ROW_MAJOR);
    schema.setDomain(domain);
    schema.addAttribute(a1);
    schema.addAttribute(a2);

    Array.create(SPARSE_ARRAY_URI, schema);
    domain.close();
    schema.close();
  }

  public void sparseHeterogeneousArrayWrite(List<Pair<String, Pair<NativeArray, NativeArray>>> data)
      throws TileDBError {
    // Create query
    Array array = new Array(ctx, SPARSE_ARRAY_URI, TILEDB_WRITE);
    Query query = new Query(array);
    query.setLayout(TILEDB_GLOBAL_ORDER);

    for (Pair<String, Pair<NativeArray, NativeArray>> pair : data) {
      // Not var-sized
      if (pair.getSecond().getSecond() == null) {
        query.setBuffer(pair.getFirst(), pair.getSecond().getFirst());
      } else {
        query.setBuffer(pair.getFirst(), pair.getSecond().getSecond(), pair.getSecond().getFirst());
      }
    }

    // Submit query
    query.submit();

    query.finalizeQuery();
    query.close();
    array.close();
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
            .load(testArrayURIString("quickstart_sparse_array"));
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
              .option("order", order)
              .option("partition_count", 1)
              .load(DENSE_ARRAY_URI);
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
  public void testHeterogeneousSparse1() throws TileDBError {
    List<Dimension> dimensions = new ArrayList<>();
    dimensions.add(new Dimension(ctx, "d1", Datatype.TILEDB_STRING_ASCII, null, null));
    dimensions.add(new Dimension(ctx, "d2", Datatype.TILEDB_INT32, new Pair(0, 100), 2));

    NativeArray d1_data =
        new NativeArray(ctx, "object1object2object3", Datatype.TILEDB_STRING_ASCII);
    NativeArray d1_off = new NativeArray(ctx, new long[] {0, 7, 14}, Datatype.TILEDB_UINT64);
    NativeArray d2_data = new NativeArray(ctx, new int[] {12, 40, 50}, Datatype.TILEDB_INT32);
    NativeArray a1_data = new NativeArray(ctx, new int[] {10, 23, 30}, Datatype.TILEDB_INT32);

    List<Pair<String, Pair<NativeArray, NativeArray>>> data = new ArrayList<>();
    data.add(new Pair<>("d1", new Pair<>(d1_data, d1_off)));
    data.add(new Pair<>("d2", new Pair<>(d2_data, null)));
    data.add(new Pair<>("a1", new Pair<>(a1_data, null)));

    sparseHeterogeneousArrayCreate(dimensions);
    sparseHeterogeneousArrayWrite(data);

    for (String order : new String[] {"row-major", "TILEDB_ROW_MAJOR"}) {
      Dataset<Row> dfRead =
          session()
              .read()
              .format("io.tiledb.spark")
              .option("order", order)
              .option("partition_count", 1)
              .load(SPARSE_ARRAY_URI);
      dfRead.createOrReplaceTempView("tmp");
      dfRead.show();
      List<Row> rows = dfRead.sqlContext().sql("SELECT * FROM tmp").collectAsList();

      String[] d1 = new String[] {"object1", "object2", "object3"};
      int[] d2 = new int[] {12, 40, 50};
      int[] a1 = new int[] {10, 23, 30};
      Assert.assertEquals(d1.length, rows.size());

      for (int i = 0; i < rows.size(); i++) {
        Assert.assertEquals(d1[i], rows.get(i).getString(0));
        Assert.assertEquals(d2[i], rows.get(i).getInt(1));
        Assert.assertEquals(a1[i], rows.get(i).getInt(2));
      }
    }
  }

  @Test
  public void testQueryCondition() throws TileDBError {
    List<Dimension> dimensions = new ArrayList<>();
    dimensions.add(new Dimension(ctx, "d1", Datatype.TILEDB_STRING_ASCII, null, null));
    dimensions.add(new Dimension(ctx, "d2", Datatype.TILEDB_INT32, new Pair(0, 100), 2));

    NativeArray d1_data =
        new NativeArray(ctx, "object1object2object3", Datatype.TILEDB_STRING_ASCII);
    NativeArray d1_off = new NativeArray(ctx, new long[] {0, 7, 14}, Datatype.TILEDB_UINT64);
    NativeArray d2_data = new NativeArray(ctx, new int[] {12, 40, 50}, Datatype.TILEDB_INT32);
    NativeArray a1_data = new NativeArray(ctx, new int[] {10, 23, 30}, Datatype.TILEDB_INT32);
    NativeArray a2_data = new NativeArray(ctx, new int[] {100, 230, 300}, Datatype.TILEDB_INT32);

    List<Pair<String, Pair<NativeArray, NativeArray>>> data = new ArrayList<>();
    data.add(new Pair<>("d1", new Pair<>(d1_data, d1_off)));
    data.add(new Pair<>("d2", new Pair<>(d2_data, null)));
    data.add(new Pair<>("a1", new Pair<>(a1_data, null)));
    data.add(new Pair<>("a2", new Pair<>(a2_data, null)));

    sparseHeterogeneousArrayCreate2A(dimensions);
    sparseHeterogeneousArrayWrite(data);

    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("order", "row-major")
            .option("partition_count", 2)
            .load(SPARSE_ARRAY_URI);
    dfRead.createOrReplaceTempView("tmp");
    dfRead.show();
    List<Row> rows1 = dfRead.sqlContext().sql("SELECT * FROM tmp").collectAsList();
    List<Row> rows2 =
        dfRead
            .sqlContext()
            .sql("SELECT * FROM tmp WHERE a2 >= 100 AND a1 > 25 ")
            .collectAsList(); // is pushed down
    List<Row> rows3 =
        dfRead
            .sqlContext()
            .sql("SELECT * FROM tmp WHERE a1 >= 24 OR a1 = 23")
            .collectAsList(); // is pushed down
    List<Row> rows4 =
        dfRead
            .sqlContext()
            .sql("SELECT * FROM tmp WHERE (a1 > 25 OR a1 < 20) AND a2 = 300")
            .collectAsList(); // is not pushed down
    List<Row> rows5 =
        dfRead
            .sqlContext()
            .sql("SELECT * FROM tmp WHERE (d2 > 20 AND a1 > 9) OR a2 = 100")
            .collectAsList(); // is pushed down

    //      for (int i = 0; i < rows3.size(); i++) {
    //        System.out.println(rows3.get(i).getInt(3));
    //      }

    String[] d1 = new String[] {"object1", "object2", "object3"};
    int[] d2 = new int[] {12, 40, 50};
    int[] a1 = new int[] {10, 23, 30};
    int[] a2 = new int[] {100, 230, 300};

    Assert.assertEquals(rows1.size(), d1.length);
    for (int i = 0; i < rows1.size(); i++) {
      Assert.assertEquals(d1[i], rows1.get(i).getString(0));
      Assert.assertEquals(d2[i], rows1.get(i).getInt(1));
      Assert.assertEquals(a1[i], rows1.get(i).getInt(2));
      Assert.assertEquals(a2[i], rows1.get(i).getInt(3));
    }

    d1 = new String[] {"object3"};
    d2 = new int[] {50};
    a1 = new int[] {30};
    a2 = new int[] {300};
    Assert.assertEquals(rows2.size(), d1.length);
    for (int i = 0; i < rows2.size(); i++) {
      Assert.assertEquals(d1[i], rows2.get(i).getString(0));
      Assert.assertEquals(d2[i], rows2.get(i).getInt(1));
      Assert.assertEquals(a1[i], rows2.get(i).getInt(2));
      Assert.assertEquals(a2[i], rows2.get(i).getInt(3));
    }

    d1 = new String[] {"object2", "object3"};
    d2 = new int[] {40, 50};
    a1 = new int[] {23, 30};
    a2 = new int[] {230, 300};
    Assert.assertEquals(rows3.size(), d1.length);
    for (int i = 0; i < rows3.size(); i++) {
      Assert.assertEquals(d1[i], rows3.get(i).getString(0));
      Assert.assertEquals(d2[i], rows3.get(i).getInt(1));
      Assert.assertEquals(a1[i], rows3.get(i).getInt(2));
      Assert.assertEquals(a2[i], rows3.get(i).getInt(3));
    }

    d1 = new String[] {"object3"};
    d2 = new int[] {50};
    a1 = new int[] {30};
    a2 = new int[] {300};
    Assert.assertEquals(rows4.size(), d1.length);
    for (int i = 0; i < rows4.size(); i++) {
      Assert.assertEquals(d1[i], rows4.get(i).getString(0));
      Assert.assertEquals(d2[i], rows4.get(i).getInt(1));
      Assert.assertEquals(a1[i], rows4.get(i).getInt(2));
      Assert.assertEquals(a2[i], rows4.get(i).getInt(3));
    }

    d1 = new String[] {"object1", "object2", "object3"};
    d2 = new int[] {12, 40, 50};
    a1 = new int[] {10, 23, 30};
    a2 = new int[] {100, 230, 300};
    Assert.assertEquals(rows5.size(), d1.length);
    for (int i = 0; i < rows5.size(); i++) {
      Assert.assertEquals(d1[i], rows5.get(i).getString(0));
      Assert.assertEquals(d2[i], rows5.get(i).getInt(1));
      Assert.assertEquals(a1[i], rows5.get(i).getInt(2));
      Assert.assertEquals(a2[i], rows5.get(i).getInt(3));
    }
  }

  @Test
  public void testHeterogeneousSparse2() throws TileDBError {
    List<Dimension> dimensions = new ArrayList<>();
    dimensions.add(new Dimension(ctx, "d1", Datatype.TILEDB_STRING_ASCII, null, null));
    dimensions.add(new Dimension(ctx, "d2", Datatype.TILEDB_STRING_ASCII, null, null));
    dimensions.add(new Dimension(ctx, "d3", Datatype.TILEDB_INT64, new Pair(0L, 1000L), 2L));

    NativeArray d1_data =
        new NativeArray(ctx, "object1object2object3object4", Datatype.TILEDB_STRING_ASCII);
    NativeArray d1_off = new NativeArray(ctx, new long[] {0, 7, 14, 21}, Datatype.TILEDB_UINT64);
    NativeArray d2_data = new NativeArray(ctx, "aabbccdd", Datatype.TILEDB_STRING_ASCII);
    NativeArray d2_off = new NativeArray(ctx, new long[] {0, 2, 4, 6}, Datatype.TILEDB_UINT64);
    NativeArray d3_data =
        new NativeArray(ctx, new long[] {100, 200, 300, 400}, Datatype.TILEDB_INT64);
    NativeArray a1_data = new NativeArray(ctx, new int[] {10, 23, 30, 60}, Datatype.TILEDB_INT32);

    List<Pair<String, Pair<NativeArray, NativeArray>>> data = new ArrayList<>();
    data.add(new Pair<>("d1", new Pair<>(d1_data, d1_off)));
    data.add(new Pair<>("d2", new Pair<>(d2_data, d2_off)));
    data.add(new Pair<>("d3", new Pair<>(d3_data, null)));
    data.add(new Pair<>("a1", new Pair<>(a1_data, null)));

    sparseHeterogeneousArrayCreate(dimensions);
    sparseHeterogeneousArrayWrite(data);

    for (String order : new String[] {"row-major", "TILEDB_ROW_MAJOR"}) {
      Dataset<Row> dfRead =
          session()
              .read()
              .format("io.tiledb.spark")
              .option("order", order)
              .option("partition_count", 1)
              .load(SPARSE_ARRAY_URI);
      dfRead.createOrReplaceTempView("tmp");
      dfRead.show();
      List<Row> rows = dfRead.sqlContext().sql("SELECT * FROM tmp").collectAsList();

      String[] d1 = new String[] {"object1", "object2", "object3", "object4"};
      String[] d2 = new String[] {"aa", "bb", "cc", "dd"};
      int[] d3 = new int[] {100, 200, 300, 400};
      int[] a1 = new int[] {10, 23, 30, 60};
      Assert.assertEquals(d1.length, rows.size());

      for (int i = 0; i < rows.size(); i++) {
        Assert.assertEquals(d1[i], rows.get(i).getString(0));
        Assert.assertEquals(d2[i], rows.get(i).getString(1));
        Assert.assertEquals(d3[i], rows.get(i).getLong(2));
        Assert.assertEquals(a1[i], rows.get(i).getInt(3));
      }
    }
  }
}
