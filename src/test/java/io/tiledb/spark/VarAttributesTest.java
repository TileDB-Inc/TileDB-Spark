package io.tiledb.spark;

import static io.tiledb.java.api.ArrayType.TILEDB_DENSE;
import static io.tiledb.java.api.Layout.TILEDB_ROW_MAJOR;
import static io.tiledb.java.api.QueryType.TILEDB_WRITE;

import io.tiledb.java.api.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.spark.sql.*;
import org.junit.*;

public class VarAttributesTest extends SharedJavaSparkSession {

  private static Context ctx;
  private String URI;

  @Before
  public void setup() throws Exception {
    ctx = new Context();
    URI = "array_var_atts";
    if (Files.exists(Paths.get(URI))) {
      TileDBObject.remove(ctx, URI);
    }
  }

  @After
  public void delete() throws Exception {
    if (Files.exists(Paths.get(URI))) {
      TileDBObject.remove(ctx, URI);
    }
    ctx.close();
  }

  /**
   * Dense array with fixed size attributes.
   *
   * @throws Exception
   */
  public void arrayCreate() throws Exception {
    // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
    Dimension<Integer> rows =
        new Dimension<>(ctx, "rows", Integer.class, new Pair<Integer, Integer>(1, 2), 2);
    Dimension<Integer> cols =
        new Dimension<>(ctx, "cols", Integer.class, new Pair<Integer, Integer>(1, 2), 2);

    // Create and set getDomain
    Domain domain = new Domain(ctx);
    domain.addDimension(rows);
    domain.addDimension(cols);

    // Add two attributes "a1" and "a2", so each (i,j) cell can store
    // a character on "a1" and a vector of two floats on "a2".
    Attribute a1 = new Attribute(ctx, "a1", Integer.class);
    Attribute a2 = new Attribute(ctx, "a2", Long.class);
    Attribute a3 = new Attribute(ctx, "a3", String.class);
    a2.setNullable(true);
    a2.setCellVar();
    a3.setNullable(true);
    a3.setCellVar();

    ArraySchema schema = new ArraySchema(ctx, TILEDB_DENSE);
    schema.setTileOrder(TILEDB_ROW_MAJOR);
    schema.setCellOrder(TILEDB_ROW_MAJOR);
    schema.setDomain(domain);
    schema.addAttribute(a1);
    schema.addAttribute(a2);
    schema.addAttribute(a3);

    Array.create(URI, schema);
  }

  public void arrayWrite() throws Exception {
    // Prepare cell buffers
    NativeArray a1 = new NativeArray(ctx, new int[] {222, 322, 422, 122}, Integer.class);
    NativeArray a2 =
        new NativeArray(
            ctx, new long[] {99L, 991L, 992L, 993L, 994L, 995L, 996L, 997L}, Long.class);
    NativeArray buffer_var_a3 =
        new NativeArray(ctx, "hhhh" + "ff" + "a" + "bb", Datatype.TILEDB_CHAR);
    NativeArray a3_offsets = new NativeArray(ctx, new long[] {0, 4, 6, 7}, Datatype.TILEDB_UINT64);

    NativeArray a2Bytemap = new NativeArray(ctx, new short[] {1, 0, 1, 0}, Datatype.TILEDB_UINT8);

    NativeArray a3Bytemap = new NativeArray(ctx, new short[] {0, 1, 0, 1}, Datatype.TILEDB_UINT8);

    NativeArray a2_offsets =
        new NativeArray(ctx, new long[] {0, 24, 32, 40}, Datatype.TILEDB_UINT64);
    // Create query
    try (Array array = new Array(ctx, URI, TILEDB_WRITE);
        Query query = new Query(array)) {
      query.setLayout(TILEDB_ROW_MAJOR);

      query.setBuffer("a1", a1);
      query.setBufferNullable("a2", a2_offsets, a2, a2Bytemap);
      query.setBufferNullable("a3", a3_offsets, buffer_var_a3, a3Bytemap);

      // Submit query
      query.submit();
    }
  }

  @Test
  public void testDense() throws Exception {
    arrayCreate();
    arrayWrite();
  }

  @Test
  public void denseArrayReadTest() throws Exception {
    arrayCreate();
    arrayWrite();
    Dataset<Row> dfRead = session().read().format("io.tiledb.spark").option("uri", URI).load();
    dfRead.show();
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();
    Assert.assertEquals(4, rows.size());
    // 1st row
    Row row = rows.get(0);
    Assert.assertEquals(1, row.getInt(0));
    Assert.assertEquals(1, row.getInt(1));
    Assert.assertEquals(222, row.getInt(2));
    Assert.assertEquals(List.of(99L, 991L, 992L), row.getList(3));
    Assert.assertNull(row.get(4));

    // 2nd row
    row = rows.get(1);
    Assert.assertEquals(1, row.getInt(0));
    Assert.assertEquals(2, row.getInt(1));
    Assert.assertEquals(322, row.getInt(2));
    Assert.assertNull(row.get(3));
    Assert.assertEquals("ff", row.getString(4));

    // 3nd row
    row = rows.get(2);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertEquals(1, row.getInt(1));
    Assert.assertEquals(422, row.getInt(2));
    Assert.assertEquals(List.of(994L), row.getList(3));
    Assert.assertNull(row.get(4));

    // 4th row
    row = rows.get(3);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertEquals(2, row.getInt(1));
    Assert.assertEquals(122, row.getInt(2));
    Assert.assertNull(row.get(3));
    Assert.assertEquals("bb", row.getString(4));
  }
}
