package io.tiledb.spark;

import static io.tiledb.java.api.ArrayType.TILEDB_DENSE;
import static io.tiledb.java.api.ArrayType.TILEDB_SPARSE;
import static io.tiledb.java.api.Layout.TILEDB_GLOBAL_ORDER;
import static io.tiledb.java.api.Layout.TILEDB_ROW_MAJOR;
import static io.tiledb.java.api.QueryType.TILEDB_WRITE;

import io.tiledb.java.api.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.spark.sql.*;
import org.junit.*;

public class SimpleTests extends SharedJavaSparkSession {

  private static Context ctx;
  private String denseURI;
  private String sparseURI;
  private String variableAttURI;
  private String writeArrayURI;

  @Before
  public void setup() throws Exception {
    ctx = new Context();
    denseURI = "dense_array";
    variableAttURI = "variable_att_array";
    writeArrayURI = "write_dense_array";
    sparseURI = "sparse_array";
    if (Files.exists(Paths.get(denseURI))) {
      TileDBObject.remove(ctx, denseURI);
    }
    if (Files.exists(Paths.get(variableAttURI))) {
      TileDBObject.remove(ctx, variableAttURI);
    }
    if (Files.exists(Paths.get(writeArrayURI))) {
      TileDBObject.remove(ctx, writeArrayURI);
    }
    if (Files.exists(Paths.get(sparseURI))) {
      TileDBObject.remove(ctx, sparseURI);
    }
  }

  @After
  public void delete() throws Exception {
    if (Files.exists(Paths.get(denseURI))) {
      TileDBObject.remove(ctx, denseURI);
    }
    if (Files.exists(Paths.get(variableAttURI))) {
      TileDBObject.remove(ctx, variableAttURI);
    }
    if (Files.exists(Paths.get(writeArrayURI))) {
      TileDBObject.remove(ctx, writeArrayURI);
    }
    if (Files.exists(Paths.get(sparseURI))) {
      TileDBObject.remove(ctx, sparseURI);
    }
    ctx.close();
  }

  /**
   * Dense array with fixed size attributes.
   *
   * @throws Exception
   */
  public void denseArrayCreate() throws Exception {
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
    Attribute a2 = new Attribute(ctx, "a2", Integer.class);
    Attribute a3 = new Attribute(ctx, "a3", String.class);
    a2.setNullable(true);
    a3.setNullable(true);
    a3.setCellVar();

    ArraySchema schema = new ArraySchema(ctx, TILEDB_DENSE);
    schema.setTileOrder(TILEDB_ROW_MAJOR);
    schema.setCellOrder(TILEDB_ROW_MAJOR);
    schema.setDomain(domain);
    schema.addAttribute(a1);
    schema.addAttribute(a2);
    schema.addAttribute(a3);

    Array.create(denseURI, schema);
  }

  public void denseArrayWrite() throws Exception {
    // Prepare cell buffers
    NativeArray a1 = new NativeArray(ctx, new int[] {222, 322, 422, 122}, Integer.class);
    NativeArray a2 = new NativeArray(ctx, new int[] {2, 3, 4, 5}, Integer.class);
    NativeArray buffer_var_a3 =
        new NativeArray(ctx, "hhhh" + "ff" + "a" + "bb", Datatype.TILEDB_CHAR);
    NativeArray a3_offsets = new NativeArray(ctx, new long[] {0, 4, 6, 7}, Datatype.TILEDB_UINT64);

    NativeArray a2Bytemap = new NativeArray(ctx, new short[] {1, 0, 1, 0}, Datatype.TILEDB_UINT8);

    NativeArray a3Bytemap = new NativeArray(ctx, new short[] {0, 1, 0, 1}, Datatype.TILEDB_UINT8);
    // Create query
    try (Array array = new Array(ctx, denseURI, TILEDB_WRITE);
        Query query = new Query(array)) {
      query.setLayout(TILEDB_ROW_MAJOR);

      query.setBuffer("a1", a1);
      query.setBufferNullable("a2", a2, a2Bytemap);
      query.setBufferNullable("a3", a3_offsets, buffer_var_a3, a3Bytemap);

      // Submit query
      query.submit();
    }
  }

  @Test
  public void testDense() throws Exception {
    denseArrayCreate();
    denseArrayWrite();
  }

  @Test
  public void denseArrayReadTest() throws Exception {
    denseArrayCreate();
    denseArrayWrite();
    Dataset<Row> dfRead = session().read().format("io.tiledb.spark").option("uri", denseURI).load();
    dfRead.show();
    //        dfRead.createOrReplaceTempView("tmp");
    //        List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();
    //        Assert.assertEquals(4, rows.size());
    //        // 1st row
    //        Row row = rows.get(0);
    //        Assert.assertEquals(1, row.getInt(0));
    //        Assert.assertEquals(1, row.getInt(1));
    //        Assert.assertNull(row.get(2));
    //        Assert.assertEquals(1, row.getInt(3));
    //
    //        // 2nd row
    //        row = rows.get(1);
    //        Assert.assertEquals(1, row.getInt(0));
    //        Assert.assertEquals(2, row.getInt(1));
    //        Assert.assertEquals(3.0f, row.getFloat(2), 0);
    //        Assert.assertEquals(4, row.getInt(3));
    //
    //        // 3nd row
    //        row = rows.get(2);
    //        Assert.assertEquals(2, row.getInt(0));
    //        Assert.assertEquals(1, row.getInt(1));
    //        Assert.assertEquals(4.0f, row.getFloat(2), 0);
    //        Assert.assertNull(row.get(3));
    //
    //        // 4th row
    //        row = rows.get(3);
    //        Assert.assertEquals(2, row.getInt(0));
    //        Assert.assertEquals(2, row.getInt(1));
    //        Assert.assertNull(row.get(2));
    //        Assert.assertEquals(2, row.getInt(3));
    //        return;
  }

  ////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Sparse array with variable size attributes
   *
   * @throws TileDBError
   */
  public void sparseArrayCreate() throws TileDBError {
    // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
    Dimension<Integer> d1 =
        new Dimension<>(ctx, "d1", Integer.class, new Pair<Integer, Integer>(1, 4), 2);
    Dimension<Integer> d2 =
        new Dimension<>(ctx, "d2", Integer.class, new Pair<Integer, Integer>(1, 4), 2);

    // Create and set getDomain
    Domain domain = new Domain(ctx);
    domain.addDimension(d1);
    domain.addDimension(d2);

    // Add two attributes "a1" and "a2", so each (i,j) cell can store
    // a character on "a1" and a vector of two floats on "a2".
    Attribute a1 = new Attribute(ctx, "a1", Integer.class);
    Attribute a2 = new Attribute(ctx, "a2", Datatype.TILEDB_CHAR);
    a2.setCellVar();

    a1.setNullable(true);
    a2.setNullable(true);

    ArraySchema schema = new ArraySchema(ctx, TILEDB_SPARSE);
    schema.setTileOrder(TILEDB_ROW_MAJOR);
    schema.setCellOrder(TILEDB_ROW_MAJOR);
    schema.setDomain(domain);
    schema.addAttribute(a1);
    //    schema.addAttribute(a2);

    Array.create(sparseURI, schema);
  }

  public void sparseArrayWrite() throws TileDBError {
    NativeArray d_data = new NativeArray(ctx, new int[] {1, 2, 3, 4}, Integer.class);
    NativeArray d_data2 = new NativeArray(ctx, new int[] {1, 2, 3, 4}, Integer.class);
    // NativeArray d_off = new NativeArray(ctx, new long[] {0, 2, 4, 6, 8},Datatype.TILEDB_UINT64);

    // Prepare cell buffers
    NativeArray a1 = new NativeArray(ctx, new int[] {1, 2, 3, 4}, Integer.class);

    NativeArray a2_data = new NativeArray(ctx, "aabbccdd", Datatype.TILEDB_CHAR);
    NativeArray a2_off = new NativeArray(ctx, new long[] {0, 2, 4, 6}, Datatype.TILEDB_UINT64);

    // Create query
    Array array = new Array(ctx, sparseURI, TILEDB_WRITE);
    Query query = new Query(array);
    query.setLayout(TILEDB_GLOBAL_ORDER);

    NativeArray a1ByteMap = new NativeArray(ctx, new short[] {0, 1, 0, 1}, Datatype.TILEDB_UINT8);
    NativeArray a2ByteMap = new NativeArray(ctx, new short[] {1, 1, 1, 0}, Datatype.TILEDB_UINT8);

    query.setBuffer("d1", d_data);
    query.setBuffer("d2", d_data2);
    query.setBufferNullable("a1", a1, a1ByteMap);
    //    query.setBufferNullable("a2", a2_off, a2_data, a2ByteMap);

    // Submit query
    query.submit();

    query.finalizeQuery();
    query.close();
    array.close();
  }

  @Test
  public void sparseArrayReadTest() throws Exception {
    sparseArrayCreate();
    sparseArrayWrite();
    Dataset<Row> dfRead =
        session().read().format("io.tiledb.spark").option("uri", sparseURI).load();
    dfRead.show();
    //    dfRead.createOrReplaceTempView("tmp");
    //    List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();
    //    Assert.assertEquals(5, rows.size());
    //
    //    Row row = rows.get(0);
    //    Assert.assertEquals(1, row.get(0));
    //    Assert.assertNull(row.get(1));
    //    Assert.assertEquals("aa", row.get(2));
    //
    //    row = rows.get(1);
    //    Assert.assertEquals(2, row.get(0));
    //    Assert.assertNull(row.get(1));
    //    Assert.assertEquals("bb", row.get(2));
    //
    //    row = rows.get(2);
    //    Assert.assertEquals(3, row.get(0));
    //    Assert.assertNull(row.get(1));
    //    Assert.assertEquals("cc", row.get(2));
    //
    //    row = rows.get(3);
    //    Assert.assertEquals(4, row.get(0));
    //    Assert.assertEquals(4, row.get(1));
    //    Assert.assertNull(row.get(2));
    //
    //    row = rows.get(4);
    //    Assert.assertEquals(5, row.get(0));
    //    Assert.assertEquals(5, row.get(1));
    //    Assert.assertNull(row.get(2));
  }
}
