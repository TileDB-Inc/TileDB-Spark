package io.tiledb.spark;

import static io.tiledb.java.api.ArrayType.TILEDB_DENSE;
import static io.tiledb.java.api.ArrayType.TILEDB_SPARSE;
import static io.tiledb.java.api.Constants.TILEDB_VAR_NUM;
import static io.tiledb.java.api.Layout.TILEDB_GLOBAL_ORDER;
import static io.tiledb.java.api.Layout.TILEDB_ROW_MAJOR;
import static io.tiledb.java.api.QueryType.TILEDB_WRITE;
import static io.tiledb.spark.TestDataFrame.assertDataFrameEquals;

import io.tiledb.java.api.*;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.*;

public class NullableAttributesTest extends SharedJavaSparkSession {

  private static Context ctx;
  private String denseURI;
  private String sparseURI;
  private String variableAttURI;
  private String writeArrayURI;
  private String tempWrite;

  @Before
  public void setup() throws Exception {
    ctx = new Context();
    denseURI = "dense_array";
    variableAttURI = "variable_att_array";
    writeArrayURI = "write_dense_array";
    sparseURI = "sparse_array";
    tempWrite = "temp_write";
    deleteArrays();
  }

  @After
  public void delete() throws Exception {
    deleteArrays();
    ctx.close();
  }

  private void deleteArrays() throws TileDBError {
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
    if (Files.exists(Paths.get(tempWrite))) {
      TileDBObject.remove(ctx, tempWrite);
    }
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
    Attribute a1 = new Attribute(ctx, "a1", Float.class);
    Attribute a2 = new Attribute(ctx, "a2", Integer.class);

    a1.setNullable(true);
    a2.setNullable(true);

    ArraySchema schema = new ArraySchema(ctx, TILEDB_DENSE);
    schema.setTileOrder(TILEDB_ROW_MAJOR);
    schema.setCellOrder(TILEDB_ROW_MAJOR);
    schema.setDomain(domain);
    schema.addAttribute(a1);
    schema.addAttribute(a2);

    Array.create(denseURI, schema);
    schema.close();
    domain.close();
  }

  public void denseArrayWrite() throws Exception {
    // Prepare cell buffers
    NativeArray a1 = new NativeArray(ctx, new float[] {2.0f, 3.0f, 4.0f, 1.0f}, Float.class);
    NativeArray a2 = new NativeArray(ctx, new int[] {1, 4, 2, 2}, Integer.class);

    // Create query
    try (Array array = new Array(ctx, denseURI, TILEDB_WRITE);
        Query query = new Query(array)) {

      array.putMetadata("one", new int[] {100});
      array.putMetadata("two", new float[] {99f});
      query.setLayout(TILEDB_ROW_MAJOR);
      NativeArray a1Bytemap = new NativeArray(ctx, new short[] {0, 1, 1, 0}, Datatype.TILEDB_UINT8);
      NativeArray a2Bytemap = new NativeArray(ctx, new short[] {1, 1, 0, 1}, Datatype.TILEDB_UINT8);

      query.setBufferNullable("a1", a1, a1Bytemap);
      query.setBufferNullable("a2", a2, a2Bytemap);

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
    Dataset<Row> dfRead = session().read().format("io.tiledb.spark").load(denseURI);

    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();
    Assert.assertEquals(4, rows.size());
    // 1st row

    Row row = rows.get(0);
    Assert.assertEquals(1, row.getInt(0));
    Assert.assertEquals(1, row.getInt(1));
    Assert.assertNull(row.get(2));
    Assert.assertEquals(1, row.getInt(3));

    // 2nd row
    row = rows.get(1);
    Assert.assertEquals(1, row.getInt(0));
    Assert.assertEquals(2, row.getInt(1));
    Assert.assertEquals(3.0f, row.getFloat(2), 0);
    Assert.assertEquals(4, row.getInt(3));

    // 3nd row
    row = rows.get(2);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertEquals(1, row.getInt(1));
    Assert.assertEquals(4.0f, row.getFloat(2), 0);
    Assert.assertNull(row.get(3));

    // 4th row
    row = rows.get(3);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertEquals(2, row.getInt(1));
    Assert.assertNull(row.get(2));
    Assert.assertEquals(2, row.getInt(3));
    return;
  }

  @Test
  public void printMetadataTest() throws Exception {
    // Create a stream to hold the output
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    // Save the old System.out!
    PrintStream old = System.out;
    // Tell Java to use your special stream
    System.setOut(ps);

    denseArrayCreate();
    denseArrayWrite();
    // Since Spark reads lazily, the code below will not actually read the array to improve
    // performance. However, the metadata is printed.
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("print_array_metadata", true)
            .load(denseURI);

    // Put things back
    System.out.flush();
    System.setOut(old);
    // Check output
    Assert.assertEquals("<one, 100>\n<two, 99.0>\n", baos.toString());
  }

  /**
   * Dense array with variable attribute size
   *
   * @throws Exception
   */
  public void denseArrayVarAttCreate() throws Exception {

    Dimension<Integer> rows =
        new Dimension<>(ctx, "rows", Integer.class, new Pair<Integer, Integer>(1, 8), 2);

    // Create and set getDomain
    Domain domain = new Domain(ctx);
    domain.addDimension(rows);

    Attribute a1 = new Attribute(ctx, "a1", Datatype.TILEDB_CHAR);
    a1.setCellValNum(TILEDB_VAR_NUM);

    a1.setNullable(true);

    ArraySchema schema = new ArraySchema(ctx, TILEDB_DENSE);
    schema.setTileOrder(TILEDB_ROW_MAJOR);
    schema.setCellOrder(TILEDB_ROW_MAJOR);
    schema.setDomain(domain);
    schema.addAttribute(a1);

    Array.create(variableAttURI, schema);
    schema.close();
    domain.close();
  }

  public void denseArrayVarAttWrite() throws Exception {

    NativeArray a1_data = new NativeArray(ctx, "aabbccddeeffgghh", Datatype.TILEDB_CHAR);
    NativeArray a1_off =
        new NativeArray(ctx, new long[] {0, 3, 4, 6, 7, 10, 13, 14}, Datatype.TILEDB_UINT64);

    // byte vector for null values
    NativeArray a1ByteMap =
        new NativeArray(ctx, new short[] {1, 0, 1, 0, 1, 1, 0, 1}, Datatype.TILEDB_UINT8);

    // Create query
    try (Array array = new Array(ctx, variableAttURI, TILEDB_WRITE);
        Query query = new Query(array)) {
      query.setLayout(TILEDB_ROW_MAJOR);
      query.setBufferNullable("a1", a1_off, a1_data, a1ByteMap);
      // Submit query
      query.submit();
    }
  }

  @Test
  public void denseArrayVarAttReadTest() throws Exception {
    denseArrayVarAttCreate();
    denseArrayVarAttWrite();
    Dataset<Row> dfRead = session().read().format("io.tiledb.spark").load(variableAttURI);
    // dfRead.show();
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();
    Assert.assertEquals(8, rows.size());
    // 1st row
    Row row = rows.get(0);
    Assert.assertEquals(1, row.getInt(0));
    Assert.assertEquals("aab", row.getString(1));

    row = rows.get(1);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertNull(row.get(1));

    row = rows.get(2);
    Assert.assertEquals(3, row.getInt(0));
    Assert.assertEquals("cc", row.getString(1));

    row = rows.get(3);
    Assert.assertEquals(4, row.getInt(0));
    Assert.assertNull(row.get(1));

    row = rows.get(4);
    Assert.assertEquals(5, row.getInt(0));
    Assert.assertEquals("dee", row.getString(1));

    row = rows.get(5);
    Assert.assertEquals(6, row.getInt(0));
    Assert.assertEquals("ffg", row.getString(1));

    row = rows.get(6);
    Assert.assertEquals(7, row.getInt(0));
    Assert.assertNull(row.get(1));

    row = rows.get(7);
    Assert.assertEquals(8, row.getInt(0));
    Assert.assertEquals("hh", row.getString(1));
    return;
  }

  /**
   * Sparse array with variable size attributes
   *
   * @throws TileDBError
   */
  public void sparseArrayCreate() throws TileDBError {
    // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
    Dimension<Integer> d1 =
        new Dimension<>(ctx, "d1", Integer.class, new Pair<Integer, Integer>(1, 8), 2);

    // Create and set getDomain
    Domain domain = new Domain(ctx);
    domain.addDimension(d1);

    // Add two attributes "a1" and "a2", so each (i,j) cell can store
    // a character on "a1" and a vector of two floats on "a2".
    Attribute a1 = new Attribute(ctx, "a1", Integer.class);
    Attribute a2 = new Attribute(ctx, "a2", Datatype.TILEDB_STRING_ASCII);
    a2.setCellVar();

    a1.setNullable(true);
    a2.setNullable(true);

    ArraySchema schema = new ArraySchema(ctx, TILEDB_SPARSE);
    schema.setTileOrder(TILEDB_ROW_MAJOR);
    schema.setCellOrder(TILEDB_ROW_MAJOR);
    schema.setDomain(domain);
    schema.addAttribute(a1);
    schema.addAttribute(a2);

    Array.create(sparseURI, schema);
    schema.close();
    domain.close();
  }

  public void sparseArrayWrite() throws TileDBError {
    NativeArray d_data = new NativeArray(ctx, new int[] {1, 2, 3, 4, 5}, Integer.class);
    // NativeArray d_off = new NativeArray(ctx, new long[] {0, 2, 4, 6, 8},Datatype.TILEDB_UINT64);

    // Prepare cell buffers
    NativeArray a1 = new NativeArray(ctx, new int[] {1, 2, 3, 4, 5}, Integer.class);

    NativeArray a2_data = new NativeArray(ctx, "aabbccddee", Datatype.TILEDB_STRING_ASCII);
    NativeArray a2_off = new NativeArray(ctx, new long[] {0, 2, 4, 6, 8}, Datatype.TILEDB_UINT64);

    // Create query
    Array array = new Array(ctx, sparseURI, TILEDB_WRITE);
    Query query = new Query(array);
    query.setLayout(TILEDB_GLOBAL_ORDER);

    NativeArray a1ByteMap =
        new NativeArray(ctx, new short[] {0, 0, 0, 1, 1}, Datatype.TILEDB_UINT8);
    NativeArray a2ByteMap =
        new NativeArray(ctx, new short[] {1, 1, 1, 0, 0}, Datatype.TILEDB_UINT8);

    query.setBuffer("d1", d_data);
    query.setBufferNullable("a1", a1, a1ByteMap);
    query.setBufferNullable("a2", a2_off, a2_data, a2ByteMap);

    // Submit query
    query.submit();

    query.finalizeQuery();
    query.close();
    array.close();
  }

  @Test
  public void testSparse() throws Exception {
    sparseArrayCreate();
    sparseArrayWrite();
  }

  @Test
  public void sparseArrayReadTest() throws Exception {
    sparseArrayCreate();
    sparseArrayWrite();
    Dataset<Row> dfRead = session().read().format("io.tiledb.spark").load(sparseURI);
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();
    Assert.assertEquals(5, rows.size());

    Row row = rows.get(0);
    Assert.assertEquals(1, row.get(0));
    Assert.assertNull(row.get(1));
    Assert.assertEquals("aa", row.get(2));

    row = rows.get(1);
    Assert.assertEquals(2, row.get(0));
    Assert.assertNull(row.get(1));
    Assert.assertEquals("bb", row.get(2));

    row = rows.get(2);
    Assert.assertEquals(3, row.get(0));
    Assert.assertNull(row.get(1));
    Assert.assertEquals("cc", row.get(2));

    row = rows.get(3);
    Assert.assertEquals(4, row.get(0));
    Assert.assertEquals(4, row.get(1));
    Assert.assertNull(row.get(2));

    row = rows.get(4);
    Assert.assertEquals(5, row.get(0));
    Assert.assertEquals(5, row.get(1));
    Assert.assertNull(row.get(2));
  }

  /*
  ==============================================================
                            WRITE TESTS
  ==============================================================
   */

  /**
   * Sparse dataset with fixed-size attributes.
   *
   * @param ss The spark session.
   * @return The created dataframe.
   */
  public Dataset<Row> createSparseDataset(SparkSession ss) {
    StructField[] structFields =
        new StructField[] {
          new StructField("d1", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("a1", DataTypes.IntegerType, true, Metadata.empty()),
          new StructField("a2", DataTypes.StringType, true, Metadata.empty()),
        };
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1, null, "a"));
    rows.add(RowFactory.create(2, null, "b"));
    rows.add(RowFactory.create(3, null, "c"));
    rows.add(RowFactory.create(4, 4, null));
    rows.add(RowFactory.create(5, 5, null));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df;
  }

  /**
   * Sparse dataset with var-size attributes.
   *
   * @param ss The spark session.
   * @return The created dataframe.
   */
  public Dataset<Row> createSparseDatasetVar(SparkSession ss) {
    StructField[] structFields =
        new StructField[] {
          new StructField("d1", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("a1", DataTypes.IntegerType, true, Metadata.empty()),
          new StructField("a2", DataTypes.StringType, true, Metadata.empty()),
        };
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1, null, "aaa"));
    rows.add(RowFactory.create(2, null, "b"));
    rows.add(RowFactory.create(3, null, "cccc"));
    rows.add(RowFactory.create(4, 4, null));
    rows.add(RowFactory.create(5, 5, null));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df;
  }

  @Test
  public void sparseWriteTest() throws Exception {
    Dataset<Row> dfReadFirst = createSparseDataset(session());
    // dfReadFirst.show();
    dfReadFirst
        .write()
        .format("io.tiledb.spark")
        .option("schema.dim.0.name", "d1")
        .option("schema.dim.0.min", 1)
        .option("schema.dim.0.max", 8)
        .option("schema.dim.0.extent", 2)
        .option("schema.attr.a1.filter_list", "(byteshuffle, -1), (gzip, -10)")
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
    Assert.assertEquals(4, row.get(1));
    Assert.assertNull(row.get(2));

    row = rows.get(4);
    Assert.assertEquals(5, row.get(0));
    Assert.assertEquals(5, row.get(1));
    Assert.assertNull(row.get(2));
  }

  @Test
  public void sparseWriteVarAttTest() throws Exception {
    Dataset<Row> dfReadFirst = createSparseDatasetVar(session());
    // dfReadFirst.show();
    dfReadFirst
        .write()
        .format("io.tiledb.spark")
        .option("schema.dim.0.name", "d1")
        .option("schema.dim.0.min", 1)
        .option("schema.dim.0.max", 8)
        .option("schema.dim.0.extent", 2)
        .option("schema.attr.a1.filter_list", "(byteshuffle, -1), (gzip, -10)")
        .option("schema.cell_order", "row-major")
        .option("schema.tile_order", "row-major")
        .option("schema.capacity", 3)
        .mode("overwrite")
        .save(writeArrayURI);

    Dataset<Row> dfRead =
        session().read().format("io.tiledb.spark").option("uri", writeArrayURI).load();
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();
    Assert.assertEquals(5, rows.size());

    Row row = rows.get(0);
    Assert.assertEquals(1, row.get(0));
    Assert.assertNull(row.get(1));
    Assert.assertEquals("aaa", row.get(2));

    row = rows.get(1);
    Assert.assertEquals(2, row.get(0));
    Assert.assertNull(row.get(1));
    Assert.assertEquals("b", row.get(2));

    row = rows.get(2);
    Assert.assertEquals(3, row.get(0));
    Assert.assertNull(row.get(1));
    Assert.assertEquals("cccc", row.get(2));

    row = rows.get(3);
    Assert.assertEquals(4, row.get(0));
    Assert.assertEquals(4, row.get(1));
    Assert.assertNull(row.get(2));

    row = rows.get(4);
    Assert.assertEquals(5, row.get(0));
    Assert.assertEquals(5, row.get(1));
    Assert.assertNull(row.get(2));
  }

  /*
  ==============================================================
                            READ WRITE READ TESTS
  ==============================================================
   */

  @Test
  public void denseReadWriteReadTest() throws Exception {
    denseArrayCreate();
    denseArrayWrite();

    Dataset<Row> dfRead = session().read().format("io.tiledb.spark").load(denseURI);

    dfRead.show();

    StructField[] structFields =
        new StructField[] {
          new StructField("rows", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("cols", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("a1", DataTypes.FloatType, true, Metadata.empty()),
          new StructField("a2", DataTypes.IntegerType, true, Metadata.empty()),
        };
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1, 1, null, 1));
    rows.add(RowFactory.create(1, 2, 3.0f, 4));
    rows.add(RowFactory.create(2, 1, 4.0f, null));
    rows.add(RowFactory.create(2, 2, null, 2));
    StructType structType = new StructType(structFields);
    Dataset<Row> expected = session().createDataFrame(rows, structType);

    dfRead.cache();

    Assert.assertTrue(assertDataFrameEquals(expected, dfRead));

    dfRead
        .write()
        .format("io.tiledb.spark")
        .option("uri", tempWrite)
        .option("schema.dim.0.name", "rows")
        .option("schema.dim.0.min", 1)
        .option("schema.dim.0.max", 2)
        .option("schema.dim.0.extent", 2)
        .option("schema.dim.1.name", "cols")
        .option("schema.dim.1.min", 1)
        .option("schema.dim.1.max", 2)
        .option("schema.dim.1.extent", 2)
        .option("schema.cell_order", "row-major")
        .option("schema.tile_order", "row-major")
        .mode("overwrite")
        .save();

    Dataset<Row> dfReadSecond = session().read().format("io.tiledb.spark").load(tempWrite);

    Assert.assertTrue(assertDataFrameEquals(expected, dfReadSecond));

    if (Array.exists(ctx, "temp_write")) {
      TileDBObject.remove(ctx, "temp_write");
    }
  }
}
