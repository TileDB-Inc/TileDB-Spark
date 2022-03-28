package io.tiledb.spark;

import static io.tiledb.java.api.ArrayType.TILEDB_DENSE;
import static io.tiledb.java.api.ArrayType.TILEDB_SPARSE;
import static io.tiledb.java.api.Layout.TILEDB_GLOBAL_ORDER;
import static io.tiledb.java.api.Layout.TILEDB_ROW_MAJOR;
import static io.tiledb.java.api.QueryType.TILEDB_WRITE;
import static io.tiledb.spark.TestDataFrame.assertDataFrameEquals;

import io.tiledb.java.api.*;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joda.time.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SparkDatetypesTest extends SharedJavaSparkSession {
  private static Context ctx;
  private String denseURI;
  private String writeURI;
  private String denseURIToAppend;
  private String sparseURI;
  private long timeNowMil = System.currentTimeMillis();
  private int days;
  private int weeks;
  private int months;
  private int years;
  private String tempWrite;

  @Before
  public void setup() throws Exception {
    ctx = new Context();
    sparseURI = "sparse";
    writeURI = "write_URI";
    tempWrite = "temp_write";
    denseURI = "dense_array_uri";
    denseURIToAppend = "dense_append";
    MutableDateTime epoch = new MutableDateTime();
    epoch.setDate(0); // Set to Epoch time
    DateTime now = new DateTime();
    days = Days.daysBetween(epoch, now).getDays();
    weeks = Weeks.weeksBetween(epoch, now).getWeeks();
    months = Months.monthsBetween(epoch, now).getMonths();
    years = Years.yearsBetween(epoch, now).getYears();

    denseArrayCreate(true, denseURI);
    denseArrayCreate(true, denseURIToAppend);
    denseArrayWrite();
  }

  @After
  public void after() throws Exception {
    if (Array.exists(ctx, denseURI)) {
      TileDBObject.remove(ctx, denseURI);
    }
    if (Array.exists(ctx, denseURIToAppend)) {
      TileDBObject.remove(ctx, denseURIToAppend);
    }
    if (Array.exists(ctx, tempWrite)) {
      TileDBObject.remove(ctx, tempWrite);
    }
    if (Array.exists(ctx, writeURI)) {
      TileDBObject.remove(ctx, writeURI);
    }
    if (Array.exists(ctx, sparseURI)) {
      TileDBObject.remove(ctx, sparseURI);
    }
    ctx.close();
  }

  //    ========================================================
  //    READ-ONLY TESTS
  //    ========================================================

  /**
   * Creates a dense TileDB array.
   *
   * @param nullable true if the array will have nullable attributes.
   * @param URI The array URI
   * @throws Exception
   */
  public void denseArrayCreate(boolean nullable, String URI) throws Exception {
    Dimension<Integer> rows =
        new Dimension<>(ctx, "rows", Integer.class, new Pair<Integer, Integer>(1, 2), 2);

    // Create and set getDomain
    Domain domain = new Domain(ctx);
    domain.addDimension(rows);

    Attribute a1 = new Attribute(ctx, "a1", Integer.class);
    Attribute a2 = new Attribute(ctx, "MS", Datatype.TILEDB_DATETIME_MS);
    Attribute a3 = new Attribute(ctx, "SEC", Datatype.TILEDB_DATETIME_SEC);
    Attribute a4 = new Attribute(ctx, "MIN", Datatype.TILEDB_DATETIME_MIN);
    Attribute a5 = new Attribute(ctx, "HR", Datatype.TILEDB_DATETIME_HR);
    Attribute a6 = new Attribute(ctx, "DAY", Datatype.TILEDB_DATETIME_DAY);
    Attribute a7 = new Attribute(ctx, "US", Datatype.TILEDB_DATETIME_US);
    Attribute a8 = new Attribute(ctx, "NS", Datatype.TILEDB_DATETIME_NS);
    Attribute a9 = new Attribute(ctx, "WEEK", Datatype.TILEDB_DATETIME_WEEK);
    Attribute a10 = new Attribute(ctx, "MONTH", Datatype.TILEDB_DATETIME_MONTH);
    Attribute a11 = new Attribute(ctx, "YEAR", Datatype.TILEDB_DATETIME_YEAR);

    if (nullable) {
      a1.setNullable(true);
    }

    ArraySchema schema = new ArraySchema(ctx, TILEDB_DENSE);
    schema.setTileOrder(TILEDB_ROW_MAJOR);
    schema.setCellOrder(TILEDB_ROW_MAJOR);
    schema.setDomain(domain);
    schema.addAttribute(a1);
    schema.addAttribute(a2);
    schema.addAttribute(a3);
    schema.addAttribute(a4);
    schema.addAttribute(a5);
    schema.addAttribute(a6);
    schema.addAttribute(a7);
    schema.addAttribute(a8);
    schema.addAttribute(a9);
    schema.addAttribute(a10);
    schema.addAttribute(a11);

    Array.create(URI, schema);
    schema.close();
    domain.close();
  }

  /**
   * Populates the dense array with data
   *
   * @throws Exception
   */
  public void denseArrayWrite() throws Exception {
    // Prepare cell buffers
    NativeArray a1 = new NativeArray(ctx, new int[] {1, 4}, Integer.class);

    NativeArray a2 = new NativeArray(ctx, new long[] {timeNowMil, 0}, Datatype.TILEDB_DATETIME_MS);

    NativeArray a3 =
        new NativeArray(ctx, new long[] {timeNowMil / 1000, 0}, Datatype.TILEDB_DATETIME_SEC);

    NativeArray a4 =
        new NativeArray(
            ctx, new long[] {timeNowMil / (1000 * 60), 0}, Datatype.TILEDB_DATETIME_MIN);

    NativeArray a5 =
        new NativeArray(
            ctx, new long[] {timeNowMil / (1000 * 60 * 60), 0}, Datatype.TILEDB_DATETIME_HR);

    NativeArray a6 = new NativeArray(ctx, new long[] {days, 0}, Datatype.TILEDB_DATETIME_DAY);

    NativeArray a7 =
        new NativeArray(ctx, new long[] {timeNowMil * 1000, 0}, Datatype.TILEDB_DATETIME_US);

    NativeArray a8 =
        new NativeArray(ctx, new long[] {timeNowMil * 1000000, 0}, Datatype.TILEDB_DATETIME_NS);

    NativeArray a9 = new NativeArray(ctx, new long[] {weeks, 0}, Datatype.TILEDB_DATETIME_WEEK);

    NativeArray a10 = new NativeArray(ctx, new long[] {months, 0}, Datatype.TILEDB_DATETIME_MONTH);

    NativeArray a11 = new NativeArray(ctx, new long[] {years, 0}, Datatype.TILEDB_DATETIME_YEAR);

    // Create query
    try (Array array = new Array(ctx, denseURI, TILEDB_WRITE);
        Query query = new Query(array)) {
      query.setLayout(TILEDB_ROW_MAJOR);
      NativeArray a1Bytemap = new NativeArray(ctx, new short[] {1, 0}, Datatype.TILEDB_UINT8);

      query.setBufferNullable("a1", a1, a1Bytemap);
      query.setBuffer("MS", a2);
      query.setBuffer("SEC", a3);
      query.setBuffer("MIN", a4);
      query.setBuffer("HR", a5);
      query.setBuffer("DAY", a6);
      query.setBuffer("US", a7);
      query.setBuffer("NS", a8);
      query.setBuffer("WEEK", a9);
      query.setBuffer("MONTH", a10);
      query.setBuffer("YEAR", a11);
      query.submit();
    }
  }

  /**
   * Read test for the dense array.
   *
   * @throws Exception
   */
  @Test
  public void testDateTypesDenseRead() {
    Dataset<Row> dfRead = session().read().format("io.tiledb.spark").option("uri", denseURI).load();
    dfRead.show();
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();
    Assert.assertEquals(2, rows.size());

    // 1st row
    Row row = rows.get(0);
    Assert.assertEquals(1, row.getInt(0));
    Assert.assertEquals(1, row.getInt(1));
    Timestamp expected = new Timestamp(timeNowMil);
    checkRow(expected, row);

    // 2nd row
    row = rows.get(1);
    Assert.assertEquals(2, row.getInt(0));
    Assert.assertNull(row.get(1));
    expected = new Timestamp(0);
    checkRow(expected, row);
    return;
  }

  /**
   * Checks the rows of the dense array. Spark reads all datetype types as milliseconds. Thus, when
   * we compare to e.g. weeks we need to format the milliseconds to weeks format.
   *
   * @param expected The expected timestamp
   * @param row The row we check
   */
  private void checkRow(Timestamp expected, Row row) {
    Assert.assertEquals(
        expected.toLocalDateTime().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss:SSS")),
        row.getTimestamp(2)
            .toLocalDateTime()
            .format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss:SSS")));
    Assert.assertEquals(
        expected.toLocalDateTime().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")),
        row.getTimestamp(3)
            .toLocalDateTime()
            .format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")));
    Assert.assertEquals(
        expected.toLocalDateTime().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm")),
        row.getTimestamp(4)
            .toLocalDateTime()
            .format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm")));
    Assert.assertEquals(
        expected.toLocalDateTime().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH")),
        row.getTimestamp(5).toLocalDateTime().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH")));
    Assert.assertEquals(
        expected.toLocalDateTime().format(DateTimeFormatter.ofPattern("dd-MM-yyyy")),
        row.getTimestamp(6).toLocalDateTime().format(DateTimeFormatter.ofPattern("dd-MM-yyyy")));
    Assert.assertEquals(
        expected
            .toLocalDateTime()
            .format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss:SSSSSS")),
        row.getTimestamp(7)
            .toLocalDateTime()
            .format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss:SSSSSS")));
    Assert.assertEquals(
        expected
            .toLocalDateTime()
            .format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss:SSSSSSSSS")),
        row.getTimestamp(8)
            .toLocalDateTime()
            .format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss:SSSSSSSSS")));
    // Less accuracy is needed for weeks. The beginning of a week might be in month X-1. However the
    // current month might be X. That will fail the test.
    Assert.assertEquals(
        expected.toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy")),
        row.getTimestamp(9).toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy")));
    Assert.assertEquals(
        expected.toLocalDateTime().format(DateTimeFormatter.ofPattern("MM-yyyy")),
        row.getTimestamp(10).toLocalDateTime().format(DateTimeFormatter.ofPattern("MM-yyyy")));
    Assert.assertEquals(
        expected.toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy")),
        row.getTimestamp(11).toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy")));
  }

  //    ========================================================
  //    WRITE-ONLY TESTS
  //    ========================================================

  /**
   * Dense dataset with fixed-size attributes.
   *
   * @param ss The spark Session
   * @return The created dataframe.
   */
  public Dataset<Row> createDenseDataset(SparkSession ss) {
    // only the timestamp type is used to refer to dates in spark. TimestampType is in microseconds.
    StructField[] structFields =
        new StructField[] {
          new StructField("rows", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("timestamp1", DataTypes.TimestampType, true, Metadata.empty()),
          new StructField("datetype1", DataTypes.DateType, true, Metadata.empty()),
        };
    List<Row> rows = new ArrayList<>();

    rows.add(RowFactory.create(1, new Timestamp(0), new Date(0)));
    rows.add(RowFactory.create(2, new Timestamp(timeNowMil), new Date(timeNowMil)));
    rows.add(RowFactory.create(3, null, null));
    rows.add(
        RowFactory.create(
            4, new Timestamp((60 * 60 * 24 * 1000) + 123), new Date((60 * 60 * 24 * 1000) + 123)));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df;
  }

  /** Writes an array after creating it in spark. */
  @Test
  public void denseWriteTest() {
    Dataset<Row> dfReadFirst = createDenseDataset(session());
    // dffReadFirst.show();
    dfReadFirst
        .write()
        .format("io.tiledb.spark")
        .option("uri", tempWrite)
        .option("schema.dim.0.name", "rows")
        .option("schema.dim.0.min", 1)
        .option("schema.dim.0.max", 4)
        .option("schema.dim.0.extent", 2)
        .option("schema.attr.a.filter_list", "(byteshuffle, -1), (gzip, -10)")
        .option("schema.cell_order", "row-major")
        .option("schema.tile_order", "row-major")
        .mode("overwrite")
        .save();

    Dataset<Row> dfRead =
        session().read().format("io.tiledb.spark").option("uri", tempWrite).load();
    dfReadFirst.show();
    dfRead.show();
    Dataset<Row> dfReadConverted =
        dfRead.selectExpr("rows", "timestamp1", "split(datetype1, ' ')[0] as datetype1");
    Dataset<Row> dfReadFirstConverted =
        dfReadFirst.selectExpr("rows", "timestamp1", "split(datetype1, ' ')[0] as datetype1");
    dfReadFirstConverted.show();
    dfReadConverted.show();
    Assert.assertEquals(dfReadFirstConverted.count(), dfReadConverted.count());
    Assert.assertTrue(assertDataFrameEquals(dfReadConverted, dfReadFirstConverted));
    return;
  }

  //    ========================================================
  //    READ-WRITE TESTS
  //    ========================================================

  /**
   * Writes an array after reading it from TileDB.
   *
   * @throws TileDBError
   */
  @Test
  public void testDateTypesDenseReadWrite() throws TileDBError {
    Dataset<Row> dfReadFirst =
        session().read().format("io.tiledb.spark").option("uri", denseURI).load();

    dfReadFirst.cache();

    dfReadFirst
        .write()
        .format("io.tiledb.spark")
        .option("uri", writeURI)
        .option("schema.dim.0.name", "rows")
        .option("schema.dim.0.min", 1)
        .option("schema.dim.0.max", 2)
        .option("schema.dim.0.extent", 2)
        .option("schema.cell_order", "row-major")
        .option("schema.tile_order", "row-major")
        .mode("overwrite")
        .save();

    Dataset<Row> dfRead = session().read().format("io.tiledb.spark").option("uri", writeURI).load();

    dfRead.show();
    Assert.assertTrue(assertDataFrameEquals(dfReadFirst, dfRead));
    if (Array.exists(ctx, writeURI)) {
      TileDBObject.remove(ctx, writeURI);
    }
  }

  //    ========================================================
  //    APPEND TESTS
  //    ========================================================

  /**
   * Creates a sparse array.
   *
   * @throws TileDBError
   */
  public void sparseArrayCreate() throws TileDBError {

    Dimension<Integer> d1 =
        new Dimension<>(ctx, "d1", Integer.class, new Pair<Integer, Integer>(1, 8), 2);

    // Create and set getDomain
    Domain domain = new Domain(ctx);
    domain.addDimension(d1);

    Attribute a1 = new Attribute(ctx, "a1", Integer.class);
    Attribute a2 = new Attribute(ctx, "SEC", Datatype.TILEDB_DATETIME_SEC);
    Attribute a3 = new Attribute(ctx, "WEEK", Datatype.TILEDB_DATETIME_WEEK);
    Attribute a4 = new Attribute(ctx, "DAY", Datatype.TILEDB_DATETIME_DAY);

    ArraySchema schema = new ArraySchema(ctx, TILEDB_SPARSE);
    schema.setTileOrder(TILEDB_ROW_MAJOR);
    schema.setCellOrder(TILEDB_ROW_MAJOR);
    schema.setDomain(domain);
    schema.addAttribute(a1);
    schema.addAttribute(a2);
    schema.addAttribute(a3);
    schema.addAttribute(a4);

    Array.create(sparseURI, schema);
    schema.close();
    domain.close();
  }

  /**
   * Populates the sparse array with data.
   *
   * @throws TileDBError
   */
  public void sparseArrayWrite() throws TileDBError {
    NativeArray d_data = new NativeArray(ctx, new int[] {1, 2}, Integer.class);

    // Prepare cell buffers
    NativeArray a1 = new NativeArray(ctx, new int[] {1, 4}, Integer.class);
    NativeArray a2 =
        new NativeArray(ctx, new long[] {timeNowMil / 1000, 0}, Datatype.TILEDB_DATETIME_SEC);
    NativeArray a3 = new NativeArray(ctx, new long[] {weeks, 0}, Datatype.TILEDB_DATETIME_WEEK);
    NativeArray a4 = new NativeArray(ctx, new long[] {days, 0}, Datatype.TILEDB_DATETIME_DAY);

    // Create query
    Array array = new Array(ctx, sparseURI, TILEDB_WRITE);
    Query query = new Query(array);
    query.setLayout(TILEDB_GLOBAL_ORDER);

    query.setBuffer("d1", d_data);
    query.setBuffer("a1", a1);
    query.setBuffer("SEC", a2);
    query.setBuffer("WEEK", a3);
    query.setBuffer("DAY", a4);

    // Submit query
    query.submit();

    query.finalizeQuery();
    query.close();
    array.close();
  }

  /**
   * Appends a row in the dense array.
   *
   * @throws TileDBError
   */
  //  @Test
  //  public void testDateTypesDenseAppend() throws TileDBError {
  //    long randomMil = 1082039767000L;
  //    Timestamp randomDate = new Timestamp(randomMil);
  //    sparseArrayCreate();
  //    sparseArrayWrite();
  //
  //    StructField[] structFields =
  //        new StructField[] {
  //          new StructField("d1", DataTypes.IntegerType, false, Metadata.empty()),
  //          new StructField("a1", DataTypes.IntegerType, false, Metadata.empty()),
  //          new StructField("SEC", DataTypes.TimestampType, false, Metadata.empty()),
  //          new StructField("WEEK", DataTypes.TimestampType, false, Metadata.empty()),
  //          new StructField(
  //              "DAY", DataTypes.DateType, false, Metadata.empty()) // days need to be DateType!
  //        };
  //    List<Row> rows = new ArrayList<>();
  //    rows.add(RowFactory.create(3, 2, randomDate, randomDate, new Date(randomMil)));
  //    StructType structType = new StructType(structFields);
  //    Dataset<Row> dfToAppend = session().createDataFrame(rows, structType);
  //    dfToAppend.show();
  //
  //    dfToAppend
  //        .write()
  //        .format("io.tiledb.spark")
  //        .option("uri", sparseURI)
  //        .option("schema.dim.0.name", "rows")
  //        .option("schema.dim.0.min", 1)
  //        .option("schema.dim.0.max", 8)
  //        .option("schema.dim.0.extent", 2)
  //        .option("schema.cell_order", "row-major")
  //        .option("schema.tile_order", "row-major")
  //        .mode("overwrite")
  //        .save();
  //
  //    Dataset<Row> dfRead =
  //        session().read().format("io.tiledb.spark").option("uri", sparseURI).load();
  //    dfRead.show();
  //    dfRead.createOrReplaceTempView("tmp");
  //    List<Row> rowsOfnew = session().sql("SELECT * FROM tmp").collectAsList();
  //    // original rows were 2, so we expect 3 since we appended 1 line
  //    Assert.assertEquals(3, rowsOfnew.size());
  //
  //    Timestamp dateNow = new Timestamp(timeNowMil);
  //    // 1st row
  //    Row row = rowsOfnew.get(0);
  //    Assert.assertEquals(1, row.getInt(0));
  //    Assert.assertEquals(1, row.getInt(1));
  //    Assert.assertEquals(
  //        dateNow.toLocalDateTime().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")),
  //        row.getTimestamp(2)
  //            .toLocalDateTime()
  //            .format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")));
  //    // Less accuracy is needed for weeks. The beginning of a week might be in month X-1. However
  // the
  //    // current month might be X. That will fail the test.
  //    Assert.assertEquals(
  //        dateNow.toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy")),
  //        row.getTimestamp(3).toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy")));
  //
  //    // 2nd row
  //    row = rowsOfnew.get(1);
  //    Assert.assertEquals(2, row.getInt(0));
  //    Assert.assertEquals(4, row.getInt(1));
  //    Assert.assertEquals(
  //        new Timestamp(0)
  //            .toLocalDateTime()
  //            .format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")),
  //        row.getTimestamp(2)
  //            .toLocalDateTime()
  //            .format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")));
  //    Assert.assertEquals(
  //        new Timestamp(0).toLocalDateTime().format(DateTimeFormatter.ofPattern("MM-yyyy")),
  //        row.getTimestamp(3).toLocalDateTime().format(DateTimeFormatter.ofPattern("MM-yyyy")));
  //
  //    // 3rd row
  //    row = rowsOfnew.get(2);
  //    Assert.assertEquals(3, row.getInt(0));
  //    Assert.assertEquals(2, row.getInt(1));
  //    Assert.assertEquals(
  //        randomDate.toLocalDateTime().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")),
  //        row.getTimestamp(2)
  //            .toLocalDateTime()
  //            .format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")));
  //    Assert.assertEquals(
  //        randomDate.toLocalDateTime().format(DateTimeFormatter.ofPattern("MM-yyyy")),
  //        row.getTimestamp(3).toLocalDateTime().format(DateTimeFormatter.ofPattern("MM-yyyy")));
  //
  //    if (Array.exists(ctx, writeURI)) {
  //      TileDBObject.remove(ctx, writeURI);
  //    }
  //  }
}
