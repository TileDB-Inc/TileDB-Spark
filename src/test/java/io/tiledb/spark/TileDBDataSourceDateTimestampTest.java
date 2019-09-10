package io.tiledb.spark;

import static io.tiledb.spark.TestDataFrame.assertDataFrameEquals;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TileDBDataSourceDateTimestampTest extends SharedJavaSparkSession
    implements Serializable {

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private final long currentTime = System.currentTimeMillis();

  protected void testWriteRead(Dataset<Row> dfWrite, String arrayURI) {
    // write dataset
    dfWrite
        .write()
        .format("io.tiledb.spark")
        .option("uri", arrayURI)
        .option("schema.dim.0.name", "id")
        .mode(SaveMode.ErrorIfExists)
        .save();

    // read saved byte dataset
    Dataset<Row> dfRead = session().read().format("io.tiledb.spark").option("uri", arrayURI).load();
    //    dfRead.show();
    Assert.assertEquals(dfWrite.count(), dfRead.count());
    Assert.assertTrue(assertDataFrameEquals(dfWrite, dfRead));
  }

  public Dataset<Row> createDateDataset(SparkSession ss) {
    StructField[] structFields =
        new StructField[] {
          new StructField("a1", DataTypes.DateType, false, Metadata.empty()),
        };
    List<Row> rows = new ArrayList<>();
    // Unix epoch 1969-12-31
    rows.add(RowFactory.create(new Date(0)));
    // Next day 1970-01-01
    rows.add(RowFactory.create(new Date(60 * 60 * 24 * 1000)));
    // Current date
    rows.add(RowFactory.create(new Date(currentTime)));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id());
  }

  @Test
  public void testDate() {
    String arrayURI = temp.getRoot().toString();
    testWriteRead(createDateDataset(session()), arrayURI);

    Dataset<Row> dfRead = session().read().format("io.tiledb.spark").option("uri", arrayURI).load();
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();
    Assert.assertEquals(3, rows.size());
    // A[0] == '1969-12-31'
    Row row = rows.get(0);
    Assert.assertEquals(0, row.getLong(0));
    Assert.assertEquals(new Date(0).toString(), row.getDate(1).toString());
    // A[1] == '1970-01-01'
    row = rows.get(1);
    Assert.assertEquals(1, row.getLong(0));
    Assert.assertEquals(new Date(60 * 60 * 24 * 1000).toString(), row.getDate(1).toString());
    // A[2] == Current_Date()
    row = rows.get(2);
    Assert.assertEquals(2, row.getLong(0));
    Assert.assertEquals(new Date(currentTime).toString(), row.getDate(1).toString());
  }

  public Dataset<Row> createTimestampDataset(SparkSession ss) {
    StructField[] structFields =
        new StructField[] {
          new StructField("a1", DataTypes.TimestampType, false, Metadata.empty()),
        };
    List<Row> rows = new ArrayList<>();
    // Unix epoch 1970-01-01 00:00:00.0
    rows.add(RowFactory.create(new Timestamp(0)));
    // Next day 1970-01-02 00:00:00.123
    rows.add(RowFactory.create(new Timestamp((60 * 60 * 24 * 1000) + 123)));
    // Current date
    rows.add(RowFactory.create(new Timestamp(currentTime)));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id());
  }

  @Test
  public void testTimestamp() {
    String arrayURI = temp.getRoot().toString();
    testWriteRead(createTimestampDataset(session()), arrayURI);

    Dataset<Row> dfRead = session().read().format("io.tiledb.spark").option("uri", arrayURI).load();
    dfRead.createOrReplaceTempView("tmp");
    List<Row> rows = session().sql("SELECT * FROM tmp").collectAsList();
    Assert.assertEquals(3, rows.size());
    // A[0] == '1970-01-01 00:00:00.0'
    Row row = rows.get(0);
    Assert.assertEquals(0, row.getLong(0));
    Assert.assertEquals(new Timestamp(0).toString(), row.getTimestamp(1).toString());
    // A[1] == '1970-01-02 00:00:00.123'
    row = rows.get(1);
    Assert.assertEquals(1, row.getLong(0));
    Assert.assertEquals(
        new Timestamp((60 * 60 * 24 * 1000) + 123).toString(), row.getTimestamp(1).toString());
    // A[2] == Current_Date()
    row = rows.get(2);
    Assert.assertEquals(2, row.getLong(0));
    Assert.assertEquals(new Timestamp(currentTime).toString(), row.getTimestamp(1).toString());
  }
}
