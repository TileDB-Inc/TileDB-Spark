package io.tiledb.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WriteScalarDataTypesTest extends SharedJavaSparkSession implements Serializable {

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  public Dataset<Row> createByteDataset(SparkSession ss) {
    StructField[] structFields =
        new StructField[] {
          new StructField("a1", DataTypes.ByteType, false, Metadata.empty()),
        };
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create((byte) 1));
    rows.add(RowFactory.create((byte) 2));
    rows.add(RowFactory.create((byte) 3));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id());
  }

  @Test
  public void testWriteByte() {
    Dataset<Row> dfWrite = createByteDataset(session());
    String arrayURI = temp.getRoot().toString();
    dfWrite
        .write()
        .format("io.tiledb.spark")
        .option("uri", arrayURI)
        .option("dimensions", "id")
        .mode(SaveMode.ErrorIfExists)
        .save();
  }

  public Dataset<Row> createShortDataset(SparkSession ss) {
    StructField[] structFields =
        new StructField[] {
          new StructField("a1", DataTypes.ShortType, false, Metadata.empty()),
        };
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create((short) 1));
    rows.add(RowFactory.create((short) 2));
    rows.add(RowFactory.create((short) 3));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id());
  }

  @Test
  public void testWriteShort() {
    Dataset<Row> dfWrite = createShortDataset(session());
    String arrayURI = temp.getRoot().toString();
    dfWrite
        .write()
        .format("io.tiledb.spark")
        .option("uri", arrayURI)
        .option("dimensions", "id")
        .mode(SaveMode.ErrorIfExists)
        .save();
  }

  public Dataset<Row> createIntegerDataset(SparkSession ss) {
    StructField[] structFields =
        new StructField[] {
          new StructField("a1", DataTypes.IntegerType, false, Metadata.empty()),
        };
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1));
    rows.add(RowFactory.create(2));
    rows.add(RowFactory.create(3));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id());
  }

  @Test
  public void testWriteInteger() {
    Dataset<Row> dfWrite = createIntegerDataset(session());
    String arrayURI = temp.getRoot().toString();
    dfWrite
        .write()
        .format("io.tiledb.spark")
        .option("uri", arrayURI)
        .option("dimensions", "id")
        .mode(SaveMode.ErrorIfExists)
        .save();
  }

  public Dataset<Row> createLongDataset(SparkSession ss) {
    StructField[] structFields =
        new StructField[] {
          new StructField("a1", DataTypes.LongType, false, Metadata.empty()),
        };
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1l));
    rows.add(RowFactory.create(2l));
    rows.add(RowFactory.create(3l));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id());
  }

  @Test
  public void testWriteLongDataset() {
    Dataset<Row> dfWrite = createLongDataset(session());
    String arrayURI = temp.getRoot().toString();
    dfWrite
        .write()
        .format("io.tiledb.spark")
        .option("uri", arrayURI)
        .option("dimensions", "id")
        .mode(SaveMode.ErrorIfExists)
        .save();
  }

  public Dataset<Row> createFloatDataset(SparkSession ss) {
    StructField[] structFields =
        new StructField[] {
          new StructField("a1", DataTypes.FloatType, false, Metadata.empty()),
        };
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1.0f));
    rows.add(RowFactory.create(2.0f));
    rows.add(RowFactory.create(3.0f));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id());
  }

  @Test
  public void testWriteFloatDataset() {
    Dataset<Row> dfWrite = createFloatDataset(session());
    String arrayURI = temp.getRoot().toString();
    dfWrite
        .write()
        .format("io.tiledb.spark")
        .option("uri", arrayURI)
        .option("dimensions", "id")
        .mode(SaveMode.ErrorIfExists)
        .save();
  }

  public Dataset<Row> createDoubleDataset(SparkSession ss) {
    StructField[] structFields =
        new StructField[] {
          new StructField("a1", DataTypes.DoubleType, false, Metadata.empty()),
        };
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1.0));
    rows.add(RowFactory.create(2.0));
    rows.add(RowFactory.create(3.0));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id());
  }

  @Test
  public void testWriteDoubleDataset() {
    Dataset<Row> dfWrite = createDoubleDataset(session());
    String arrayURI = temp.getRoot().toString();
    dfWrite
        .write()
        .format("io.tiledb.spark")
        .option("uri", arrayURI)
        .option("dimensions", "id")
        .mode(SaveMode.ErrorIfExists)
        .save();
  }

  public Dataset<Row> createStringDataset(SparkSession ss) {
    StructField[] structFields =
        new StructField[] {
          new StructField("a1", DataTypes.StringType, false, Metadata.empty()),
        };
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create("one"));
    rows.add(RowFactory.create("two"));
    rows.add(RowFactory.create("three"));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id());
  }

  @Test
  public void testWriteStringDataset() {
    Dataset<Row> dfWrite = createStringDataset(session());
    String arrayURI = temp.getRoot().toString();
    dfWrite
        .write()
        .format("io.tiledb.spark")
        .option("uri", arrayURI)
        .option("dimensions", "id")
        .mode(SaveMode.ErrorIfExists)
        .save();
  }
}
