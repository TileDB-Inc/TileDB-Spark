package io.tiledb.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SparkPartitioningTest extends SharedJavaSparkSession implements Serializable {

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  protected void testWriteRead(Dataset<Row> persistedDF, int partitions, int expectedPartitions) {
    String arrayURI = temp.getRoot().toString();
    persistedDF
        .write()
        .format("io.tiledb.spark")
        .option("uri", arrayURI)
        .option("schema.dim.0.name", "id")
        .mode(SaveMode.ErrorIfExists)
        .save();

    Dataset<Row> inputDF =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("partition_count", partitions)
            .option("uri", arrayURI)
            .load();
    Assert.assertEquals(inputDF.rdd().partitions().length, expectedPartitions);
  }

  protected void testWriteRead2Dim(Dataset<Row> persistedDF, int partitions, int expectedPartitions) {
    String arrayURI = temp.getRoot().toString();
    persistedDF
        .write()
        .format("io.tiledb.spark")
        .option("uri", arrayURI)
        .option("schema.dim.0.name", "a1")
        .option("schema.dim.1.name", "a2")
        .mode(SaveMode.ErrorIfExists)
        .save();

    Dataset<Row> inputDF =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("partition_count", partitions)
            .option("uri", arrayURI)
            .load();
    Assert.assertEquals(inputDF.rdd().partitions().length, expectedPartitions);
  }

  public Dataset<Row> createByteDataset(SparkSession ss) {
    StructField[] structFields =
        new StructField[] {
          new StructField("a1", DataTypes.ByteType, false, Metadata.empty()),
        };
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create((byte) 0));
    rows.add(RowFactory.create((byte) 2));
    rows.add(RowFactory.create((byte) 3));
    rows.add(RowFactory.create((byte) 10));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id());
  }

  @Test
  public void testWriteByte1() {
    testWriteRead(createByteDataset(session()), 3, 3);
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
    rows.add(RowFactory.create((short) 10));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id());
  }

  @Test
  public void testPartitioningShort1() {
    testWriteRead(createShortDataset(session()), 3, 3);
  }

  @Test
  public void testWritePartitioning2() {
    testWriteRead(createShortDataset(session()), 10, 4);
  }

  @Test
  public void testPartitioningShort3() {
    testWriteRead(createShortDataset(session()), 2, 2);
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
    rows.add(RowFactory.create(10));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id());
  }

  @Test
  public void testPartitioningInteger1() {
    testWriteRead(createIntegerDataset(session()), 3, 3);
  }

  @Test
  public void testPartitioningInteger2() {
    testWriteRead(createIntegerDataset(session()), 10, 4);
  }

  @Test
  public void testPartitioningInteger3() {
    testWriteRead(createIntegerDataset(session()), 2, 2);
  }

  public Dataset<Row> createLongDataset(SparkSession ss) {
    StructField[] structFields =
        new StructField[] {
          new StructField("a1", DataTypes.LongType, false, Metadata.empty()),
        };
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1L));
    rows.add(RowFactory.create(2L));
    rows.add(RowFactory.create(3L));
    rows.add(RowFactory.create(10L));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id());
  }

  @Test
  public void testPartitioningLong1() {
    testWriteRead(createLongDataset(session()), 3, 3);
  }

  @Test
  public void testPartitioningLong2() {
    testWriteRead(createLongDataset(session()), 10, 4);
  }

  @Test
  public void testPartitioningLong3() {
    testWriteRead(createLongDataset(session()), 2, 2);
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
    rows.add(RowFactory.create(10.0));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id());
  }

  @Test
  public void testPartitioningDouble1() {
    testWriteRead(createDoubleDataset(session()), 3, 3);
  }

  @Test
  public void testPartitioningDouble2() {
    testWriteRead(createDoubleDataset(session()), 10, 4);
  }

  @Test
  public void testPartitioningDouble3() {
    testWriteRead(createDoubleDataset(session()), 2, 2);
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
    rows.add(RowFactory.create("four"));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id()).repartition(1);
  }

    public Dataset<Row> createIntegerDatasetMultiDim(SparkSession ss) {
        StructField[] structFields =
                new StructField[] {
                        new StructField("a1", DataTypes.IntegerType, false, Metadata.empty()),
                };
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1));
        rows.add(RowFactory.create(2));
        rows.add(RowFactory.create(3));
        rows.add(RowFactory.create(10));
        StructType structType = new StructType(structFields);
        Dataset<Row> df = ss.createDataFrame(rows, structType);
        return df.withColumn("id", functions.monotonically_increasing_id());
    }

    @Test
    public void testPartitioningIntegerMultiDim1() {
        testWriteRead(createIntegerDatasetMultiDim(session()), 3, 3);
    }

    @Test
    public void testPartitioningIntegerMultiDim2() {
        testWriteRead(createIntegerDatasetMultiDim(session()), 10, 4);
    }

    @Test
    public void testPartitioningIntegerMultiDim3() {
        testWriteRead(createIntegerDatasetMultiDim(session()), 2, 2);
    }

  @Test
  public void testWriteStringDataset1() {
    testWriteRead(createStringDataset(session()), 3, 3);
  }

  @Test
  public void testWriteStringDataset2() {
    testWriteRead(createStringDataset(session()), 10, 4);
  }

  @Test
  public void testWriteStringDataset3() {
    testWriteRead(createStringDataset(session()), 2, 2);
  }


  /* Multidimensional arrays */

  public Dataset<Row> createLongDatasetMultiDim(SparkSession ss) {
    StructField[] structFields =
            new StructField[] {
                    new StructField("a1", DataTypes.LongType, false, Metadata.empty()),
                    new StructField("a2", DataTypes.LongType, false, Metadata.empty()),
            };
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1L, 1L));
    rows.add(RowFactory.create(2L, 2L));
    rows.add(RowFactory.create(3L, 3L));
    rows.add(RowFactory.create(10L, 10L));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id());
  }

  @Test
  public void testPartitioningLongMultiDim1() {
    Dataset ds = createLongDatasetMultiDim(session());
    testWriteRead2Dim(ds, 3, 3);
  }

  @Test
  public void testPartitioningLongMultiDim2() {
    testWriteRead(createLongDataset(session()), 10, 4);
  }

  @Test
  public void testPartitioningLongMultiDim3() {
    testWriteRead(createLongDataset(session()), 2, 2);
  }

  public Dataset<Row> createDoubleDatasetMultiDim(SparkSession ss) {
    StructField[] structFields =
            new StructField[] {
                    new StructField("a1", DataTypes.DoubleType, false, Metadata.empty()),
                    new StructField("a2", DataTypes.DoubleType, false, Metadata.empty()),
            };
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1.0, 1.1));
    rows.add(RowFactory.create(2.0, 2.0));
    rows.add(RowFactory.create(3.0, 3.0));
    rows.add(RowFactory.create(10.0, 10.0));
    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df.withColumn("id", functions.monotonically_increasing_id());
  }

  @Test
  public void testPartitioningDoubleMultiDim1() {
    testWriteRead2Dim(createDoubleDatasetMultiDim(session()), 3, 3);
  }

  @Test
  public void testPartitioningDoubleMultiDim2() {
    testWriteRead(createDoubleDataset(session()), 10, 4);
  }

  @Test
  public void testPartitioningDoubleMultiDim3() {
    testWriteRead(createDoubleDataset(session()), 2, 2);
  }

    public Dataset<Row> createByteDatasetMultiDim(SparkSession ss) {
        StructField[] structFields =
                new StructField[] {
                        new StructField("a1", DataTypes.ByteType, false, Metadata.empty()),
                        new StructField("a2", DataTypes.ByteType, false, Metadata.empty()),
                };
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create((byte) 0, (byte) 0));
        rows.add(RowFactory.create((byte) 2, (byte) 2));
        rows.add(RowFactory.create((byte) 3, (byte) 3));
        rows.add(RowFactory.create((byte) 10, (byte) 10));
        StructType structType = new StructType(structFields);
        Dataset<Row> df = ss.createDataFrame(rows, structType);
        return df.withColumn("id", functions.monotonically_increasing_id());
    }

    @Test
    public void testWriteByteMultiDim1() {
        testWriteRead2Dim(createByteDatasetMultiDim(session()), 3, 3);
    }
}
