package io.tiledb.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.Assert;
import org.junit.Test;

public class SimpleReadWriteTest extends SharedJavaSparkSession implements Serializable {

  public Dataset<Row> createDataset(SparkSession ss) {
    StructField[] structFields =
        new StructField[] {
          new StructField("d1", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("d2", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("a1", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("a2", DataTypes.StringType, false, Metadata.empty()),
          new StructField(
              "a3", DataTypes.createArrayType(DataTypes.FloatType, false), false, Metadata.empty()),
        };

    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1, 1, 0, "a", new float[] {0.1f, 0.2f}));
    rows.add(RowFactory.create(1, 2, 1, "bb", new float[] {1.1f, 1.2f}));
    rows.add(RowFactory.create(1, 3, 2, "ccc", new float[] {2.1f, 2.2f}));
    rows.add(RowFactory.create(1, 4, 3, "dddd", new float[] {3.1f, 3.2f}));
    rows.add(RowFactory.create(2, 1, 4, "e", new float[] {4.1f, 4.2f}));
    rows.add(RowFactory.create(2, 2, 5, "ff", new float[] {5.1f, 5.2f}));
    rows.add(RowFactory.create(2, 3, 6, "ggg", new float[] {6.1f, 6.2f}));
    rows.add(RowFactory.create(2, 4, 7, "hhhh", new float[] {7.1f, 7.2f}));
    rows.add(RowFactory.create(3, 1, 8, "i", new float[] {8.1f, 8.2f}));
    rows.add(RowFactory.create(3, 2, 9, "jj", new float[] {9.1f, 9.2f}));
    rows.add(RowFactory.create(3, 3, 10, "kkk", new float[] {10.1f, 10.2f}));
    rows.add(RowFactory.create(3, 4, 11, "llll", new float[] {11.1f, 11.2f}));
    rows.add(RowFactory.create(4, 1, 12, "m", new float[] {13.1f, 13.2f}));
    rows.add(RowFactory.create(4, 2, 13, "nn", new float[] {14.1f, 14.2f}));
    rows.add(RowFactory.create(4, 3, 14, "ooo", new float[] {15.1f, 15.2f}));
    rows.add(RowFactory.create(4, 4, 15, "pppp", new float[] {16.1f, 16.2f}));

    StructType structType = new StructType(structFields);
    Dataset<Row> df = ss.createDataFrame(rows, structType);
    return df;
  }

  @Test
  public void testCreateDataset() {
    Dataset<Row> df = createDataset(session());
    df.show(10);
    String[] fieldNames = df.schema().fieldNames();
    Assert.assertArrayEquals(fieldNames, new String[] {"d1", "d2", "a1", "a2", "a3"});
    Assert.assertEquals(df.collectAsList().size(), 16);
  }

  @Test
  public void testReadWrite() {
    Dataset<Row> dfWrite = createDataset(session());
    dfWrite.show(10);
    dfWrite
        .write()
        .format("io.tiledb.spark")
        .option("uri", "my_dense_array")
        .option("dimensions", "d1,d2")
        .option("subarray.d1.min", 1)
        .option("subarray.d1.max", 4)
        .option("subarray.d1.extent", 2)
        .option("subarray.d2.min", 1)
        .option("subarray.d2.max", 4)
        .option("subarray.d2.extent", 2)
        .mode(SaveMode.Overwrite)
        .save();
    Dataset<Row> dfRead =
        session().read().format("io.tiledb.spark").option("uri", "my_dense_array").load();
    dfRead.show(10);
    Assert.assertTrue(dfWrite.schema().canEqual(dfRead.schema()));
    Assert.assertTrue(dfWrite.collectAsList().containsAll(dfRead.collectAsList()));
  }
}
