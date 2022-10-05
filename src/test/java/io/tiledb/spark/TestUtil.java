package io.tiledb.spark;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TestUtil {
  /**
   * Sparse dataset with fixed-size attributes.
   *
   * @param ss The spark session.
   * @return The created dataframe.
   */
  public static Dataset<Row> createSparseDataset(SparkSession ss) {
    StructField[] structFields =
        new StructField[] {
          new StructField("d1", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("a1", DataTypes.FloatType, true, Metadata.empty()),
          new StructField("a2", DataTypes.StringType, true, Metadata.empty()),
        };
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1, null, "a"));
    rows.add(RowFactory.create(2, null, "b"));
    rows.add(RowFactory.create(3, null, "c"));
    rows.add(RowFactory.create(4, 4.0f, null));
    rows.add(RowFactory.create(5, 5.0f, null));
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
  public static Dataset<Row> createSparseDatasetVar(SparkSession ss) {
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
}
