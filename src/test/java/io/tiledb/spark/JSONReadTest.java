package io.tiledb.spark;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JSONReadTest extends SharedJavaSparkSession {
  private final String URI = "json_test_array";

  private String pathFinder(String fileName) {
    Path arraysPath = Paths.get("src", "test", "resources", fileName);
    return "file://".concat(arraysPath.toAbsolutePath().toString());
  }

  @Before
  public void setup() throws Exception {
    if (Files.exists(Paths.get(URI))) {
      FileUtils.deleteDirectory(new File(URI));
    }
  }

  @After
  public void delete() throws Exception {
    if (Files.exists(Paths.get(URI))) {
      FileUtils.deleteDirectory(new File(URI));
    }
  }

  /** This test reads a json file of 97 lines, writes it and reads it back. */
  @Test
  public void JSONReadTest() {
    Dataset<Row> ds = session().read().json(pathFinder("sample.json"));

    // column types of the following columns are string arrays and boolean which are not supported
    // by TileDB-Spark
    ArrayList<String> columnsToCast = new ArrayList<>();
    columnsToCast.add("trait_efos");
    columnsToCast.add("ancestry_replication");
    columnsToCast.add("ancestry_initial");
    columnsToCast.add("has_sumstats");

    for (String c : ds.columns()) {
      if (columnsToCast.contains(c)) {
        ds = ds.withColumn(c, ds.col(c).cast("string"));
      }
    }

    ds.write()
        .format("io.tiledb.spark")
        .option("uri", URI)
        .option("schema.dim.0.name", "study_id")
        .mode("overwrite")
        .save();

    Dataset<Row> dfRead = session().read().format("io.tiledb.spark").option("uri", URI).load();
    // use default buffer read size

    // Spark dataframes are lazy which means that since we do not check the data of the dataframes
    // they will not be read if we dont print them.
    ds.show();
    dfRead.show();
    // When reading the dataframe from json spark adds -study_id- as a nullable attribute. However,
    // since TileDB will write an array with study_id as a dimension it can not be nullable. This
    // creates a difference in the schema, thus study_id is excluded in the comparison.
    ds = ds.drop("study_id");
    dfRead = dfRead.drop("study_id");

    // Here we are using a custom comparison method since the original throws an outOfMemoryError.
    Assert.assertTrue(assertDataFrameEqualsCustom(ds, dfRead));
  }

  /**
   * Checks whether two dataframes are equal. Attention, it does not check the data, only the schema
   * and columns
   *
   * @param a Dataframe a
   * @param b Dataframe b
   * @return True if dataframes are equal.
   */
  private boolean assertDataFrameEqualsCustom(Dataset<Row> a, Dataset<Row> b) {
    try {
      a.rdd().cache();
      b.rdd().cache();

      String[] aColNames = a.columns();
      String[] bColNames = b.columns();

      Arrays.sort(aColNames);
      Arrays.sort(bColNames);

      List<Column> aCols = Arrays.stream(aColNames).map(n -> a.col(n)).collect(Collectors.toList());
      List<Column> bCols = Arrays.stream(bColNames).map(n -> b.col(n)).collect(Collectors.toList());

      Dataset<Row> aa = a.select(aCols.toArray(new Column[aCols.size()]));
      Dataset<Row> bb = b.select(bCols.toArray(new Column[bCols.size()]));

      // check schema equality
      if (!aa.schema().toString().equalsIgnoreCase(bb.schema().toString())) {
        return false;
      }

      // check the number of rows
      long nrowsA = a.count();
      long nrowsB = b.count();
      if (nrowsA != nrowsB) {
        return false;
      }
    } finally {
      a.rdd().unpersist(true);
      b.rdd().unpersist(true);
    }
    return true;
  }
}
