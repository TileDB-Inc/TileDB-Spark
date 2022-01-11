package io.tiledb.spark;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.After;
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

  @Test
  public void test() {
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
    ds.show();
    ds.printSchema();

    ds.write()
        .format("io.tiledb.spark")
        .option("uri", URI)
        .option("schema.dim.0.name", "study_id")
        .mode(SaveMode.ErrorIfExists)
        .save();

    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.spark")
            .option("read_buffer_size", 8)
            .option("uri", URI)
            .load();

    //    dfRead.show();
  }
}
