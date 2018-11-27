package io.tiledb.spark;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class TestDataFrame {

  static boolean assertDataFrameEquals(Dataset<Row> a, Dataset<Row> b) {
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

      // collect locally and compare (expensive!)
      List<Row> localA = aa.collectAsList();
      List<Row> localB = bb.collectAsList();
      if (!localA.equals(localB)) {
        return false;
      }

    } finally {
      a.rdd().unpersist(true);
      b.rdd().unpersist(true);
    }
    return true;
  }
}
