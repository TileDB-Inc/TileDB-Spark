package io.tiledb.spark;

import io.tiledb.java.api.Layout;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.*;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.types.*;
import org.junit.Assert;
import org.junit.Test;

public class TileDBDataSourceOptionsTest {

  @Test
  public void testArrayURIOptionMissing() throws Exception {
    DataSourceOptions dsOptions = new DataSourceOptions(new HashMap<>());
    TileDBDataSourceOptions options = new TileDBDataSourceOptions(dsOptions);
    Assert.assertFalse(options.getArrayURI().isPresent());
  }

  @Test(expected = URISyntaxException.class)
  public void testArrayURIInvalid() throws Exception {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("uri", "s3://$%foo/baz/");
    TileDBDataSourceOptions options = new TileDBDataSourceOptions(new DataSourceOptions(optionMap));
    Optional<URI> uri = options.getArrayURI();
  }

  @Test
  public void testArrayURIOption() throws Exception {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("uri", "s3://foo/bar");
    TileDBDataSourceOptions options = new TileDBDataSourceOptions(new DataSourceOptions(optionMap));
    Optional<URI> uri = options.getArrayURI();
    Assert.assertTrue(uri.isPresent());
    Assert.assertEquals(URI.create("s3://foo/bar"), uri.get());
  }

  @Test
  public void testNoArrayLayoutOption() throws Exception {
    HashMap<String, String> optionMap = new HashMap<>();
    TileDBDataSourceOptions options = new TileDBDataSourceOptions(new DataSourceOptions(optionMap));
    Assert.assertFalse(options.getArrayLayout().isPresent());
  }

  @Test
  public void testValidArrayLayoutOptions() throws Exception {
    String[] validLayoutStrings =
        new String[] {
          "row-major",
          "TILEDB_ROW_MAJOR",
          "col-major",
          "TILEDB_COL_MAJOR",
          "unordered",
          "TILEDB_UNORDERED"
        };
    Layout[] expectedLayouts =
        new Layout[] {
          Layout.TILEDB_ROW_MAJOR,
          Layout.TILEDB_ROW_MAJOR,
          Layout.TILEDB_COL_MAJOR,
          Layout.TILEDB_COL_MAJOR,
          Layout.TILEDB_UNORDERED,
          Layout.TILEDB_UNORDERED
        };
    for (int i = 0; i < validLayoutStrings.length; i++) {
      HashMap<String, String> optionMap = new HashMap<>();
      optionMap.put("order", validLayoutStrings[i]);
      TileDBDataSourceOptions options =
          new TileDBDataSourceOptions(new DataSourceOptions(optionMap));
      Optional<Layout> layout = options.getArrayLayout();
      Assert.assertTrue(layout.isPresent());
      Assert.assertEquals(expectedLayouts[i], layout.get());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidLayoutOptions() throws Exception {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("order", "bad-layout-option");
    TileDBDataSourceOptions options = new TileDBDataSourceOptions(new DataSourceOptions(optionMap));
    Optional<Layout> layout = options.getArrayLayout();
  }

  @Test
  public void testEmptyTileDBConfigOption() throws Exception {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("uri", "s3://foo/bar");
    TileDBDataSourceOptions options = new TileDBDataSourceOptions(new DataSourceOptions(optionMap));
    Assert.assertTrue(options.getTileDBConfigMap().isEmpty());
  }

  @Test
  public void testTileDBConfigOption() throws Exception {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("uri", "s3://foo/bar");
    optionMap.put("tiledb.sm.dedup_coords", "true");
    optionMap.put("tiledb.sm.check_coord_dups", "false");
    TileDBDataSourceOptions options = new TileDBDataSourceOptions(new DataSourceOptions(optionMap));

    Map<String, String> tiledbOptions = options.getTileDBConfigMap();
    Assert.assertEquals(tiledbOptions.get("sm.check_coord_dups"), "false");
    Assert.assertEquals(tiledbOptions.get("sm.dedup_coords"), "true");
  }
}
