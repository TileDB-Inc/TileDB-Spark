package io.tiledb.spark;

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
