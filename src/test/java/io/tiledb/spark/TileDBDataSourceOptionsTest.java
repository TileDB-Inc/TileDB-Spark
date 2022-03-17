package io.tiledb.spark;

import io.tiledb.java.api.Layout;
import io.tiledb.java.api.Pair;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  public void testEmptyDimensionPartitions() throws Exception {
    HashMap<String, String> optionMap = new HashMap<>();
    TileDBDataSourceOptions options = new TileDBDataSourceOptions(new DataSourceOptions(optionMap));
    Optional<List<OptionDimPartition>> partitions = options.getDimPartitions();
    Assert.assertFalse(partitions.isPresent());
  }

  @Test
  public void testDimensionPartitionIdx() throws Exception {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("partition.dim.0", "10");
    optionMap.put("partition.dim.foo", "2");
    TileDBDataSourceOptions options = new TileDBDataSourceOptions(new DataSourceOptions(optionMap));
    Optional<List<OptionDimPartition>> partitions = options.getDimPartitions();

    Assert.assertTrue(partitions.isPresent());

    OptionDimPartition p0 = partitions.get().get(0);
    Assert.assertFalse(p0.getDimName().isPresent());
    Assert.assertEquals(0, (int) p0.getDimIdx().get());
    Assert.assertEquals(10, (int) p0.getNPartitions());

    OptionDimPartition p1 = partitions.get().get(1);
    Assert.assertEquals("foo", p1.getDimName().get());
    Assert.assertEquals(2, (int) p1.getNPartitions());
  }

  @Test
  public void testEmptyTileDBSchemaDims() throws Exception {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("uri", "s3://foo/bar");
    TileDBDataSourceOptions options = new TileDBDataSourceOptions(new DataSourceOptions(optionMap));
    Assert.assertFalse(options.getSchemaDimensionNames().isPresent());
  }

  @Test
  public void testTileDBSchemaDims() throws Exception {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("uri", "s3://foo/bar");
    optionMap.put("schema.dim.0.name", "rows");
    optionMap.put("schema.dim.1.name", "cols");
    TileDBDataSourceOptions options = new TileDBDataSourceOptions(new DataSourceOptions(optionMap));
    Assert.assertTrue(options.getSchemaDimensionNames().isPresent());
    List<Pair<String, Integer>> schemaDims = options.getSchemaDimensionNames().get();
    Assert.assertEquals("rows", schemaDims.get(0).getFirst());
    Assert.assertEquals(Integer.valueOf(0), schemaDims.get(0).getSecond());
    Assert.assertEquals("cols", schemaDims.get(1).getFirst());
    Assert.assertEquals(Integer.valueOf(1), schemaDims.get(1).getSecond());
  }

  @Test
  public void testTileDBSchemaExtent() throws Exception {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("uri", "s3://foo/bar");
    optionMap.put("schema.dim.0.extent", "10");
    optionMap.put("schema.dim.1.extent", "1025.34");
    TileDBDataSourceOptions options = new TileDBDataSourceOptions(new DataSourceOptions(optionMap));
    Optional<Long> dim0Extent = options.getSchemaDimensionExtentLong(0);
    Optional<Double> dim1Extent = options.getSchemaDimensionExtentDouble(1);
    Assert.assertTrue(dim0Extent.isPresent());
    Assert.assertEquals(Long.valueOf(10), dim0Extent.get());
    Assert.assertTrue(dim1Extent.isPresent());
    Assert.assertEquals(Double.parseDouble("1025.34"), (double) dim1Extent.get(), 0.001);
    Assert.assertFalse(options.getSchemaDimensionExtentLong(2).isPresent());
    Assert.assertFalse(options.getSchemaDimensionExtentLong(1).isPresent());
  }

  @Test
  public void testEmptyTileDBConfigOption() throws Exception {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("uri", "s3://foo/bar");
    TileDBDataSourceOptions options = new TileDBDataSourceOptions(new DataSourceOptions(optionMap));
    Assert.assertTrue(
        options.getTileDBConfigMap(true).size()
            == 3); // 3 parameters for var attributes on arrowBuffs
  }

  @Test
  public void testTileDBConfigOption() throws Exception {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("uri", "s3://foo/bar");
    optionMap.put("tiledb.sm.dedup_coords", "true");
    optionMap.put("tiledb.sm.check_coord_dups", "false");
    TileDBDataSourceOptions options = new TileDBDataSourceOptions(new DataSourceOptions(optionMap));

    Map<String, String> tiledbOptions = options.getTileDBConfigMap(true);
    Assert.assertEquals("false", tiledbOptions.get("sm.check_coord_dups"));
    Assert.assertEquals("true", tiledbOptions.get("sm.dedup_coords"));
  }

  @Test
  public void testSingleFilter() throws Exception {
    String filters = "(gzip, 2)";
    Optional<List<Pair<String, Integer>>> filterList =
        TileDBDataSourceOptions.tryParseFilterList(filters);
    Assert.assertTrue(filterList.isPresent());
    Assert.assertEquals(1, filterList.get().size());
    Assert.assertEquals("gzip", filterList.get().get(0).getFirst());
    Assert.assertEquals(Integer.valueOf(2), filterList.get().get(0).getSecond());
  }

  @Test
  public void testFilterList() throws Exception {
    String filters = "(byteshuffle, -1), (gzip, 2)";
    Optional<List<Pair<String, Integer>>> filterList =
        TileDBDataSourceOptions.tryParseFilterList(filters);
    Assert.assertTrue(filterList.isPresent());
    Assert.assertEquals(2, filterList.get().size());
    Assert.assertEquals("byteshuffle", filterList.get().get(0).getFirst());
    Assert.assertEquals(Integer.valueOf(-1), filterList.get().get(0).getSecond());
    Assert.assertEquals("gzip", filterList.get().get(1).getFirst());
    Assert.assertEquals(Integer.valueOf(2), filterList.get().get(1).getSecond());
  }
}
