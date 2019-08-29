package io.tiledb.spark;

import io.tiledb.java.api.Layout;
import io.tiledb.java.api.Pair;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

public class TileDBDataSourceOptions implements Serializable {

  private static final int DEFAULT_PARTITIONS = 10;
  // we need to serialize the options to each partition reader / writer
  // DataSourceOptions is not serializable so we convert to a Java HashMap
  private HashMap<String, String> optionMap;

  // Query buffer size capacity in bytes (default 10mb)
  private static final int QUERY_BUFFER_SIZE = 1024 * 1024 * 10;

  /**
   * Parses TileDB Spark DataSource and TileDB config options (prefixed with `tiledb.confg = value`)
   *
   * @param options Spark DataSource options
   */
  public TileDBDataSourceOptions(DataSourceOptions options) {
    // copy the DataSource object values into a HashMap
    optionMap = new HashMap<>(options.asMap());
  }

  /**
   * @return Optional URI to TileDB array resource
   * @throws URISyntaxException
   */
  public Optional<URI> getArrayURI() throws URISyntaxException {
    if (optionMap.containsKey("uri")) {
      return Optional.of(new URI(optionMap.get("uri")));
    }
    return Optional.empty();
  }

  /** @return Read buffer size * */
  public long getReadBufferSizes() {
    if (optionMap.containsKey("read_buffer_size")) {
      return Long.parseLong(optionMap.get("read_buffer_size"));
    }
    return QUERY_BUFFER_SIZE;
  }

  /** @return Allow read buffers to be reallocated if a query is incomplete due to buffer size * */
  public boolean getAllowReadBufferReallocation() {
    if (optionMap.containsKey("allow_read_buffer_realloc")) {
      return Boolean.parseBoolean(optionMap.get("allow_read_buffer_realloc"));
    }
    return true;
  }

  /** @return partition count * */
  public int getPartitionCount() {
    if (optionMap.containsKey("partition_count")) {
      return Integer.parseInt(optionMap.get("partition_count"));
    }
    return DEFAULT_PARTITIONS;
  }

  /** @return Optional TileDB.Layout description for overriding dataframe sorted order * */
  public Optional<io.tiledb.java.api.Layout> getArrayLayout() {
    if (optionMap.containsKey("order")) {
      String val = optionMap.get("order");
      // accept either the python string values or tiledb enum string value (uppercase) for
      // consistency with python api
      if (val.equalsIgnoreCase("row-major") || val.equalsIgnoreCase("TILEDB_ROW_MAJOR")) {
        return Optional.of(Layout.TILEDB_ROW_MAJOR);
      } else if (val.equalsIgnoreCase("col-major") || val.equalsIgnoreCase("TILEDB_COL_MAJOR")) {
        return Optional.of(Layout.TILEDB_COL_MAJOR);
      } else if (val.equalsIgnoreCase("unordered") || val.equalsIgnoreCase("TILEDB_UNORDERED")) {
        return Optional.of(Layout.TILEDB_UNORDERED);
      } else {
        throw new IllegalArgumentException(
            "Unknown TileDB result layout order, valid values are 'row-major', 'col-major' and 'unordered', got: "
                + val);
      }
    }
    return Optional.empty();
  }

  /**
   * @return Optional List of OptionDimPartitions describing the dimension name, index and number of
   *     partitions *
   */
  public Optional<List<OptionDimPartition>> getDimPartitions() {
    List<Pair<String, String>> results =
        collectOptionsWithPrefixSuffix(optionMap, "partition.", null);
    if (results.isEmpty()) {
      return Optional.empty();
    }
    List<OptionDimPartition> dimPartitions = new ArrayList<>();
    for (Pair<String, String> entry : results) {
      dimPartitions.add(new OptionDimPartition(entry.getFirst(), entry.getSecond()));
    }
    return Optional.of(dimPartitions);
  }

  /** @return Optional List of Dimension names for creating a TileDB ArraySchema */
  public Optional<List<Pair<String, Integer>>> getSchemaDimensions() {
    List<Pair<String, String>> results =
        collectOptionsWithPrefixSuffix(optionMap, "schema.dim.", ".name");
    if (results.isEmpty()) {
      return Optional.empty();
    }
    ArrayList<Pair<String, Integer>> schemaDimensions = new ArrayList<>();
    for (Pair<String, String> entry : results) {
      String dimIntString = entry.getFirst();
      String val = entry.getSecond();
      Integer dimIdx;
      try {
        dimIdx = Integer.parseInt(dimIntString);
      } catch (NumberFormatException err) {
        continue;
      }
      schemaDimensions.add(new Pair<>(val, dimIdx));
    }
    return Optional.of(schemaDimensions);
  }

  public long getWriteBufferSize() {
    if (optionMap.containsKey("write_buffer_size")) {
      return Long.parseLong(optionMap.get("write_buffer_size"));
    }
    return QUERY_BUFFER_SIZE;
  }

  /** @return Optional String HashMap of tiledb config options and values * */
  public Map<String, String> getTileDBConfigMap() {

    HashMap<String, String> configMap = new HashMap<>();
    List<Pair<String, String>> results = collectOptionsWithPrefixSuffix(optionMap, "tiledb.", null);
    if (results.isEmpty()) {
      return configMap;
    }
    for (Pair<String, String> entry : results) {
      configMap.put(entry.getFirst(), entry.getSecond());
    }
    return configMap;
  }

  private static List<Pair<String, String>> collectOptionsWithPrefixSuffix(
      Map<String, String> options, String prefix, String suffix) {
    ArrayList<Pair<String, String>> results = new ArrayList<>();
    if (options.isEmpty() && (prefix == null)) {
      return results;
    }
    Iterator<Map.Entry<String, String>> entries = options.entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry<String, String> entry = entries.next();
      String key = entry.getKey();
      String val = entry.getValue();
      if (key.startsWith(prefix)) {
        String strippedKeyPrefix = key.substring(prefix.length());
        if (strippedKeyPrefix.length() == 0) {
          continue;
        }
        if (suffix == null || suffix.length() == 0) {
          results.add(new Pair<>(strippedKeyPrefix, val));
          continue;
        }
        if (!key.endsWith(suffix)) {
          continue;
        }
        String strippedKeySuffix =
            strippedKeyPrefix.substring(0, strippedKeyPrefix.length() - suffix.length());
        if (strippedKeySuffix.length() == 0) {
          continue;
        }
        results.add(new Pair<>(strippedKeySuffix, val));
      }
    }
    return results;
  }
}
