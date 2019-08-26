package io.tiledb.spark;

import io.tiledb.java.api.Layout;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
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

  /** @return Optional String HashMap of tiledb config options and values * */
  public Map<String, String> getTileDBConfigMap() {
    HashMap<String, String> configMap = new HashMap<>();
    if (optionMap.isEmpty()) {
      return configMap;
    }
    Iterator<Map.Entry<String, String>> entries = optionMap.entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry<String, String> entry = entries.next();
      String key = entry.getKey();
      String val = entry.getValue();
      if (key.startsWith("tiledb.")) {
        // Strip out leading "tiledb." prefix
        String strippedKey = key.substring(7);
        configMap.put(strippedKey, val);
      }
    }
    return configMap;
  }
}
