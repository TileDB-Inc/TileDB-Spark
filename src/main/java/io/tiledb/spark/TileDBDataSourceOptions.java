package io.tiledb.spark;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import io.tiledb.java.api.Config;
import io.tiledb.java.api.TileDBError;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

public class TileDBDataSourceOptions implements Serializable {

  // we need to serialize the options to each partition reader / writer
  // DataSourceOptions is not serializable so we convert to a Java HashMap
  private HashMap<String, String> optionMap;

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
   * @return Array resource URI *
   */
  public Optional<URI> getArrayURI() throws URISyntaxException {
    if (optionMap.containsKey("uri")) {
      return Optional.of(new URI(optionMap.get("uri")));
    }
    return Optional.empty();
  }

  /** @return A String HashMap of tiledb config options and values **/
  public Map<String,String> getTileDBConfigMap() throws TileDBError {
    HashMap<String,String> configMap = new HashMap<>();
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
