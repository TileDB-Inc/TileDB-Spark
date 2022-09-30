package io.tiledb.spark;

import io.tiledb.java.api.Layout;
import io.tiledb.java.api.Pair;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TileDBDataSourceOptions implements Serializable {

  // Query buffer size capacity in bytes (default 10mb)
  private static final int QUERY_BUFFER_SIZE = 1024 * 1024 * 10;
  private static final int DEFAULT_PARTITIONS = 10;

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
   * @return Optional URI to TileDB array resource
   * @throws URISyntaxException A URISyntaxException exception
   */
  public Optional<String> getArrayURI() throws URISyntaxException {
    if (optionMap.containsKey("path")) {
      return Optional.of(optionMap.get("path"));
    }
    // the following 'if' should enable passing the uri as an option as it was implemented
    // originally. The 'spark' way requires the uri to be inside the 'load()' method call.
    // this is left here for backwards compatibility.
    if (optionMap.containsKey("uri")) {
      return Optional.of(optionMap.get("uri"));
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

  /** @return True if the legacy non-arrow reader is requested * */
  public boolean getLegacyReader() {
    if (optionMap.containsKey("legacy_reader")) {
      return Boolean.parseBoolean(optionMap.get("legacy_reader"));
    }
    return false;
  }

  /** @return True if only the array metadata is requested * */
  public boolean printMetadata() {
    if (optionMap.containsKey("print_array_metadata")) {
      return Boolean.parseBoolean(optionMap.get("print_array_metadata"));
    }
    return false;
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
      Optional<Layout> arrayResultLayout = tryParseOptionLayout(val);
      if (!arrayResultLayout.isPresent()) {
        throw new IllegalArgumentException(
            "Unknown TileDB result layout order, valid values are 'row-major', 'col-major', 'global-order' and 'unordered', got: "
                + val);
      }
      return arrayResultLayout;
    }
    return Optional.empty();
  }

  /**
   * @return Optional List of OptionDimPartitions describing the dimension name, index and number of
   *     partitions *
   */
  public Optional<List<OptionDimPartition>> getDimPartitions() {
    List<Pair<String, String>> results =
        collectOptionsWithKeyPrefixSuffix(optionMap, "partition.", null);
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
  public Optional<List<Pair<String, Integer>>> getSchemaDimensionNames() {
    List<Pair<String, String>> results =
        collectOptionsWithKeyPrefixSuffix(optionMap, "schema.dim.", ".name");
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
    schemaDimensions.sort(Comparator.comparing(Pair::getSecond));
    return Optional.of(schemaDimensions);
  }

  /** @return A map with the metadata keys and values */
  public Map<String, String> getMetadata() {
    Map<String, String> results =
        collectOptionsWithKeyPrefixSuffixMap(optionMap, "metadata_value.", null);
    return results;
  }

  /** @return A map with the metadata keys and datatypes */
  public Map<String, String> getMetadataTypes() {
    Map<String, String> results =
        collectOptionsWithKeyPrefixSuffixMap(optionMap, "metadata_type.", null);
    return results;
  }

  /**
   * Returns the timestamp start necessary for time travelling in intervals.
   *
   * @return the start timestamp.
   */
  public Optional<Long> getTimestampStart() {
    String key = "timestamp_start";
    if (!optionMap.containsKey(key)) {
      return Optional.empty();
    }
    Long val;
    try {
      val = Long.parseLong(optionMap.get(key));
    } catch (NumberFormatException err) {
      return Optional.empty();
    }
    return Optional.of(val);
  }

  /**
   * Returns the timestamp end necessary for time travelling.
   *
   * @return the end timestamp.
   */
  public Optional<Long> getTimestampEnd() {
    String key = "timestamp_end";
    if (!optionMap.containsKey(key)) {
      return Optional.empty();
    }
    Long val;
    try {
      val = Long.parseLong(optionMap.get(key));
    } catch (NumberFormatException err) {
      return Optional.empty();
    }
    return Optional.of(val);
  }

  public Optional<Long> getSchemaDimensionMinDomainLong(int dimIdx) {
    String dimExtentKey = "schema.dim." + dimIdx + ".min";
    return tryParseOptionKeyLong(optionMap, dimExtentKey);
  }

  public Optional<Long> getSchemaDimensionMaxDomainLong(int dimIdx) {
    String dimExtentKey = "schema.dim." + dimIdx + ".max";
    return tryParseOptionKeyLong(optionMap, dimExtentKey);
  }

  // For dense reads only
  public Optional<Integer> getSchemaDimensionMinDomainInt(int dimIdx) {
    String dimExtentKey = "schema.dim." + dimIdx + ".min";
    return tryParseOptionKeyInt(optionMap, dimExtentKey);
  }

  public Optional<Integer> getSchemaDimensionMaxDomainInt(int dimIdx) {
    String dimExtentKey = "schema.dim." + dimIdx + ".max";
    return tryParseOptionKeyInt(optionMap, dimExtentKey);
  }

  public Optional<Long> getSchemaDimensionExtentLong(int dimIdx) {
    String dimExtentKey = "schema.dim." + dimIdx + ".extent";
    return tryParseOptionKeyLong(optionMap, dimExtentKey);
  }

  public Optional<Double> getSchemaDimensionMinDomainDouble(int dimIdx) {
    String dimExtentKey = "schema.dim." + dimIdx + ".min";
    return tryParseOptionKeyDouble(optionMap, dimExtentKey);
  }

  public Optional<Double> getSchemaDimensionMaxDomainDouble(int dimIdx) {
    String dimExtentKey = "schema.dim." + dimIdx + ".max";
    return tryParseOptionKeyDouble(optionMap, dimExtentKey);
  }

  public Optional<Double> getSchemaDimensionExtentDouble(int dimIdx) {
    String dimExtentKey = "schema.dim." + dimIdx + ".extent";
    return tryParseOptionKeyDouble(optionMap, dimExtentKey);
  }

  public Optional<List<Pair<String, Object[]>>> getAttributeFilterList(String attrName) {
    String filterListKey = "schema.attr." + attrName + ".filter_list";
    if (!optionMap.containsKey(filterListKey)) {
      return Optional.empty();
    }
    return tryParseFilterList(optionMap.get(filterListKey));
  }

  public Optional<List<Pair<String, Object[]>>> getSchemaCoordsFilterList() {
    String filterListKey = "schema.coords_filter_list";
    if (!optionMap.containsKey(filterListKey)) {
      return Optional.empty();
    }
    return tryParseFilterList(optionMap.get(filterListKey));
  }

  public Optional<List<Pair<String, Object[]>>> getSchemaOffsetsFilterList() {
    String filterListKey = "schema.offsets_filter_list";
    if (!optionMap.containsKey(filterListKey)) {
      return Optional.empty();
    }
    return tryParseFilterList(optionMap.get(filterListKey));
  }

  public Optional<Layout> getSchemaCellOrder() {
    String cellOrderLayoutKey = "schema.cell_order";
    if (!optionMap.containsKey(cellOrderLayoutKey)) {
      return Optional.empty();
    }
    return tryParseOptionLayout(optionMap.get(cellOrderLayoutKey));
  }

  public Optional<Layout> getSchemaTileOrder() {
    String tileOrderLayoutKey = "schema.tile_order";
    if (!optionMap.containsKey(tileOrderLayoutKey)) {
      return Optional.empty();
    }
    return tryParseOptionLayout(optionMap.get(tileOrderLayoutKey));
  }

  public Optional<Long> getSchemaCapacity() {
    String capacityKey = "schema.capacity";
    return tryParseOptionKeyLong(optionMap, capacityKey);
  }

  public Optional<Boolean> getSchemaAllowDups() {
    String allowDupsKey = "schema.set_allows_dups";

    return tryParseOptionKeyBoolean(optionMap, allowDupsKey);
  }

  public long getWriteBufferSize() {
    Optional<Long> bufferSize = tryParseOptionKeyLong(optionMap, "write_buffer_size");
    if (bufferSize.isPresent()) {
      return bufferSize.get();
    }
    return QUERY_BUFFER_SIZE;
  }

  /** @return Optional String HashMap of tiledb config options and values * */
  public Map<String, String> getTileDBConfigMap(boolean useArrowConfig) {
    HashMap<String, String> configMap = new HashMap<>();
    // necessary configs for var sized attributes
    if (useArrowConfig) {
      configMap.put("sm.var_offsets.bitsize", "32");
      configMap.put("sm.var_offsets.mode", "elements");
      configMap.put("sm.var_offsets.extra_element", "true");
    }

    List<Pair<String, String>> results =
        collectOptionsWithKeyPrefixSuffix(optionMap, "tiledb.", null);
    if (results.isEmpty()) {
      return configMap;
    }
    for (Pair<String, String> entry : results) {
      configMap.put(entry.getFirst(), entry.getSecond());
    }
    // arrow buffer support configs
    return configMap;
  }

  private static Optional<Layout> tryParseOptionLayout(String val) {
    // accept either the python string values or tiledb enum string value (uppercase) for
    // consistency with python api
    if (val.equalsIgnoreCase("row-major") || val.equalsIgnoreCase("TILEDB_ROW_MAJOR")) {
      return Optional.of(Layout.TILEDB_ROW_MAJOR);
    } else if (val.equalsIgnoreCase("col-major") || val.equalsIgnoreCase("TILEDB_COL_MAJOR")) {
      return Optional.of(Layout.TILEDB_COL_MAJOR);
    } else if (val.equalsIgnoreCase("unordered") || val.equalsIgnoreCase("TILEDB_UNORDERED")) {
      return Optional.of(Layout.TILEDB_UNORDERED);
    } else if (val.equalsIgnoreCase("global-order")
        || val.equalsIgnoreCase("TILEDB_GLOBAL_ORDER")) {
      return Optional.of(Layout.TILEDB_GLOBAL_ORDER);
    }
    return Optional.empty();
  }

  public static Optional<List<Pair<String, Object[]>>> tryParseFilterList(String csvList)
      throws IllegalArgumentException {
    // filter lists are in the form "(filter, option), (filter, option), etc.")
    List<Pair<String, Object[]>> filterResults = new ArrayList<>();
    // String[] splitVals = csvList.split("\\s*,\\s*");
    Pattern filterListRegex = Pattern.compile("\\(\\s?(.*?)\\s?,\\s?(.*?)\\s?\\)");
    Matcher filterListMatcher = filterListRegex.matcher(csvList);
    while (filterListMatcher.find()) {
      // remove parens
      String filterString = filterListMatcher.group().replace(")", "").replace("(", "");
      String[] filterWithOptions = filterString.split("\\s*,\\s*");
      String filterName = filterWithOptions[0];
      if (filterName.equalsIgnoreCase("NONE") || filterName.equalsIgnoreCase("NOOP")) {
      } else if (filterName.equalsIgnoreCase("GZIP")) {
      } else if (filterName.equalsIgnoreCase("ZSTD")) {
      } else if (filterName.equalsIgnoreCase("LZ4")) {
      } else if (filterName.equalsIgnoreCase("RLE")) {
      } else if (filterName.equalsIgnoreCase("BZIP2")) {
      } else if (filterName.equalsIgnoreCase("DOUBLE_DELTA")) {
      } else if (filterName.equalsIgnoreCase("BIT_WIDTH_REDUCTION")) {
      } else if (filterName.equalsIgnoreCase("BITSHUFFLE")) {
      } else if (filterName.equalsIgnoreCase("BYTESHUFFLE")) {
      } else if (filterName.equalsIgnoreCase("POSITIVE_DELTA")) {
      } else if (filterName.equalsIgnoreCase("SCALE_FLOAT")) {
      } else {
        throw new IllegalArgumentException("Unknown TileDB filter string value: " + filterName);
      }
      Object[] filterOptions = new Object[filterWithOptions.length - 1]; // without name
      if (filterWithOptions.length > 1) { // if options present
        try {
          int j = 0;
          for (int i = 1; i < filterWithOptions.length; i++) {
            filterOptions[j] = filterWithOptions[i];
            j++;
          }
        } catch (NumberFormatException err) {
          throw new IllegalArgumentException(
              "Cannot parse filter options for filter: " + filterName);
        }
      }
      filterResults.add(new Pair<>(filterName, filterOptions));
    }
    if (filterResults.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(filterResults);
  }

  private static Optional<Long> tryParseOptionKeyLong(Map<String, String> options, String key) {
    if (!options.containsKey(key)) {
      return Optional.empty();
    }
    Long val;
    try {
      val = Long.parseLong(options.get(key));
    } catch (NumberFormatException err) {
      return Optional.empty();
    }
    return Optional.of(val);
  }

  private static Optional<Integer> tryParseOptionKeyInt(Map<String, String> options, String key) {
    if (!options.containsKey(key)) {
      return Optional.empty();
    }
    int val;
    try {
      val = Integer.parseInt(options.get(key));
    } catch (NumberFormatException err) {
      return Optional.empty();
    }
    return Optional.of(val);
  }

  private static Optional<Double> tryParseOptionKeyDouble(Map<String, String> options, String key) {
    if (!options.containsKey(key)) {
      return Optional.empty();
    }
    Double val;
    try {
      val = Double.parseDouble(options.get(key));
    } catch (NumberFormatException err) {
      return Optional.empty();
    }
    return Optional.of(val);
  }

  private static Optional<Boolean> tryParseOptionKeyBoolean(
      Map<String, String> options, String key) {
    if (!options.containsKey(key)) {
      return Optional.empty();
    }
    Boolean val = Boolean.parseBoolean(options.get(key));
    return Optional.of(val);
  }

  private static List<Pair<String, String>> collectOptionsWithKeyPrefixSuffix(
      Map<String, String> options, String prefix, String suffix) {
    ArrayList<Pair<String, String>> results = new ArrayList<>();
    boolean hasPrefix = (prefix != null && prefix.length() > 0);
    boolean hasSuffix = (suffix != null && suffix.length() > 0);
    if (options.isEmpty() || !hasPrefix) {
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
        if (!hasSuffix) {
          results.add(new Pair<>(strippedKeyPrefix, val));
          continue;
        }
        // check suffix
        if (key.endsWith(suffix)) {
          String strippedKeySuffix =
              strippedKeyPrefix.substring(0, strippedKeyPrefix.length() - suffix.length());
          if (strippedKeySuffix.length() > 0) {
            results.add(new Pair<>(strippedKeySuffix, val));
          }
        }
      }
    }
    return results;
  }

  private static Map<String, String> collectOptionsWithKeyPrefixSuffixMap(
      Map<String, String> options, String prefix, String suffix) {
    Map<String, String> results = new HashMap<>();
    boolean hasPrefix = (prefix != null && prefix.length() > 0);
    boolean hasSuffix = (suffix != null && suffix.length() > 0);
    if (options.isEmpty() || !hasPrefix) {
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
        if (!hasSuffix) {
          results.put(strippedKeyPrefix, val);
          continue;
        }
        // check suffix
        if (key.endsWith(suffix)) {
          String strippedKeySuffix =
              strippedKeyPrefix.substring(0, strippedKeyPrefix.length() - suffix.length());
          if (strippedKeySuffix.length() > 0) {
            results.put(strippedKeySuffix, val);
          }
        }
      }
    }
    return results;
  }
}
