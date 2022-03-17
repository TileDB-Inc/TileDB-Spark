package io.tiledb.spark;

import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class TileDBTable implements SupportsRead, SupportsWrite {
  private final StructType schema;
  private final Map<String, String> properties;
  private Set<TableCapability> capabilities;

  public TileDBTable(StructType schema, Map<String, String> properties) {
    this.schema = schema;
    this.properties = properties;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    try {
      TileDBDataSourceOptions tileDBDataSourceOptions =
          new TileDBDataSourceOptions(new DataSourceOptions(options));
      return new TileDBScanBuilder(properties, tileDBDataSourceOptions);
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return new TileDBWriteBuilder(properties, info);
  }

  @Override
  public String name() {
    return properties.get("uri");
  }

  @Override
  public StructType schema() {
    return this.schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    if (capabilities == null) {
      this.capabilities = new HashSet<>();
      capabilities.add(TableCapability.BATCH_READ);
      capabilities.add(TableCapability.BATCH_WRITE);
      capabilities.add(TableCapability.TRUNCATE);
    }
    return capabilities;
  }
}
