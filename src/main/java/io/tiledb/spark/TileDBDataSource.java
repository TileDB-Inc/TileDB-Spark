package io.tiledb.spark;

import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class TileDBDataSource implements TableProvider {

  static Logger log = Logger.getLogger(TileDBDataSource.class.getName());

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    TileDBDataSourceOptions tiledbOptions =
        new TileDBDataSourceOptions(new DataSourceOptions(options));

    TileDBReadSchema tileDBReadSchema = null;
    tileDBReadSchema = new TileDBReadSchema(util.tryGetArrayURI(tiledbOptions), tiledbOptions);
    assert tileDBReadSchema != null;
    return tileDBReadSchema.getSparkSchema();
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return new TileDBTable(schema, properties);
  }

  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }
}
