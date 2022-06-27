package io.tiledb.spark;

import io.tiledb.java.api.Array;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.TileDBError;
import java.net.URISyntaxException;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class TileDBDataSource implements TableProvider {

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {

    TileDBDataSourceOptions tiledbOptions =
        new TileDBDataSourceOptions(new DataSourceOptions(options));

    TileDBReadSchema tileDBReadSchema;
    tileDBReadSchema = new TileDBReadSchema(util.tryGetArrayURI(tiledbOptions), tiledbOptions);
    if (tiledbOptions.printMetadata()) {
      try {
        printMetadata(tiledbOptions);
      } catch (TileDBError | URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
    return tileDBReadSchema.getSparkSchema();
  }

  private void printMetadata(TileDBDataSourceOptions tileDBDataSourceOptions)
      throws TileDBError, URISyntaxException {
    // This enables us to request the metadata without the need to read the array.
    Array array = new Array(new Context(), tileDBDataSourceOptions.getArrayURI().get());
    Map<String, Object> a = array.getMetadataMap();
    for (String key : a.keySet()) {
      System.out.println("<" + key + ", " + a.get(key) + ">");
    }
    array.close();
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
