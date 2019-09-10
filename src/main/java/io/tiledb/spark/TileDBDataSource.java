package io.tiledb.spark;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;

public class TileDBDataSource implements DataSourceV2, ReadSupport, WriteSupport {

  static Logger log = Logger.getLogger(TileDBDataSource.class.getName());

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    TileDBDataSourceOptions tiledbOptions = new TileDBDataSourceOptions(options);
    URI arrayURI = tryGetArrayURI(tiledbOptions);
    log.trace("Creating TileDBDataSourceReader for " + arrayURI);
    return new TileDBDataSourceReader(arrayURI, tiledbOptions);
  }

  @Override
  public Optional<DataSourceWriter> createWriter(
      String writeUUID, StructType schema, SaveMode mode, DataSourceOptions options) {
    TileDBDataSourceOptions tiledbOptions = new TileDBDataSourceOptions(options);
    URI arrayURI = tryGetArrayURI(tiledbOptions);
    log.trace("Creating TileDBDataSourceWriter for " + arrayURI);
    TileDBDataSourceWriter writer =
        new TileDBDataSourceWriter(arrayURI, schema, mode, tiledbOptions);
    return Optional.of(writer);
  }

  private URI tryGetArrayURI(TileDBDataSourceOptions tiledbOptions) {
    Optional<URI> arrayURI;
    try {
      arrayURI = tiledbOptions.getArrayURI();
    } catch (URISyntaxException ex) {
      throw new RuntimeException("Error parsing array URI option: " + ex.getMessage());
    }
    if (!arrayURI.isPresent()) {
      throw new RuntimeException("TileDB URI option required");
    }
    return arrayURI.get();
  }
}
