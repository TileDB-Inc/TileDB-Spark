package io.tiledb.spark;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import org.apache.log4j.Logger;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.*;

public class TileDBDataSource implements DataSourceV2, ReadSupport {

  static Logger log = Logger.getLogger(TileDBDataSource.class.getName());

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    TileDBDataSourceOptions tiledbOptions = new TileDBDataSourceOptions(options);
    Optional<URI> arrayURI;
    try {
      arrayURI = tiledbOptions.getArrayURI();
    } catch (URISyntaxException ex) {
      throw new RuntimeException("Error parsing array URI option: " + ex.getMessage());
    }
    if (!arrayURI.isPresent()) {
      throw new RuntimeException("TileDB URI option required");
    }
    log.trace("Creating TileDBDataSourceReader for " + arrayURI.get());
    return new TileDBDataSourceReader(arrayURI.get(), tiledbOptions);
  }
}
