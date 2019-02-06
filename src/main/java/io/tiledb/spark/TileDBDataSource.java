package io.tiledb.spark;

import java.net.URI;
import java.util.Optional;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;

public class TileDBDataSource implements DataSourceV2, ReadSupport, WriteSupport {

  static Logger log = Logger.getLogger(TileDBDataSource.class.getName());

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    Optional<String> uriString = options.get("uri");
    if (!uriString.isPresent()) {
      throw new RuntimeException("TileDB URI must be defined as an option");
    }
    URI uri;
    try {
      uri = URI.create(uriString.get());
    } catch (Exception err) {
      throw new RuntimeException("cannot parse TileDB URI option into valid URI object");
    }
    log.info("Creating TileDBDataSource Reader for `" + uriString + "`");
    return new TileDBDataSourceReader(uri, options);
  }

  @Override
  public Optional<DataSourceWriter> createWriter(
      String jobId, StructType schema, SaveMode mode, DataSourceOptions options) {
    return TileDBDataSourceWriter.createWriter(jobId, schema, mode, options);
  }
}
