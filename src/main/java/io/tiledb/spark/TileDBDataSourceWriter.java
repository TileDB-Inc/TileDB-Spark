package io.tiledb.spark;

import io.tiledb.java.api.*;
import java.util.Optional;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

public class TileDBDataSourceWriter implements DataSourceWriter {

  private final String jobId;
  private final StructType schema;
  private final TileDBOptions options;

  public static Optional<DataSourceWriter> createWriter(
      String jobId, StructType schema, SaveMode mode, DataSourceOptions options) {
    boolean arrayExists = false;
    TileDBOptions tiledbOptions = new TileDBOptions(options);
    try (Context ctx = new Context()) {
      TileDBObject obj = new TileDBObject(ctx, tiledbOptions.ARRAY_URI);
      if (obj.getType() == TileDBObjectType.TILEDB_ARRAY) {
        arrayExists = true;
      }
      // create or delete arrays depending on save mode
      switch (mode) {
        case Append:
          {
            if (!arrayExists) {
              createArray(ctx, schema, tiledbOptions);
            }
            break;
          }
        case Overwrite:
          {
            if (arrayExists) {
              deleteArray(ctx, tiledbOptions);
            }
            createArray(ctx, schema, tiledbOptions);
            break;
          }
        case ErrorIfExists:
          {
            if (!arrayExists) {
              createArray(ctx, schema, tiledbOptions);
            }
            break;
          }
        case Ignore:
          {
            if (!arrayExists) {
              createArray(ctx, schema, tiledbOptions);
            }
            break;
          }
      }
    } catch (Exception error) {
      throw new RuntimeException(error.getMessage());
    }
    switch (mode) {
      case ErrorIfExists:
        if (arrayExists) {
          return Optional.empty();
        }
      case Ignore:
        if (arrayExists) {
          return Optional.empty();
        }
    }
    return Optional.of(new TileDBDataSourceWriter(jobId, schema, tiledbOptions));
  }

  private static void createArray(Context ctx, StructType schema, TileDBOptions options)
      throws Exception {
    TileDBSchemaConverter tileDBSchemaConverter = new TileDBSchemaConverter(ctx, options);
    try (ArraySchema arraySchema = tileDBSchemaConverter.toTileDBSchema(schema)) {
      Array.create(options.ARRAY_URI, arraySchema);
    }
  }

  private static void deleteArray(Context ctx, TileDBOptions options) throws Exception {
    TileDBObject.remove(ctx, options.ARRAY_URI);
  }

  public TileDBDataSourceWriter(String jobId, StructType schema, TileDBOptions options) {
    this.jobId = jobId;
    this.schema = schema;
    this.options = options;
  }

  @Override
  public DataWriterFactory<Row> createWriterFactory() {
    return new TileDBWriterFactory(jobId, schema, options);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {}

  @Override
  public void abort(WriterCommitMessage[] messages) {}
}
