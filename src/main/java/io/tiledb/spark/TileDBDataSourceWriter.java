package io.tiledb.spark;

import io.tiledb.java.api.*;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TileDBDataSourceWriter implements DataSourceWriter {
  static Logger log = Logger.getLogger(TileDBDataSourceWriter.class.getName());
  public final URI uri;
  public final StructType sparkSchema;
  public final SaveMode saveMode;
  public final TileDBDataSourceOptions options;

  public TileDBDataSourceWriter(
      URI uri, StructType schema, SaveMode mode, TileDBDataSourceOptions options) {
    this.uri = uri;
    this.sparkSchema = schema;
    this.saveMode = mode;
    this.options = options;
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    boolean isOkToWrite = tryWriteArraySchema();
    if (!isOkToWrite) {
      throw new RuntimeException(
          "Writing to an existing array: '" + uri + "' with save mode " + saveMode);
    }
    return new TileDBDataWriterFactory(uri, sparkSchema, options);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {}

  @Override
  public void abort(WriterCommitMessage[] messages) {
    if (messages.length > 0) {
      try (Context ctx = new Context(options.getTileDBConfigMap())) {
        TileDBObject.remove(ctx, uri.toString());
      } catch (TileDBError err) {
        throw new RuntimeException(
            "Error removing tiledb array at '"
                + uri
                + "' after aborted / failed write: "
                + err.getMessage());
      }
    }
  }

  private boolean tryWriteArraySchema() {
    try (Context ctx = new Context(options.getTileDBConfigMap())) {
      boolean arrayExists = Array.exists(ctx, uri.toString());
      if (saveMode == SaveMode.Append) {
        if (!arrayExists) {
          writeArraySchema(ctx, uri, sparkSchema, options);
        }
        return true;
      } else if (saveMode == SaveMode.Overwrite) {
        if (arrayExists) {
          TileDBObject.remove(ctx, uri.toString());
        }
        writeArraySchema(ctx, uri, sparkSchema, options);
        return true;
      } else if (saveMode == SaveMode.ErrorIfExists) {
        if (!arrayExists) {
          writeArraySchema(ctx, uri, sparkSchema, options);
          return true;
        } else {
          return false;
        }
      } else if (saveMode == SaveMode.Ignore) {
        if (!arrayExists) {
          writeArraySchema(ctx, uri, sparkSchema, options);
          return true;
        } else {
          return false;
        }
      }
    } catch (TileDBError err) {
      log.error(err.getMessage(),err);
      throw new RuntimeException(err.getMessage());
    }
    return false;
  }

  private static void writeArraySchema(
      Context ctx, URI uri, StructType sparkSchema, TileDBDataSourceOptions options)
      throws TileDBError {
    writeSparseArraySchema(ctx, uri, sparkSchema, options);
  }

  private static void writeSparseArraySchema(
      Context ctx, URI uri, StructType sparkSchema, TileDBDataSourceOptions options)
      throws TileDBError {
    try (ArraySchema arraySchema = new ArraySchema(ctx, ArrayType.TILEDB_SPARSE);
        Domain domain = new Domain(ctx)) {
      String[] dimNames = TileDBWriteSchema.getSchemaDimensionOptions(sparkSchema, options);
      StructField[] sparkFields = sparkSchema.fields();
      for (int dimIdx = 0; dimIdx < dimNames.length; dimIdx++) {
        String dimName = dimNames[dimIdx];
        int idx = sparkSchema.fieldIndex(dimName);
        try (Dimension dim =
            TileDBWriteSchema.toDimension(ctx, dimName, dimIdx, sparkFields[idx], options)) {
          domain.addDimension(dim);
        }
      }
      // set domain
      arraySchema.setDomain(domain);
      // add attributes
      for (StructField field : sparkFields) {
        // skip over dims
        if (Arrays.stream(dimNames).anyMatch(field.name()::equals)) {
          continue;
        }
        try (Attribute attr = TileDBWriteSchema.toAttribute(ctx, field, options)) {
          arraySchema.addAttribute(attr);
        }
      }
      // set schema tile / layouts and remaining attributes
      Optional<Layout> schemaCellOrder = options.getSchemaCellOrder();
      if (schemaCellOrder.isPresent()) {
        arraySchema.setCellOrder(schemaCellOrder.get());
      }
      Optional<Layout> schemaTileOrder = options.getSchemaTileOrder();
      if (schemaTileOrder.isPresent()) {
        arraySchema.setTileOrder(schemaTileOrder.get());
      }
      // schema filters
      Optional<List<Pair<String, Integer>>> coordsFilters = options.getSchemaCoordsFilterList();
      if (coordsFilters.isPresent()) {
        try (FilterList filterList =
            TileDBWriteSchema.createTileDBFilterList(ctx, coordsFilters.get())) {
          arraySchema.setCoodsFilterList(filterList);
        }
      }
      Optional<List<Pair<String, Integer>>> offsetsFilters = options.getSchemaOffsetsFilterList();
      if (coordsFilters.isPresent()) {
        try (FilterList filterList =
            TileDBWriteSchema.createTileDBFilterList(ctx, offsetsFilters.get())) {
          arraySchema.setOffsetsFilterList(filterList);
        }
      }
      // set capacity
      Optional<Long> schemaCapacity = options.getSchemaCapacity();
      if (schemaCapacity.isPresent()) {
        arraySchema.setCapacity(schemaCapacity.get());
      }
      arraySchema.check();
      Array.create(uri.toString(), arraySchema);
    }
  }
}
