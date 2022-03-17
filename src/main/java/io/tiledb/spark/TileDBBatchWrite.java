package io.tiledb.spark;

import io.tiledb.java.api.Array;
import io.tiledb.java.api.ArraySchema;
import io.tiledb.java.api.ArrayType;
import io.tiledb.java.api.Attribute;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Dimension;
import io.tiledb.java.api.Domain;
import io.tiledb.java.api.FilterList;
import io.tiledb.java.api.Layout;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.TileDBError;
import io.tiledb.java.api.TileDBObject;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TileDBBatchWrite implements BatchWrite {
  private final Map<String, String> properties;
  private final TileDBDataSourceOptions tileDBDataSourceOptions;
  public final SaveMode saveMode;
  private final URI uri;
  private final LogicalWriteInfo logicalWriteInfo;
  private final StructType sparkSchema;

  public TileDBBatchWrite(
      Map<String, String> properties, LogicalWriteInfo info, SaveMode saveMode) {
    this.properties = properties;
    this.tileDBDataSourceOptions =
        new TileDBDataSourceOptions(new DataSourceOptions(info.options()));
    this.uri = util.tryGetArrayURI(tileDBDataSourceOptions);
    this.logicalWriteInfo = info;
    this.sparkSchema = this.logicalWriteInfo.schema();
    this.saveMode = saveMode;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    boolean isOkToWrite = tryWriteArraySchema();
    if (!isOkToWrite) {
      throw new RuntimeException(
          "Writing to an existing array: '" + uri + "' with save mode " + saveMode);
    }
    return new TileDBDataWriterFactory(
        this.uri, this.logicalWriteInfo.schema(), tileDBDataSourceOptions);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {}

  @Override
  public void abort(WriterCommitMessage[] messages) {
    if (messages.length > 0) {
      try (Context ctx = new Context(tileDBDataSourceOptions.getTileDBConfigMap(false))) {
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
    try (Context ctx = new Context(tileDBDataSourceOptions.getTileDBConfigMap(false))) {
      boolean arrayExists = Array.exists(ctx, uri.toString());
      if (saveMode == SaveMode.Append) {
        throw new RuntimeException(
            "Append is not currently supported by TileDB. URI '"
                + uri
                + "' with save mode "
                + saveMode
                + ". Use  '"
                + SaveMode.Overwrite
                + "' mode instead.");
      } else if (saveMode == SaveMode.Overwrite) {
        if (arrayExists) {
          TileDBObject.remove(ctx, uri.toString());
        }
        writeArraySchema(ctx, uri, sparkSchema, tileDBDataSourceOptions);
        return true;
      }
    } catch (TileDBError err) {
      err.printStackTrace();
      throw new RuntimeException(err.getMessage());
    }
    return false;
  }

  private static void writeArraySchema(
      Context ctx, URI uri, StructType sparkSchema, TileDBDataSourceOptions options)
      throws TileDBError {
    ArrayType type = ArrayType.TILEDB_SPARSE;
    try (ArraySchema arraySchema = new ArraySchema(ctx, type);
        Domain domain = new Domain(ctx)) {
      String[] dimNames = TileDBWriteSchema.getSchemaDimensionOptions(sparkSchema, options);
      StructField[] sparkFields = sparkSchema.fields();
      for (int dimIdx = 0; dimIdx < dimNames.length; dimIdx++) {
        String dimName = dimNames[dimIdx];
        int idx = sparkSchema.fieldIndex(dimName);
        try (Dimension dim =
            TileDBWriteSchema.toDimension(
                ctx, dimName, dimIdx, sparkFields[idx].dataType(), options)) {
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

      // set allows dups
      if (options.getSchemaAllowDups().isPresent() && options.getSchemaAllowDups().get())
        arraySchema.setAllowDups(1);

      arraySchema.check();
      Array.create(uri.toString(), arraySchema);
    }
  }
}
