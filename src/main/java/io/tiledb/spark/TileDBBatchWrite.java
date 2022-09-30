package io.tiledb.spark;

import io.tiledb.java.api.Array;
import io.tiledb.java.api.ArraySchema;
import io.tiledb.java.api.ArrayType;
import io.tiledb.java.api.Attribute;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Dimension;
import io.tiledb.java.api.Domain;
import io.tiledb.java.api.FilterList;
import io.tiledb.java.api.Layout;
import io.tiledb.java.api.NativeArray;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.QueryType;
import io.tiledb.java.api.TileDBError;
import io.tiledb.java.api.TileDBObject;
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
  private final String uri;
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

    // Write metadata if present
    Map<String, String> metadata = tileDBDataSourceOptions.getMetadata();
    Map<String, String> metadataTypes = tileDBDataSourceOptions.getMetadataTypes();
    if (!metadata.isEmpty()) {
      try (Array array = new Array(new Context(), uri, QueryType.TILEDB_WRITE)) {
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
          String metaDatatype = metadataTypes.get(entry.getKey());
          if (metaDatatype == null)
            throw new TileDBError(
                "Please include the 'metadata_type' option for metadata." + entry.getKey());
          NativeArray valueNativeArray = metadataValueToNativeArray(entry.getValue(), metaDatatype);
          array.putMetadata(entry.getKey(), valueNativeArray);
        }
      } catch (TileDBError e) {
        throw new RuntimeException(e);
      }
    }
    return new TileDBDataWriterFactory(
        this.uri, this.logicalWriteInfo.schema(), tileDBDataSourceOptions);
  }

  /**
   * @param stringValue
   * @param metaDatatype
   * @return
   */
  private NativeArray metadataValueToNativeArray(String stringValue, String metaDatatype)
      throws TileDBError {
    try (Context context = new Context()) {
      switch (metaDatatype) {
        case "TILEDB_INT32":
          int intValue = Integer.parseInt(stringValue);
          return new NativeArray(context, new int[] {intValue}, Datatype.TILEDB_INT32);
        case "TILEDB_FLOAT32":
          float floatValue = Float.parseFloat(stringValue);
          return new NativeArray(context, new float[] {floatValue}, Datatype.TILEDB_FLOAT32);
        case "TILEDB_STRING_ASCII":
          return new NativeArray(context, stringValue, Datatype.TILEDB_STRING_ASCII);
        default:
          throw new TileDBError(
              "Metadata type: " + metaDatatype + " is not supported in TileDB-Spark.");
      }
    } catch (TileDBError e) {
      throw e;
    }
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
        if (!arrayExists) {
          writeArraySchema(ctx, uri, sparkSchema, tileDBDataSourceOptions);
        }
        return true;
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
      Context ctx, String uri, StructType sparkSchema, TileDBDataSourceOptions options)
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
      Optional<List<Pair<String, Object[]>>> coordsFilters = options.getSchemaCoordsFilterList();
      if (coordsFilters.isPresent()) {
        try (FilterList filterList =
            TileDBWriteSchema.createTileDBFilterList(ctx, coordsFilters.get())) {
          arraySchema.setCoodsFilterList(filterList);
        }
      }
      Optional<List<Pair<String, Object[]>>> offsetsFilters = options.getSchemaOffsetsFilterList();
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
      Array.create(uri, arraySchema);
    }
  }
}
