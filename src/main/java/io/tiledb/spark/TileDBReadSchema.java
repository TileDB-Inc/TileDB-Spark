package io.tiledb.spark;

import io.tiledb.java.api.*;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Optional;
import org.apache.spark.sql.types.*;

public class TileDBReadSchema implements Serializable {

  private String uri;
  private TileDBDataSourceOptions options;
  private StructType pushDownSparkSchema;
  private StructType tiledbSparkSchema;
  public HashMap<String, Integer> dimensionIndex;
  public HashMap<String, Integer> attributeIndex;
  public HashMap<Integer, String> dimensionName;
  public HashMap<Integer, String> attributeName;
  public HashMap<Integer, Datatype> columnTypes;

  public TileDBReadSchema(String uri, TileDBDataSourceOptions options) {
    this.uri = uri;
    this.options = options;
    this.dimensionIndex = new HashMap<>();
    this.attributeIndex = new HashMap<>();
    this.dimensionName = new HashMap<>();
    this.attributeName = new HashMap<>();
    this.columnTypes = new HashMap<>();
    this.tiledbSparkSchema = this.getSparkSchema();
    if (this.tiledbSparkSchema == null) throw new RuntimeException("Unable to create Spark Schema");
  }

  public TileDBReadSchema setPushDownSchema(StructType pushDownSchema) {
    pushDownSparkSchema = pushDownSchema;
    return this;
  }

  /** @return StructType spark schema given by the array schema and projected columns * */
  public StructType getSparkSchema() {
    // for pushdown Spark first loads the entire schema and then does validation
    // if pushdown succeeds it means that Spark has already loaded the schema and verified
    // that the pushed down columns are correct, so we can just return the pushdown schema here
    if (pushDownSparkSchema != null) {
      return pushDownSparkSchema;
    }
    // TileDB schemas are assumed to be immutable and unchanging (for now),
    // so we can cache the schema for a given URI
    if (tiledbSparkSchema != null) {
      return tiledbSparkSchema;
    }
    // we have not yet loaded and converted the TileDB schema, do that now and cache the result
    try {
      tiledbSparkSchema = getTileDBSchema(options);
    } catch (TileDBError err) {
      throw new RuntimeException(
          "Error  converting TileDB schema for '" + uri + "': " + err.getMessage());
    }
    return tiledbSparkSchema;
  }

  private StructType getTileDBSchema(TileDBDataSourceOptions options) throws TileDBError {
    StructType sparkSchema = new StructType();
    try (Context ctx = new Context(options.getTileDBConfigMap(true));
        // fetch and load the schema (IO)
        ArraySchema arraySchema = new ArraySchema(ctx, uri.toString());
        Domain arrayDomain = arraySchema.getDomain()) {

      // for every dimension add a struct field
      int i;
      for (i = 0; i < arrayDomain.getNDim(); i++) {
        try (Dimension dim = arrayDomain.getDimension(i)) {
          String dimName = dim.getName();
          this.dimensionIndex.put(dimName, i);
          this.dimensionName.put(i, dimName);
          this.columnTypes.put(i, dim.getType());
          // schema is immutable so to iteratively add we need to re-assign
          sparkSchema = sparkSchema.add(toStructField(dimName, true, dim.getType(), 1l, false));
        }
      }
      // for every attribute add a struct field
      for (int j = 0; j < arraySchema.getAttributeNum(); j++) {
        try (Attribute attr = arraySchema.getAttribute(j)) {
          this.attributeIndex.put(attr.getName(), j + i);
          this.columnTypes.put(i + j, attr.getType());
          this.attributeName.put(j + i, attr.getName());
          String attrName = attr.getName();
          sparkSchema =
              sparkSchema.add(
                  toStructField(
                      attrName, false, attr.getType(), attr.getCellValNum(), attr.getNullable()));
        }
      }
    }
    return sparkSchema;
  }

  public Optional<Integer> getColumnId(String columnName) {
    if (this.dimensionIndex.containsKey(columnName)) {
      return Optional.of(this.dimensionIndex.get(columnName));
    }
    if (this.attributeIndex.containsKey(columnName)) {
      return Optional.of(this.attributeIndex.get(columnName));
    }

    return Optional.empty();
  }

  public boolean hasDimension(String dimName) {
    return this.dimensionName.containsValue(dimName);
  }

  public Optional<String> getColumnName(Integer id) {
    if (this.dimensionName.containsKey(id)) return Optional.of(this.dimensionName.get(id));
    if (this.attributeName.containsKey(id)) return Optional.of(this.attributeName.get(id));
    return Optional.empty();
  }

  private StructField toStructField(
      String name, boolean isDim, Datatype tiledbType, long cellValNum, boolean isNullable)
      throws TileDBError {
    MetadataBuilder metadataBuilder = new MetadataBuilder();
    if (isDim) {
      metadataBuilder.putBoolean("tiledb.dimension", true);
    } else {
      metadataBuilder.putBoolean("tiledb.attribute", true);
    }
    Metadata metadata = metadataBuilder.build();
    StructField field;
    switch (tiledbType) {
      case TILEDB_FLOAT32:
        {
          if (cellValNum > 1) {
            field =
                new StructField(
                    name, DataTypes.createArrayType(DataTypes.FloatType), isNullable, metadata);
          } else {
            field = new StructField(name, DataTypes.FloatType, isNullable, metadata);
          }
          break;
        }
      case TILEDB_FLOAT64:
        {
          if (cellValNum > 1) {
            field =
                new StructField(
                    name, DataTypes.createArrayType(DataTypes.DoubleType), isNullable, metadata);
          } else {
            field = new StructField(name, DataTypes.DoubleType, isNullable, metadata);
          }
          break;
        }
      case TILEDB_INT8:
        {
          if (cellValNum > 1) {
            field =
                new StructField(
                    name, DataTypes.createArrayType(DataTypes.ByteType), isNullable, metadata);
          } else {
            field = new StructField(name, DataTypes.ByteType, isNullable, metadata);
          }
          break;
        }
      case TILEDB_UINT8:
      case TILEDB_INT16:
        {
          if (cellValNum > 1) {
            field =
                new StructField(
                    name, DataTypes.createArrayType(DataTypes.ShortType), isNullable, metadata);
          } else {
            field = new StructField(name, DataTypes.ShortType, isNullable, metadata);
          }
          break;
        }
      case TILEDB_UINT16:
      case TILEDB_INT32:
        {
          if (cellValNum > 1) {
            field =
                new StructField(
                    name, DataTypes.createArrayType(DataTypes.IntegerType), isNullable, metadata);
          } else {
            field = new StructField(name, DataTypes.IntegerType, isNullable, metadata);
          }
          break;
        }
      case TILEDB_UINT32:
      case TILEDB_INT64:
        {
          if (cellValNum > 1) {
            field =
                new StructField(
                    name, DataTypes.createArrayType(DataTypes.LongType), isNullable, metadata);
          } else {
            field = new StructField(name, DataTypes.LongType, isNullable, metadata);
          }
          break;
        }
      case TILEDB_CHAR:
      case TILEDB_STRING_ASCII:
      case TILEDB_STRING_UTF8:
        {
          field = new StructField(name, DataTypes.StringType, isNullable, metadata);
          break;
        }
      case TILEDB_DATETIME_DAY:
      case TILEDB_DATETIME_WEEK:
      case TILEDB_DATETIME_MONTH:
      case TILEDB_DATETIME_YEAR:
      case TILEDB_DATETIME_MS:
      case TILEDB_DATETIME_AS:
      case TILEDB_DATETIME_FS:
      case TILEDB_DATETIME_PS:
      case TILEDB_DATETIME_NS:
      case TILEDB_DATETIME_US:
      case TILEDB_DATETIME_SEC:
      case TILEDB_DATETIME_MIN:
      case TILEDB_DATETIME_HR:
        {
          field = new StructField(name, DataTypes.TimestampType, isNullable, metadata);
          break;
        }
      default:
        {
          throw new TileDBError(
              "Unsupported TileDB to Spark DataFrame type mapping for schema column '"
                  + name
                  + "': "
                  + tiledbType.name());
        }
    }
    return field;
  }
}
