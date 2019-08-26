package io.tiledb.spark;

import io.tiledb.java.api.*;
import java.io.Serializable;
import java.net.URI;
import java.util.*;
import org.apache.spark.sql.types.*;

public class TileDBReadSchema implements Serializable {

  private URI uri;
  private TileDBDataSourceOptions options;
  private StructType pushDownSparkSchema;
  private StructType tiledbSparkSchema;
  private ArrayList<DomainDimRange> nonEmptyDomain;
  private HashMap<String, Integer> dimensionNameIdxMap;
  private HashMap<String, Integer> attributeNameIdxMap;

  public TileDBReadSchema(URI uri, TileDBDataSourceOptions options) {
    this.uri = uri;
    this.options = options;
    this.nonEmptyDomain = new ArrayList<>();
    this.dimensionNameIdxMap = new HashMap<>();
    this.attributeNameIdxMap = new HashMap<>();
    this.getSparkSchema();
  }

  public TileDBReadSchema setPushDownSchema(StructType pushDownSchema) {
    pushDownSparkSchema = pushDownSchema;
    return this;
  }

  /** @return True if the the given name is a dimension, false otherwise * */
  public boolean isDimensionName(String name) {
    return dimensionNameIdxMap.containsKey(name);
  };

  /** @return Optional integer index * */
  public Optional<Integer> getDimensionIdx(String name) {
    if (!isDimensionName(name)) {
      return Optional.empty();
    }
    return Optional.of(dimensionNameIdxMap.get(name));
  };

  /** @return Number of dimensions in domain * */
  public Integer getDomainNDim() {
    return nonEmptyDomain.size();
  }

  /** @return Non-empty domain range * */
  public DomainDimRange[] getNonEmptyDomainRange() {
    return nonEmptyDomain.toArray(new DomainDimRange[nonEmptyDomain.size()]);
  };

  /** @return True if the given name is an attribute, false otherwise * */
  public boolean isAttributeName(String name) {
    return dimensionNameIdxMap.containsKey(name);
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
          "Error converting TileDB schema for '" + uri + "': " + err.getMessage());
    }
    return tiledbSparkSchema;
  }

  private StructType getTileDBSchema(TileDBDataSourceOptions options) throws TileDBError {
    StructType sparkSchema = new StructType();
    try (Context ctx = new Context(options.getTileDBConfigMap());
        // fetch and load the array / schema (IO)
        Array array = new Array(ctx, uri.toString());
        ArraySchema arraySchema = array.getSchema();
        Domain arrayDomain = arraySchema.getDomain()) {
      HashMap<String, Pair> nonEmptyDomain = array.nonEmptyDomain();
      // for every dimension add a struct field
      for (int i = 0; i < arrayDomain.getNDim(); i++) {
        try (Dimension dim = arrayDomain.getDimension(i)) {
          String dimName = dim.getName();
          Pair dimExtent = nonEmptyDomain.get(dimName);
          this.nonEmptyDomain.add(
              new DomainDimRange(
                  dimName, i, dim.getType(), dimExtent.getFirst(), dimExtent.getSecond()));
          this.dimensionNameIdxMap.put(dimName, i);
          // schema is immutable so to iteratively add we need to re-assign
          sparkSchema = sparkSchema.add(toStructField(dimName, true, dim.getType(), 1l, false));
        }
      }
      // for every attribute add a struct field
      for (int i = 0; i < arraySchema.getAttributeNum(); i++) {
        try (Attribute attr = arraySchema.getAttribute(i)) {
          String attrName = attr.getName();
          attributeNameIdxMap.put(attrName, i);
          sparkSchema =
              sparkSchema.add(
                  toStructField(attrName, false, attr.getType(), attr.getCellValNum(), false));
        }
      }
    }
    return sparkSchema;
  }

  private StructField toStructField(
      String name, boolean isDim, Datatype tiledbType, long cellValNum, boolean isNullable)
      throws TileDBError {
    MetadataBuilder metadataBuilder = new MetadataBuilder();
    if (isDim) {
      metadataBuilder.putBoolean("dimension", true);
    } else {
      metadataBuilder.putBoolean("attribute", true);
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
      case TILEDB_CHAR:
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
      case TILEDB_STRING_ASCII:
      case TILEDB_STRING_UTF8:
        {
          field = new StructField(name, DataTypes.StringType, isNullable, metadata);
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
