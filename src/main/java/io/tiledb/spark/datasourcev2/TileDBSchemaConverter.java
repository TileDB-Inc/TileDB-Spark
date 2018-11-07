/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.tiledb.spark.datasourcev2;

import static org.apache.spark.sql.types.DataTypes.*;

import io.tiledb.java.api.*;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.ArrayType;

public class TileDBSchemaConverter {

  private Context ctx;
  private TileDBOptions options;
  private StructType requiredSchema;

  public TileDBSchemaConverter(Context ctx, DataSourceOptions options) {
    this.ctx = ctx;
    this.options = new TileDBOptions(options);
  }

  public TileDBSchemaConverter(Context ctx, TileDBOptions tileDBOptions) {
    this.ctx = ctx;
    this.options = tileDBOptions;
  }

  public void setRequiredSchema(StructType requiredSchema) {
    this.requiredSchema = requiredSchema;
  }

  public StructType getSchema() throws TileDBError {
    String arrayURI = options.ARRAY_URI;
    StructType sparkSchema = new StructType();
    try (ArraySchema arraySchema = new ArraySchema(ctx, arrayURI);
        Domain arrayDomain = arraySchema.getDomain()) {
      for (int i = 0; i < arrayDomain.getRank(); i++) {
        try (Dimension dim = arrayDomain.getDimension(i)) {
          String dimName = dim.getName();
          if (requiredSchema == null || requiredSchema.getFieldIndex(dimName).isDefined()) {
            sparkSchema = sparkSchema.add(toStructField(dimName, dim.getType(), 1l, true));
          }
        }
      }
      for (int i = 0; i < arraySchema.getAttributeNum(); i++) {
        try (Attribute attr = arraySchema.getAttribute(i)) {
          String attrName = attr.getName();
          if (requiredSchema == null || requiredSchema.getFieldIndex(attrName).isDefined()) {
            sparkSchema =
                sparkSchema.add(
                    toStructField(attrName, attr.getType(), attr.getCellValNum(), false));
          }
        }
      }
    }
    return sparkSchema;
  }

  private StructField toStructField(
      String name, Datatype tiledbType, long cellValNum, boolean isDim) throws TileDBError {
    StructField field;
    MetadataBuilder metadataBuilder = new MetadataBuilder();
    if (isDim) {
      metadataBuilder.putBoolean("dimension", true);
    } else {
      metadataBuilder.putBoolean("attribute", true);
    }
    Metadata metadata = metadataBuilder.build();
    switch (tiledbType) {
      case TILEDB_FLOAT32:
        {
          if (cellValNum > 1)
            field = new StructField(name, DataTypes.createArrayType(FloatType), false, metadata);
          else field = new StructField(name, FloatType, false, metadata);
          break;
        }
      case TILEDB_FLOAT64:
        {
          if (cellValNum > 1)
            field = new StructField(name, DataTypes.createArrayType(DoubleType), false, metadata);
          else field = new StructField(name, DoubleType, false, Metadata.empty());
          break;
        }
      case TILEDB_INT8:
        {
          if (cellValNum > 1)
            field = new StructField(name, DataTypes.createArrayType(ByteType), false, metadata);
          else field = new StructField(name, ByteType, false, metadata);
          break;
        }
      case TILEDB_INT16:
        {
          if (cellValNum > 1)
            field = new StructField(name, DataTypes.createArrayType(ShortType), false, metadata);
          else field = new StructField(name, ShortType, false, Metadata.empty());
          break;
        }
      case TILEDB_INT32:
        {
          if (cellValNum > 1)
            field = new StructField(name, DataTypes.createArrayType(IntegerType), false, metadata);
          else field = new StructField(name, IntegerType, false, metadata);
          break;
        }
      case TILEDB_INT64:
        {
          if (cellValNum > 1)
            field = new StructField(name, DataTypes.createArrayType(LongType), false, metadata);
          else field = new StructField(name, LongType, false, Metadata.empty());
          break;
        }
      case TILEDB_UINT8:
        {
          if (cellValNum > 1) {
            field = new StructField(name, DataTypes.createArrayType(ShortType), false, metadata);
          } else {
            field = new StructField(name, ShortType, false, metadata);
          }
          break;
        }
      case TILEDB_UINT16:
        {
          if (cellValNum > 1)
            field = new StructField(name, DataTypes.createArrayType(IntegerType), false, metadata);
          else field = new StructField(name, IntegerType, false, metadata);
          break;
        }
      case TILEDB_UINT32:
        {
          if (cellValNum > 1)
            field = new StructField(name, DataTypes.createArrayType(LongType), false, metadata);
          else field = new StructField(name, LongType, false, metadata);
          break;
        }
      case TILEDB_UINT64:
        {
          if (cellValNum > 1)
            field = new StructField(name, DataTypes.createArrayType(LongType), false, metadata);
          else field = new StructField(name, LongType, false, metadata);
          break;
        }
      case TILEDB_CHAR:
        {
          field = new StructField(name, StringType, false, metadata);
          break;
        }
      default:
        {
          throw new TileDBError(
              "Unsupported TileDB <-> Spark DataFrame type: " + tiledbType.name());
        }
    }
    return field;
  }

  public ArraySchema toTileDBSchema(StructType schema) throws Exception {
    ArraySchema arraySchema = new ArraySchema(ctx, io.tiledb.java.api.ArrayType.TILEDB_SPARSE);
    try (Domain domain = new Domain(ctx)) {
      Compressor compressor;
      switch (options.COMPRESSION) {
        case "none":
          compressor = new Compressor(CompressorType.TILEDB_NO_COMPRESSION, -1);
          break;
        case "gzip":
          compressor = new Compressor(CompressorType.TILEDB_GZIP, -1);
          break;
        default:
          compressor = new Compressor(CompressorType.TILEDB_NO_COMPRESSION, -1);
      }
      for (StructField field : schema.fields()) {
        if (options.DIMENSIONS.contains(field.name())) {
          try (Dimension dimension = toDimension(field)) {
            domain.addDimension(dimension);
          }
        } else {
          try (Attribute attribute = toAttribute(field)) {
            attribute.setCompressor(compressor);
            arraySchema.addAttribute(attribute);
          }
        }
      }
      arraySchema.setDomain(domain);
      arraySchema.check();
    } catch (TileDBError err) {
      arraySchema.close();
      throw err;
    }
    // Check array schema
    return arraySchema;
  }

  private Dimension toDimension(StructField field) throws Exception {
    DataType dataType = field.dataType();
    if (dataType instanceof IntegerType) {
      int min =
          Integer.parseInt(
              options
                  .get(TileDBOptions.SUBARRAY_MIN_KEY.replace("{}", field.name()))
                  .orElse(Integer.MIN_VALUE + ""));
      int max =
          Integer.parseInt(
              options
                  .get(TileDBOptions.SUBARRAY_MAX_KEY.replace("{}", field.name()))
                  .orElse(Integer.MAX_VALUE + ""));
      int extent =
          Integer.parseInt(
              options
                  .get(TileDBOptions.SUBARRAY_EXTENT_KEY.replace("{}", field.name()))
                  .orElse("1"));
      return new Dimension<Integer>(
          ctx, field.name(), Integer.class, new Pair<Integer, Integer>(min, max), extent);
    } else if (dataType instanceof LongType) {
      long min =
          Long.parseLong(
              options
                  .get(TileDBOptions.SUBARRAY_MIN_KEY.replace("{}", field.name()))
                  .orElse(Long.MIN_VALUE + ""));
      long max =
          Long.parseLong(
              options
                  .get(TileDBOptions.SUBARRAY_MAX_KEY.replace("{}", field.name()))
                  .orElse(Long.MAX_VALUE + ""));
      long extent =
          Long.parseLong(
              options
                  .get(TileDBOptions.SUBARRAY_EXTENT_KEY.replace("{}", field.name()))
                  .orElse("1"));
      return new Dimension<Long>(
          ctx, field.name(), Long.class, new Pair<Long, Long>(min, max), extent);
    } else if (dataType instanceof ShortType) {
      int min =
          Short.parseShort(
              options
                  .get(TileDBOptions.SUBARRAY_MIN_KEY.replace("{}", field.name()))
                  .orElse(Short.MIN_VALUE + ""));
      int max =
          Short.parseShort(
              options
                  .get(TileDBOptions.SUBARRAY_MAX_KEY.replace("{}", field.name()))
                  .orElse(Short.MAX_VALUE + ""));
      short extent =
          Short.parseShort(
              options
                  .get(TileDBOptions.SUBARRAY_EXTENT_KEY.replace("{}", field.name()))
                  .orElse("1"));
      return new Dimension<Short>(
          ctx, field.name(), Short.class, new Pair<Short, Short>((short) min, (short) max), extent);
    } else if (dataType instanceof ByteType) {
      int min =
          Integer.parseInt(
              options
                  .get(TileDBOptions.SUBARRAY_MIN_KEY.replace("{}", field.name()))
                  .orElse("-128"));
      int max =
          Integer.parseInt(
              options
                  .get(TileDBOptions.SUBARRAY_MAX_KEY.replace("{}", field.name()))
                  .orElse("128"));
      short extent =
          Short.parseShort(
              options
                  .get(TileDBOptions.SUBARRAY_EXTENT_KEY.replace("{}", field.name()))
                  .orElse("1"));
      return new Dimension<Byte>(
          ctx,
          field.name(),
          Byte.class,
          new Pair<Byte, Byte>((byte) min, (byte) max),
          (byte) extent);
    } else {
      throw new Exception("Datatype not supported for dimension: " + dataType);
    }
  }

  private Attribute toAttribute(StructField field) throws Exception {
    DataType dataType = field.dataType();
    if (dataType instanceof IntegerType) {
      return new Attribute(ctx, field.name(), Integer.class);
    } else if (dataType instanceof StringType) {
      Attribute attribute = new Attribute(ctx, field.name(), String.class);
      attribute.setCellValNum(Constants.TILEDB_VAR_NUM);
      return attribute;
    } else if (dataType instanceof ShortType) {
      return new Attribute(ctx, field.name(), Short.class);
    } else if (dataType instanceof ByteType) {
      return new Attribute(ctx, field.name(), Byte.class);
    } else if (dataType instanceof LongType) {
      return new Attribute(ctx, field.name(), Long.class);
    } else if (dataType instanceof FloatType) {
      return new Attribute(ctx, field.name(), Float.class);
    } else if (dataType instanceof DoubleType) {
      return new Attribute(ctx, field.name(), Double.class);
    } else if (dataType instanceof ArrayType) {
      ArrayType at = (ArrayType) dataType;
      DataType type = at.elementType();
      if (type instanceof IntegerType) {
        Attribute attribute = new Attribute(ctx, field.name(), Integer.class);
        attribute.setCellValNum(Constants.TILEDB_VAR_NUM);
        return attribute;
      } else if (type instanceof ShortType) {
        Attribute attribute = new Attribute(ctx, field.name(), Short.class);
        attribute.setCellValNum(Constants.TILEDB_VAR_NUM);
        return attribute;
      } else if (type instanceof ByteType) {
        Attribute attribute = new Attribute(ctx, field.name(), Byte.class);
        attribute.setCellValNum(Constants.TILEDB_VAR_NUM);
        return attribute;
      } else if (type instanceof LongType) {
        Attribute attribute = new Attribute(ctx, field.name(), Long.class);
        attribute.setCellValNum(Constants.TILEDB_VAR_NUM);
        return attribute;
      } else if (type instanceof FloatType) {
        Attribute attribute = new Attribute(ctx, field.name(), Float.class);
        attribute.setCellValNum(Constants.TILEDB_VAR_NUM);
        return attribute;
      } else if (type instanceof DoubleType) {
        Attribute attribute = new Attribute(ctx, field.name(), Double.class);
        attribute.setCellValNum(Constants.TILEDB_VAR_NUM);
        return attribute;
      } else {
        throw new Exception(
            "Spark DataType not supported for TileDB schema conversion: " + dataType.toString());
      }
    } else {
      throw new Exception(
          "Spark DataType not supported for TileDB schema conversion: " + dataType.toString());
    }
  }
}
