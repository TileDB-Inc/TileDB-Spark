package io.tiledb.spark;

import io.tiledb.java.api.*;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.ArrayType;

public class TileDBWriteSchema {

  static String[] getSchemaDimensionOptions(StructType sparkSchema, TileDBDataSourceOptions options)
      throws TileDBError {
    Optional<List<Pair<String, Integer>>> schemaDimsOpt = options.getSchemaDimensionNames();
    if (!schemaDimsOpt.isPresent()) {
      throw new TileDBError(
          "must specify one or more dimension columns when writing a TileDB array");
    }
    List<StructField> fields = Arrays.asList(sparkSchema.fields());
    List<Pair<String, Integer>> schemaDims = schemaDimsOpt.get();
    // check that schema dims are in monotonically increasing sorted order
    for (int i = 0; i < schemaDims.size(); i++) {
      Pair<String, Integer> schemaDim = schemaDims.get(i);
      if (schemaDim.getSecond() != i) {
        throw new TileDBError("array schema dimension 'schema.dim." + i + "' not specified");
      }
      String dimName = schemaDim.getFirst();
      Optional<StructField> sparkDimField =
          fields.stream().filter((StructField f) -> f.name().equals(dimName)).findAny();
      if (!sparkDimField.isPresent()) {
        throw new TileDBError(
            "specified write dimension 'schema.dim."
                + i
                + " = "
                + dimName
                + "' does not exist in spark dataframe schema");
      }
    }
    return schemaDims.stream().map(Pair::getFirst).toArray(String[]::new);
  }

  static Dimension toDimension(
      Context ctx, String dimName, int dimIdx, StructField field, TileDBDataSourceOptions options)
      throws TileDBError {
    DataType dataType = field.dataType();
    if ((dataType instanceof FloatType) || (dataType instanceof DoubleType)) {
      Optional<Double> realMin = options.getSchemaDimensionMinDomainDouble(dimIdx);
      Optional<Double> realMax = options.getSchemaDimensionMaxDomainDouble(dimIdx);
      Optional<Double> realExtent = options.getSchemaDimensionExtentDouble(dimIdx);
      if (dataType instanceof FloatType) {
        Float min = Float.MIN_VALUE;
        if (realMin.isPresent()) {
          min = BigDecimal.valueOf(realMin.get()).floatValue();
        }
        Float max = Float.MAX_VALUE;
        if (realMax.isPresent()) {
          max = BigDecimal.valueOf(realMax.get()).floatValue();
        }
        Float extent;
        if (realExtent.isPresent()) {
          extent = BigDecimal.valueOf(realExtent.get()).floatValue();
        } else {
          extent = min - max;
        }
        return new Dimension(
            ctx, field.name(), Datatype.TILEDB_FLOAT32, new Pair<>(min, max), extent);
      } else if (dataType instanceof DoubleType) {

        Double min = Double.MIN_VALUE;
        if (realMin.isPresent()) {
          min = realMin.get();
        }
        Double max = Double.MAX_VALUE;
        if (realMax.isPresent()) {
          max = realMax.get();
        }
        Double extent;
        if (realExtent.isPresent()) {
          extent = realExtent.get();
        } else {
          extent = max - min;
        }
        return new Dimension(
            ctx, field.name(), Datatype.TILEDB_FLOAT64, new Pair<>(min, max), extent);
      }
    } else {
      Optional<Long> longMin = options.getSchemaDimensionMinDomainLong(dimIdx);
      Optional<Long> longMax = options.getSchemaDimensionMaxDomainLong(dimIdx);
      Optional<Long> longExtent = options.getSchemaDimensionExtentLong(dimIdx);
      if (dataType instanceof IntegerType) {
        Integer min = Integer.MIN_VALUE;
        if (longMin.isPresent()) {
          min = Math.toIntExact(longMin.get());
        }
        Integer max = Integer.MAX_VALUE;
        if (longMax.isPresent()) {
          max = Math.toIntExact(longMax.get());
        }
        Integer extent;
        if (longExtent.isPresent()) {
          extent = Math.toIntExact(longExtent.get());
        } else {
          extent = max - min;
        }
        return new Dimension(
            ctx, field.name(), Datatype.TILEDB_INT32, new Pair<>(min, max), extent);
      } else if (dataType instanceof LongType) {
        Long min = Long.MIN_VALUE + 1l;
        if (longMin.isPresent()) {
          min = longMin.get();
        }
        Long max = Long.MAX_VALUE - 1l;
        if (longMax.isPresent()) {
          max = longMax.get();
        }
        Long extent;
        if (longExtent.isPresent()) {
          extent = longExtent.get();
        } else {
          extent = max;
        }
        return new Dimension(
            ctx, field.name(), Datatype.TILEDB_INT64, new Pair<>(min, max), extent);
      } else if (dataType instanceof ShortType) {
        Short min = Short.MIN_VALUE;
        if (longMin.isPresent()) {
          min = Short.valueOf(longMin.get().toString());
        }
        Short max = Short.MAX_VALUE;
        if (longMax.isPresent()) {
          max = Short.valueOf(longMax.get().toString());
        }
        Short extent;
        if (longExtent.isPresent()) {
          extent = Short.valueOf(longExtent.get().toString());
        } else {
          extent = Short.valueOf((short) (max - min));
        }
        return new Dimension(
            ctx, field.name(), Datatype.TILEDB_INT16, new Pair<>(min, max), extent);
      } else if (dataType instanceof ByteType) {
        Byte min = Byte.MIN_VALUE;
        if (longMin.isPresent()) {
          min = Byte.valueOf(longMin.get().toString());
        }
        Byte max = Byte.MAX_VALUE;
        if (longMax.isPresent()) {
          max = Byte.valueOf(longMin.get().toString());
        }
        Byte extent;
        if (longExtent.isPresent()) {
          extent = Byte.valueOf(longExtent.get().toString());
        } else {
          extent = Byte.valueOf((byte) (max - min));
        }
        return new Dimension(
            ctx, field.name(), Datatype.TILEDB_UINT8, new Pair<>(min, max), extent);
      }
    }
    throw new TileDBError(
        "Datatype not supported for TileDB spark write schema dimension: " + dataType);
  }

  static Attribute toAttribute(Context ctx, StructField field) throws TileDBError {
    DataType dataType = field.dataType();
    if (dataType instanceof IntegerType) {
      return new Attribute(ctx, field.name(), Integer.class);
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
    } else if (dataType instanceof StringType) {
      return new Attribute(ctx, field.name(), String.class).setCellVar();
    } else if (dataType instanceof ArrayType) {
      ArrayType arrayType = ((ArrayType) dataType);
      DataType elementType = arrayType.elementType();
      if (elementType instanceof IntegerType) {
        return new Attribute(ctx, field.name(), Integer.class).setCellVar();
      } else if (elementType instanceof ShortType) {
        return new Attribute(ctx, field.name(), Short.class).setCellVar();
      } else if (elementType instanceof ByteType) {
        return new Attribute(ctx, field.name(), Byte.class).setCellVar();
      } else if (elementType instanceof LongType) {
        return new Attribute(ctx, field.name(), Long.class).setCellVar();
      } else if (elementType instanceof FloatType) {
        return new Attribute(ctx, field.name(), Float.class).setCellVar();
      } else if (elementType instanceof DoubleType) {
        return new Attribute(ctx, field.name(), Double.class).setCellVar();
      } else {
        throw new TileDBError(
            "Spark Array DataType not supported for TileDB schema conversion: "
                + arrayType.toString());
      }
    } else {
      throw new TileDBError(
          "Spark DataType not supported for TileDB schema conversion: " + dataType.toString());
    }
  }
}
