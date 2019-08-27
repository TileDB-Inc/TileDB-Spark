package io.tiledb.spark;

import io.tiledb.java.api.*;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.ArrayType;

public class TileDBWriteSchema {

  static String[] getSchemaDimensionOptions(StructType sparkSchema, TileDBDataSourceOptions options)
      throws TileDBError {
    Optional<List<Pair<String, Integer>>> schemaDimsOpt = options.getSchemaDimensions();
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

  static Dimension toDimension(Context ctx, StructField field) throws TileDBError {
    DataType dataType = field.dataType();
    if (dataType instanceof IntegerType) {
      int extent = 10000;
      int min = Integer.MIN_VALUE;
      int max = Integer.MAX_VALUE - extent;
      return new Dimension(ctx, field.name(), Datatype.TILEDB_INT32, new Pair<>(min, max), extent);
    } else if (dataType instanceof LongType) {
      long extent = 10000;
      long min = Long.MIN_VALUE;
      long max = Long.MAX_VALUE - extent;
      return new Dimension(ctx, field.name(), Datatype.TILEDB_INT64, new Pair<>(min, max), extent);
    } else if (dataType instanceof ShortType) {
      short extent = 10000;
      short min = Short.MIN_VALUE;
      short max = (short) (Short.MAX_VALUE - extent);
      return new Dimension(ctx, field.name(), Datatype.TILEDB_INT16, new Pair<>(min, max), extent);
    } else if (dataType instanceof ByteType) {
      byte extent = 64;
      byte min = Byte.MIN_VALUE;
      byte max = (byte) (Byte.MAX_VALUE - extent);
      return new Dimension(ctx, field.name(), Datatype.TILEDB_UINT8, new Pair<>(min, max), extent);
    } else {
      throw new TileDBError("Datatype not supported for dimension: " + dataType);
    }
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
