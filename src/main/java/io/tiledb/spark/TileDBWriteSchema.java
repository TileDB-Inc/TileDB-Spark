package io.tiledb.spark;

import io.tiledb.java.api.*;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.swing.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.ArrayType;
import scala.annotation.meta.field;

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
          extent = max - min;
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
        Integer extent;
        if (longExtent.isPresent()) {
          extent = Math.toIntExact(longExtent.get());
        } else {
          extent = 100;
        }
        Integer min = Integer.MIN_VALUE;
        if (longMin.isPresent()) {
          min = Math.toIntExact(longMin.get());
        }
        Integer max = (int) Integer.MAX_VALUE - extent;
        if (longMax.isPresent()) {
          max = Math.toIntExact(longMax.get());
        }
        return new Dimension(
            ctx, field.name(), Datatype.TILEDB_INT32, new Pair<>(min, max), extent);
      } else if (dataType instanceof LongType) {
        Long extent;

        if (longExtent.isPresent()) {
          extent = longExtent.get();
        } else {
          extent = 100L;
        }

        Long min = Long.MIN_VALUE;
        if (longMin.isPresent()) {
          min = longMin.get();
        }

        Long max = Long.MAX_VALUE - extent;
        if (longMax.isPresent()) {
          max = longMax.get();
        }

        return new Dimension(
            ctx, field.name(), Datatype.TILEDB_INT64, new Pair<>(min, max), extent);
      } else if (dataType instanceof ShortType) {
        short extent;

        if (longExtent.isPresent()) {
          extent = Short.valueOf(longExtent.get().toString());
        } else {
          extent = Short.MAX_VALUE;
        }
        Short min = Short.MIN_VALUE;
        if (longMin.isPresent()) {
          min = (short) (long) longMin.get();
        }
        Short max = Short.MAX_VALUE;
        if (longMax.isPresent()) {
          max = (short) (longMax.get() - extent);
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
          extent = Byte.MAX_VALUE;
        }
        return new Dimension(ctx, field.name(), Datatype.TILEDB_INT8, new Pair<>(min, max), extent);
      } else if (dataType instanceof DateType) {
        Long extent;
        if (longExtent.isPresent()) {
          extent = longExtent.get();
        } else {
          extent = 100L;
        }

        Long min = Long.MIN_VALUE;
        if (longMin.isPresent()) {
          min = longMin.get();
        }
        Long max = Long.MAX_VALUE - extent;
        if (longMax.isPresent()) {
          max = longMax.get();
        }

        return new Dimension(
            ctx, field.name(), Datatype.TILEDB_DATETIME_DAY, new Pair<>(min, max), extent);

      } else if (dataType instanceof TimestampType) {
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
          extent = max - min;
        }
        return new Dimension(
            ctx, field.name(), Datatype.TILEDB_DATETIME_MS, new Pair<>(min, max), extent);
      } else if (dataType instanceof StringType) {
        return new Dimension(ctx, field.name(), Datatype.TILEDB_STRING_ASCII, null, null);
      }
    }
    throw new TileDBError(
        "Datatype not supported for TileDB spark write schema dimension: " + dataType);
  }

  static Attribute toAttribute(Context ctx, StructField field, TileDBDataSourceOptions options)
      throws TileDBError {
    DataType dataType = field.dataType();
    Attribute attribute;
    if (dataType instanceof IntegerType) {
      attribute = new Attribute(ctx, field.name(), Integer.class);
    } else if (dataType instanceof ShortType) {
      attribute = new Attribute(ctx, field.name(), Short.class);
    } else if (dataType instanceof ByteType) {
      attribute = new Attribute(ctx, field.name(), Byte.class);
    } else if (dataType instanceof LongType) {
      attribute = new Attribute(ctx, field.name(), Long.class);
    } else if (dataType instanceof FloatType) {
      attribute = new Attribute(ctx, field.name(), Float.class);
    } else if (dataType instanceof DoubleType) {
      attribute = new Attribute(ctx, field.name(), Double.class);
    } else if (dataType instanceof DateType) {
      attribute = new Attribute(ctx, field.name(), Datatype.TILEDB_DATETIME_DAY);
    } else if (dataType instanceof TimestampType) {
      attribute = new Attribute(ctx, field.name(), Datatype.TILEDB_DATETIME_MS);
    } else if (dataType instanceof StringType) {
      attribute = new Attribute(ctx, field.name(), String.class).setCellVar();
    } else if (dataType instanceof ArrayType) {
      ArrayType arrayType = ((ArrayType) dataType);
      DataType elementType = arrayType.elementType();
      if (elementType instanceof IntegerType) {
        attribute = new Attribute(ctx, field.name(), Integer.class).setCellVar();
      } else if (elementType instanceof ShortType) {
        attribute = new Attribute(ctx, field.name(), Short.class).setCellVar();
      } else if (elementType instanceof ByteType) {
        attribute = new Attribute(ctx, field.name(), Byte.class).setCellVar();
      } else if (elementType instanceof LongType) {
        attribute = new Attribute(ctx, field.name(), Long.class).setCellVar();
      } else if (elementType instanceof FloatType) {
        attribute = new Attribute(ctx, field.name(), Float.class).setCellVar();
      } else if (elementType instanceof DoubleType) {
        attribute = new Attribute(ctx, field.name(), Double.class).setCellVar();
      } else {
        throw new TileDBError(
            "Spark Array DataType not supported for TileDB schema conversion: "
                + arrayType.toString());
      }
    } else {
      throw new TileDBError(
          "Spark DataType not supported for TileDB schema conversion: " + dataType.toString());
    }
    Optional<List<Pair<String, Integer>>> filterListDesc =
        options.getAttributeFilterList(field.name());
    try {
      if (filterListDesc.isPresent()) {
        try (FilterList filterList = createTileDBFilterList(ctx, filterListDesc.get())) {
          attribute.setFilterList(filterList);
        }
      }
    } catch (TileDBError err) {
      attribute.close();
      throw err;
    }
    return attribute;
  }

  public static FilterList createTileDBFilterList(
      Context ctx, List<Pair<String, Integer>> filterListDesc) throws TileDBError {
    FilterList filterList = new FilterList(ctx);
    try {
      for (Pair<String, Integer> filterDesc : filterListDesc) {
        String filterName = filterDesc.getFirst();
        Integer filterOption = filterDesc.getSecond();
        if (filterName.equalsIgnoreCase("NONE")) {
          try (Filter filter = new NoneFilter(ctx)) {
            filterList.addFilter(filter);
          }
        } else if (filterName.equalsIgnoreCase("GZIP")) {
          try (Filter filter = new GzipFilter(ctx, filterOption)) {
            filterList.addFilter(filter);
          }
        } else if (filterName.equalsIgnoreCase("ZSTD")) {
          try (Filter filter = new ZstdFilter(ctx, filterOption)) {
            filterList.addFilter(filter);
          }
        } else if (filterName.equalsIgnoreCase("LZ4")) {
          try (Filter filter = new LZ4Filter(ctx, filterOption)) {
            filterList.addFilter(filter);
          }
        } else if (filterName.equalsIgnoreCase("RLE")) {
          try (Filter filter = new LZ4Filter(ctx, filterOption)) {
            filterList.addFilter(filter);
          }
        } else if (filterName.equalsIgnoreCase("BZIP2")) {
          try (Filter filter = new Bzip2Filter(ctx, filterOption)) {
            filterList.addFilter(filter);
          }
        } else if (filterName.equalsIgnoreCase("DOUBLE_DELTA")) {
          try (Filter filter = new DoubleDeltaFilter(ctx, filterOption)) {
            filterList.addFilter(filter);
          }
        } else if (filterName.equalsIgnoreCase("BIT_WIDTH_REDUCTION")) {
          try (Filter filter = new BitWidthReductionFilter(ctx, filterOption)) {
            filterList.addFilter(filter);
          }
        } else if (filterName.equalsIgnoreCase("BITSHUFFLE")) {
          try (Filter filter = new BitShuffleFilter(ctx)) {
            filterList.addFilter(filter);
          }
        } else if (filterName.equalsIgnoreCase("BYTESHUFFLE")) {
          try (Filter filter = new ByteShuffleFilter(ctx)) {
            filterList.addFilter(filter);
          }
        } else if (filterName.equalsIgnoreCase("POSITIVE_DELTA")) {
          try (Filter filter = new ByteShuffleFilter(ctx)) {
            filterList.addFilter(filter);
          }
        }
      }
    } catch (TileDBError err) {
      filterList.close();
      throw err;
    }
    return filterList;
  }
}
