package io.tiledb.spark;

import static io.tiledb.spark.util.addEpsilon;
import static java.lang.Math.abs;

import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.TileDBError;

public class Range implements java.io.Serializable {
  private Pair range;
  private Class dataClassType;

  Range(Pair range) {
    this.range = range;
    this.dataClassType = range.getFirst().getClass();
  }

  public <T extends Number> T width() {
    try {
      if (range.getFirst() instanceof Byte) {
        return (T) widthByte();
      } else if (range.getFirst() instanceof Short) {
        return (T) widthShort();
      } else if (range.getFirst() instanceof Integer) {
        return (T) widthInteger();
      } else if (range.getFirst() instanceof Long) {
        return (T) widthLong();
      } else if (range.getFirst() instanceof Float) {
        return (T) widthFloat();
      } else if (range.getFirst() instanceof Double) {
        return (T) widthDouble();
      }
    } catch (TileDBError tileDBError) {
      tileDBError.printStackTrace();
    }

    return (T) (Integer) (0);
  }

  private Integer widthByte() throws TileDBError {
    Pair<Byte, Byte> tmp = range;
    return ((Byte) addEpsilon(abs(tmp.getSecond() - tmp.getFirst()), Datatype.TILEDB_INT8))
        .intValue();
  }

  private Integer widthShort() throws TileDBError {
    Pair<Short, Short> tmp = range;
    return ((Short) addEpsilon(abs(tmp.getSecond() - tmp.getFirst()), Datatype.TILEDB_INT16))
        .intValue();
  }

  private Integer widthInteger() throws TileDBError {
    Pair<Integer, Integer> tmp = range;
    return (Integer) addEpsilon(abs(tmp.getSecond() - tmp.getFirst()), Datatype.TILEDB_INT32);
  }

  private Long widthLong() throws TileDBError {
    Pair<Long, Long> tmp = range;
    return (Long) addEpsilon(abs(tmp.getSecond() - tmp.getFirst()), Datatype.TILEDB_INT64);
  }

  private Float widthFloat() throws TileDBError {
    Pair<Float, Float> tmp = range;
    return (Float) addEpsilon(abs(tmp.getSecond() - tmp.getFirst()), Datatype.TILEDB_FLOAT32);
  }

  private Double widthDouble() throws TileDBError {
    Pair<Double, Double> tmp = range;
    return (Double) addEpsilon(abs(tmp.getSecond() - tmp.getFirst()), Datatype.TILEDB_FLOAT64);
  }

  public Class dataClassType() {
    return dataClassType;
  }

  public Object getFirst() {
    return range.getFirst();
  }

  public Object getSecond() {
    return range.getSecond();
  }

  public Pair getRange() {
    return range;
  }
}
