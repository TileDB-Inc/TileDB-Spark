package io.tiledb.spark;

import static io.tiledb.spark.util.addEpsilon;
import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.lang.Math.min;

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

  public Range merge(Range other) {
    if (other.dataClassType != this.dataClassType) return null;
    if (this.dataClassType == Byte.class) {
      Pair<Byte, Byte> range = this.range;
      Pair<Byte, Byte> otherRange = other.range;
      return new Range(
          new Pair(
              min(range.getFirst(), otherRange.getFirst()),
              max(range.getSecond(), otherRange.getSecond())));
    } else if (this.dataClassType == Short.class) {
      Pair<Short, Short> range = this.range;
      Pair<Short, Short> otherRange = other.range;
      return new Range(
          new Pair(
              min(range.getFirst(), otherRange.getFirst()),
              max(range.getSecond(), otherRange.getSecond())));
    } else if (this.dataClassType == Integer.class) {
      Pair<Integer, Integer> range = this.range;
      Pair<Integer, Integer> otherRange = other.range;
      return new Range(
          new Pair(
              min(range.getFirst(), otherRange.getFirst()),
              max(range.getSecond(), otherRange.getSecond())));
    } else if (this.dataClassType == Long.class) {
      Pair<Long, Long> range = this.range;
      Pair<Long, Long> otherRange = other.range;
      return new Range(
          new Pair(
              min(range.getFirst(), otherRange.getFirst()),
              max(range.getSecond(), otherRange.getSecond())));
    } else if (this.dataClassType == Float.class) {
      Pair<Float, Float> range = this.range;
      Pair<Float, Float> otherRange = other.range;
      return new Range(
          new Pair(
              min(range.getFirst(), otherRange.getFirst()),
              max(range.getSecond(), otherRange.getSecond())));
    } else if (this.dataClassType == Double.class) {
      Pair<Double, Double> range = this.range;
      Pair<Double, Double> otherRange = other.range;
      return new Range(
          new Pair(
              min(range.getFirst(), otherRange.getFirst()),
              max(range.getSecond(), otherRange.getSecond())));
    }
    return null;
  }

  public boolean canMerge(Range other) throws TileDBError {
    if (other.dataClassType != this.dataClassType) return false;

    if (this.dataClassType == Byte.class) {
      Pair<Byte, Byte> range = this.range;
      Pair<Byte, Byte> otherRange = other.range;

      // If this range completely encompasses the other range it can be merged
      // That is if we have to ranges, [1, 10] and [3, 7]. Then 1 < 3 AND 10 > 7 so [3, 7] can be
      // merged [1, 10]
      if (range.getFirst() <= otherRange.getFirst() && range.getSecond() >= otherRange.getSecond())
        return true;

      // If the other range completely encompasses the other range it can be merged
      // That is if we have to ranges, [3, 7] and [1, 10]. Then 1 < 3 AND 10 > 7 so [3, 7] can be
      // merged [1, 10]
      if (otherRange.getFirst() <= range.getFirst() && otherRange.getSecond() >= range.getSecond())
        return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [1, 5] and [3, 7]. Then 1 < 3 AND 5 < 7 AND 5 > 3 so these
      // can be merged [1, 7]
      if (range.getFirst() <= otherRange.getFirst()
          && range.getSecond() <= otherRange.getSecond()
          && range.getSecond() >= otherRange.getFirst()) return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [1, 5] and [3, 7]. Then 1 < 3 AND 5 < 7 AND 5 > 3 so these
      // can be merged [1, 7]
      if (otherRange.getFirst() <= range.getFirst()
          && otherRange.getSecond() <= range.getSecond()
          && otherRange.getSecond() >= range.getFirst()) return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [3, 7] and [1, 5]. Then 3 > 1 AND 3 < 5 AND 7 > 1 so these
      // can be merged [1, 7]
      if (range.getFirst() >= otherRange.getFirst()
          && range.getFirst() <= otherRange.getSecond()
          && range.getSecond() >= otherRange.getSecond()) return true;

      // If the min value of range is adjacent to the min or max value of other range we can merge
      // them
      // That is if we have ranges [1, 1] and [2, 2] they can be merged.
      if (range.getFirst() == addEpsilon(otherRange.getFirst(), Datatype.TILEDB_INT8)
          || range.getFirst() == addEpsilon(otherRange.getSecond(), Datatype.TILEDB_INT8))
        return true;

      // If the min value of range is adjacent to the min or max value of other range we can merge
      // them
      // That is if we have ranges [1, 3] and [3, 7] they can be merged.
      // Or [1, 5] and [3, 6]
      if (range.getSecond() == addEpsilon(otherRange.getFirst(), Datatype.TILEDB_INT8)
          || range.getSecond() == addEpsilon(otherRange.getSecond(), Datatype.TILEDB_INT8))
        return true;
    } else if (this.dataClassType == Short.class) {
      Pair<Short, Short> range = this.range;
      Pair<Short, Short> otherRange = other.range;

      // If this range completely encompasses the other range it can be merged
      // That is if we have to ranges, [1, 10] and [3, 7]. Then 1 < 3 AND 10 > 7 so [3, 7] can be
      // merged [1, 10]
      if (range.getFirst() <= otherRange.getFirst() && range.getSecond() >= otherRange.getSecond())
        return true;

      // If the other range completely encompasses the other range it can be merged
      // That is if we have to ranges, [3, 7] and [1, 10]. Then 1 < 3 AND 10 > 7 so [3, 7] can be
      // merged [1, 10]
      if (otherRange.getFirst() <= range.getFirst() && otherRange.getSecond() >= range.getSecond())
        return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [1, 5] and [3, 7]. Then 1 < 3 AND 5 < 7 AND 5 > 3 so these
      // can be merged [1, 7]
      if (range.getFirst() <= otherRange.getFirst()
          && range.getSecond() <= otherRange.getSecond()
          && range.getSecond() >= otherRange.getFirst()) return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [1, 5] and [3, 7]. Then 1 < 3 AND 5 < 7 AND 5 > 3 so these
      // can be merged [1, 7]
      if (otherRange.getFirst() <= range.getFirst()
          && otherRange.getSecond() <= range.getSecond()
          && otherRange.getSecond() >= range.getFirst()) return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [3, 7] and [1, 5]. Then 3 > 1 AND 3 < 5 AND 7 > 1 so these
      // can be merged [1, 7]
      if (range.getFirst() >= otherRange.getFirst()
          && range.getFirst() <= otherRange.getSecond()
          && range.getSecond() >= otherRange.getSecond()) return true;

      // If the min value of range is adjacent to the min or max value of other range we can merge
      // them
      // That is if we have ranges [1, 1] and [2, 2] they can be merged.
      if (range.getFirst() == addEpsilon(otherRange.getFirst(), Datatype.TILEDB_INT16)
          || range.getFirst() == addEpsilon(otherRange.getSecond(), Datatype.TILEDB_INT16))
        return true;

      // If the min value of range is adjacent to the min or max value of other range we can merge
      // them
      // That is if we have ranges [1, 3] and [3, 7] they can be merged.
      // Or [1, 5] and [3, 6]
      if (range.getSecond() == addEpsilon(otherRange.getFirst(), Datatype.TILEDB_INT16)
          || range.getSecond() == addEpsilon(otherRange.getSecond(), Datatype.TILEDB_INT16))
        return true;

    } else if (this.dataClassType == Integer.class) {
      Pair<Integer, Integer> range = this.range;
      Pair<Integer, Integer> otherRange = other.range;

      // If this range completely encompasses the other range it can be merged
      // That is if we have to ranges, [1, 10] and [3, 7]. Then 1 < 3 AND 10 > 7 so [3, 7] can be
      // merged [1, 10]
      if (range.getFirst() <= otherRange.getFirst() && range.getSecond() >= otherRange.getSecond())
        return true;

      // If the other range completely encompasses the other range it can be merged
      // That is if we have to ranges, [3, 7] and [1, 10]. Then 1 < 3 AND 10 > 7 so [3, 7] can be
      // merged [1, 10]
      if (otherRange.getFirst() <= range.getFirst() && otherRange.getSecond() >= range.getSecond())
        return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [1, 5] and [3, 7]. Then 1 < 3 AND 5 < 7 AND 5 > 3 so these
      // can be merged [1, 7]
      if (range.getFirst() <= otherRange.getFirst()
          && range.getSecond() <= otherRange.getSecond()
          && range.getSecond() >= otherRange.getFirst()) return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [1, 5] and [3, 7]. Then 1 < 3 AND 5 < 7 AND 5 > 3 so these
      // can be merged [1, 7]
      if (otherRange.getFirst() <= range.getFirst()
          && otherRange.getSecond() <= range.getSecond()
          && otherRange.getSecond() >= range.getFirst()) return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [3, 7] and [1, 5]. Then 3 > 1 AND 3 < 5 AND 7 > 1 so these
      // can be merged [1, 7]
      if (range.getFirst() >= otherRange.getFirst()
          && range.getFirst() <= otherRange.getSecond()
          && range.getSecond() >= otherRange.getSecond()) return true;

      // If the min value of range is adjacent to the min or max value of other range we can merge
      // them
      // That is if we have ranges [1, 1] and [2, 2] they can be merged.
      if (range.getFirst() == addEpsilon(otherRange.getFirst(), Datatype.TILEDB_INT32)
          || range.getFirst() == addEpsilon(otherRange.getSecond(), Datatype.TILEDB_INT32))
        return true;

      // If the min value of range is adjacent to the min or max value of other range we can merge
      // them
      // That is if we have ranges [1, 3] and [3, 7] they can be merged.
      // Or [1, 5] and [3, 6TILEDB_INT32]
      if (range.getSecond() == addEpsilon(otherRange.getFirst(), Datatype.TILEDB_INT32)
          || range.getSecond() == addEpsilon(otherRange.getSecond(), Datatype.TILEDB_INT32))
        return true;
    } else if (this.dataClassType == Long.class) {
      Pair<Long, Long> range = this.range;
      Pair<Long, Long> otherRange = other.range;

      // If this range completely encompasses the other range it can be merged
      // That is if we have to ranges, [1, 10] and [3, 7]. Then 1 < 3 AND 10 > 7 so [3, 7] can be
      // merged [1, 10]
      if (range.getFirst() <= otherRange.getFirst() && range.getSecond() >= otherRange.getSecond())
        return true;

      // If the other range completely encompasses the other range it can be merged
      // That is if we have to ranges, [3, 7] and [1, 10]. Then 1 < 3 AND 10 > 7 so [3, 7] can be
      // merged [1, 10]
      if (otherRange.getFirst() <= range.getFirst() && otherRange.getSecond() >= range.getSecond())
        return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [1, 5] and [3, 7]. Then 1 < 3 AND 5 < 7 AND 5 > 3 so these
      // can be merged [1, 7]
      if (range.getFirst() <= otherRange.getFirst()
          && range.getSecond() <= otherRange.getSecond()
          && range.getSecond() >= otherRange.getFirst()) return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [1, 5] and [3, 7]. Then 1 < 3 AND 5 < 7 AND 5 > 3 so these
      // can be merged [1, 7]
      if (otherRange.getFirst() <= range.getFirst()
          && otherRange.getSecond() <= range.getSecond()
          && otherRange.getSecond() >= range.getFirst()) return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [3, 7] and [1, 5]. Then 3 > 1 AND 3 < 5 AND 7 > 1 so these
      // can be merged [1, 7]
      if (range.getFirst() >= otherRange.getFirst()
          && range.getFirst() <= otherRange.getSecond()
          && range.getSecond() >= otherRange.getSecond()) return true;

      // If the min value of range is adjacent to the min or max value of other range we can merge
      // them
      // That is if we have ranges [1, 1] and [2, 2] they can be merged.
      if (range.getFirst() == addEpsilon(otherRange.getFirst(), Datatype.TILEDB_INT64)
          || range.getFirst() == addEpsilon(otherRange.getSecond(), Datatype.TILEDB_INT64))
        return true;

      // If the min value of range is adjacent to the min or max value of other range we can merge
      // them
      // That is if we have ranges [1, 3] and [3, 7] they can be merged.
      // Or [1, 5] and [3, 6]
      if (range.getSecond() == addEpsilon(otherRange.getFirst(), Datatype.TILEDB_INT64)
          || range.getSecond() == addEpsilon(otherRange.getSecond(), Datatype.TILEDB_INT64))
        return true;
    } else if (this.dataClassType == Float.class) {
      Pair<Float, Float> range = this.range;
      Pair<Float, Float> otherRange = other.range;

      // If this range completely encompasses the other range it can be merged
      // That is if we have to ranges, [1, 10] and [3, 7]. Then 1 < 3 AND 10 > 7 so [3, 7] can be
      // merged [1, 10]
      if (range.getFirst() <= otherRange.getFirst() && range.getSecond() >= otherRange.getSecond())
        return true;

      // If the other range completely encompasses the other range it can be merged
      // That is if we have to ranges, [3, 7] and [1, 10]. Then 1 < 3 AND 10 > 7 so [3, 7] can be
      // merged [1, 10]
      if (otherRange.getFirst() <= range.getFirst() && otherRange.getSecond() >= range.getSecond())
        return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [1, 5] and [3, 7]. Then 1 < 3 AND 5 < 7 AND 5 > 3 so these
      // can be merged [1, 7]
      if (range.getFirst() <= otherRange.getFirst()
          && range.getSecond() <= otherRange.getSecond()
          && range.getSecond() >= otherRange.getFirst()) return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [1, 5] and [3, 7]. Then 1 < 3 AND 5 < 7 AND 5 > 3 so these
      // can be merged [1, 7]
      if (otherRange.getFirst() <= range.getFirst()
          && otherRange.getSecond() <= range.getSecond()
          && otherRange.getSecond() >= range.getFirst()) return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [3, 7] and [1, 5]. Then 3 > 1 AND 3 < 5 AND 7 > 1 so these
      // can be merged [1, 7]
      if (range.getFirst() >= otherRange.getFirst()
          && range.getFirst() <= otherRange.getSecond()
          && range.getSecond() >= otherRange.getSecond()) return true;

      // If the min value of range is adjacent to the min or max value of other range we can merge
      // them
      // That is if we have ranges [1, 1] and [2, 2] they can be merged.
      if (range.getFirst() == addEpsilon(otherRange.getFirst(), Datatype.TILEDB_FLOAT32)
          || range.getFirst() == addEpsilon(otherRange.getSecond(), Datatype.TILEDB_FLOAT32))
        return true;

      // If the min value of range is adjacent to the min or max value of other range we can merge
      // them
      // That is if we have ranges [1, 3] and [3, 7] they can be merged.
      // Or [1, 5] and [3, 6]
      if (range.getSecond() == addEpsilon(otherRange.getFirst(), Datatype.TILEDB_FLOAT32)
          || range.getSecond() == addEpsilon(otherRange.getSecond(), Datatype.TILEDB_FLOAT32))
        return true;
    } else if (this.dataClassType == Double.class) {
      Pair<Double, Double> range = this.range;
      Pair<Double, Double> otherRange = other.range;

      // If this range completely encompasses the other range it can be merged
      // That is if we have to ranges, [1, 10] and [3, 7]. Then 1 < 3 AND 10 > 7 so [3, 7] can be
      // merged [1, 10]
      if (range.getFirst() <= otherRange.getFirst() && range.getSecond() >= otherRange.getSecond())
        return true;

      // If the other range completely encompasses the other range it can be merged
      // That is if we have to ranges, [3, 7] and [1, 10]. Then 1 < 3 AND 10 > 7 so [3, 7] can be
      // merged [1, 10]
      if (otherRange.getFirst() <= range.getFirst() && otherRange.getSecond() >= range.getSecond())
        return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [1, 5] and [3, 7]. Then 1 < 3 AND 5 < 7 AND 5 > 3 so these
      // can be merged [1, 7]
      if (range.getFirst() <= otherRange.getFirst()
          && range.getSecond() <= otherRange.getSecond()
          && range.getSecond() >= otherRange.getFirst()) return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [1, 5] and [3, 7]. Then 1 < 3 AND 5 < 7 AND 5 > 3 so these
      // can be merged [1, 7]
      if (otherRange.getFirst() <= range.getFirst()
          && otherRange.getSecond() <= range.getSecond()
          && otherRange.getSecond() >= range.getFirst()) return true;

      // If this range partially encompasses the other range it can be merged
      // That is if we have to ranges, [3, 7] and [1, 5]. Then 3 > 1 AND 3 < 5 AND 7 > 1 so these
      // can be merged [1, 7]
      if (range.getFirst() >= otherRange.getFirst()
          && range.getFirst() <= otherRange.getSecond()
          && range.getSecond() >= otherRange.getSecond()) return true;

      // If the min value of range is adjacent to the min or max value of other range we can merge
      // them
      // That is if we have ranges [1, 1] and [2, 2] they can be merged.
      if (range.getFirst() == addEpsilon(otherRange.getFirst(), Datatype.TILEDB_FLOAT64)
          || range.getFirst() == addEpsilon(otherRange.getSecond(), Datatype.TILEDB_FLOAT64))
        return true;

      // If the min value of range is adjacent to the min or max value of other range we can merge
      // them
      // That is if we have ranges [1, 3] and [3, 7] they can be merged.
      // Or [1, 5] and [3, 6]
      if (range.getSecond() == addEpsilon(otherRange.getFirst(), Datatype.TILEDB_FLOAT64)
          || range.getSecond() == addEpsilon(otherRange.getSecond(), Datatype.TILEDB_FLOAT64))
        return true;
    }

    return false;
  }
}
