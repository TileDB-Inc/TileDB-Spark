package io.tiledb.spark;

import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.TileDBError;
import java.util.ArrayList;
import java.util.List;

public class util {

  /* Returns v + eps, where eps is the smallest value for the datatype such that v + eps > v. */
  public static Object addEpsilon(Object value, Datatype type) throws TileDBError {
    switch (type) {
      case TILEDB_CHAR:
      case TILEDB_INT8:
        return ((byte) value) < Byte.MAX_VALUE ? ((byte) value + 1) : value;
      case TILEDB_INT16:
        return ((short) value) < Short.MAX_VALUE ? ((short) value + 1) : value;
      case TILEDB_INT32:
        return ((int) value) < Integer.MAX_VALUE ? ((int) value + 1) : value;
      case TILEDB_INT64:
        return ((long) value) < Long.MAX_VALUE ? ((long) value + 1) : value;
      case TILEDB_UINT8:
        return ((short) value) < ((short) Byte.MAX_VALUE + 1) ? ((short) value + 1) : value;
      case TILEDB_UINT16:
        return ((int) value) < ((int) Short.MAX_VALUE + 1) ? ((int) value + 1) : value;
      case TILEDB_UINT32:
        return ((long) value) < ((long) Integer.MAX_VALUE + 1) ? ((long) value + 1) : value;
      case TILEDB_UINT64:
        return ((long) value) < ((long) Integer.MAX_VALUE + 1) ? ((long) value + 1) : value;
      case TILEDB_FLOAT32:
        return ((float) value) < Float.MAX_VALUE ? Math.nextUp((float) value) : value;
      case TILEDB_FLOAT64:
        return ((double) value) < Double.MAX_VALUE ? Math.nextUp((double) value) : value;
      default:
        throw new TileDBError("Unsupported TileDB Datatype enum: " + type);
    }
  }

  /* Returns v - eps, where eps is the smallest value for the datatype such that v - eps < v. */
  public static Object subtractEpsilon(Object value, Datatype type) throws TileDBError {
    switch (type) {
      case TILEDB_CHAR:
      case TILEDB_INT8:
        return ((byte) value) > Byte.MIN_VALUE ? ((byte) value - 1) : value;
      case TILEDB_INT16:
        return ((short) value) > Short.MIN_VALUE ? ((short) value - 1) : value;
      case TILEDB_INT32:
        return ((int) value) > Integer.MIN_VALUE ? ((int) value - 1) : value;
      case TILEDB_INT64:
        return ((long) value) > Long.MIN_VALUE ? ((long) value - 1) : value;
      case TILEDB_UINT8:
        return ((short) value) > ((short) Byte.MIN_VALUE - 1) ? ((short) value - 1) : value;
      case TILEDB_UINT16:
        return ((int) value) > ((int) Short.MIN_VALUE - 1) ? ((int) value - 1) : value;
      case TILEDB_UINT32:
        return ((long) value) > ((long) Integer.MIN_VALUE - 1) ? ((long) value - 1) : value;
      case TILEDB_UINT64:
        return ((long) value) > ((long) Integer.MIN_VALUE - 1) ? ((long) value - 1) : value;
      case TILEDB_FLOAT32:
        return ((float) value) > Float.MIN_VALUE ? Math.nextDown((float) value) : value;
      case TILEDB_FLOAT64:
        return ((double) value) > Double.MIN_VALUE ? Math.nextDown((double) value) : value;
      default:
        throw new TileDBError("Unsupported TileDB Datatype enum: " + type);
    }
  }

  /**
   * Helper function because java doesn't support template numeric operations
   *
   * @param a first value to operate on
   * @param b second value to operate on
   * @param dataClassType class type, used to cast objects
   * @return operatior results
   */
  public static Object add_objects(Object a, Object b, Class dataClassType) {
    if (dataClassType == Byte.class) {
      return (Byte) a + (Byte) b;
    } else if (dataClassType == Short.class) {
      return (Short) a + (Short) b;
    } else if (dataClassType == Integer.class) {
      return (Integer) a + (Integer) b;
    } else if (dataClassType == Long.class) {
      return (Long) a + (Long) b;
    } else if (dataClassType == Float.class) {
      return (Float) a + (Float) b;
    }

    // Else assume double
    return (Double) a + (Double) b;
  }

  /**
   * Helper function because java doesn't support template numeric operations
   *
   * @param a first value to operate on
   * @param b second value to operate on
   * @param dataClassType class type, used to cast objects
   * @return operatior results
   */
  public static Object subtract_objects(Object a, Object b, Class dataClassType) {
    if (dataClassType == Byte.class) {
      return (Byte) a - (Byte) b;
    } else if (dataClassType == Short.class) {
      return (Short) a - (Short) b;
    } else if (dataClassType == Integer.class) {
      return (Integer) a - (Integer) b;
    } else if (dataClassType == Long.class) {
      return (Long) a - (Long) b;
    } else if (dataClassType == Float.class) {
      return (Float) a - (Float) b;
    }

    // Else assume double
    return (Double) a - (Double) b;
  }

  /**
   * Helper function because java doesn't support template numeric operations
   *
   * @param a first value to operate on
   * @param b second value to operate on
   * @param dataClassType class type, used to cast objects
   * @return operatior results
   */
  public static Object divide_objects(Object a, Object b, Class dataClassType) {
    if (dataClassType == Byte.class) {
      return (Byte) a / (Byte) b;
    } else if (dataClassType == Short.class) {
      return (Short) a / (Short) b;
    } else if (dataClassType == Integer.class) {
      return (Integer) a / (Integer) b;
    } else if (dataClassType == Long.class) {
      return (Long) a / (Long) b;
    } else if (dataClassType == Float.class) {
      return (Float) a / (Float) b;
    }

    // Else assume double
    return (Double) a / (Double) b;
  }

  /**
   * Helper function because java doesn't support template numeric operations
   *
   * @param a first value to operate on
   * @param b second value to operate on
   * @param dataClassType class type, used to cast objects
   * @return operatior results
   */
  public static Object modulo_objects(Object a, Object b, Class dataClassType) {
    if (dataClassType == Byte.class) {
      return (Byte) a % (Byte) b;
    } else if (dataClassType == Short.class) {
      return (Short) a % (Short) b;
    } else if (dataClassType == Integer.class) {
      return (Integer) a % (Integer) b;
    } else if (dataClassType == Long.class) {
      return (Long) a % (Long) b;
    } else if (dataClassType == Float.class) {
      return (Float) a % (Float) b;
    }

    // Else assume double
    return (Double) a % (Double) b;
  }

  /**
   * Generate all combination of subarrays from a list of ranges per dimension
   *
   * @param ranges ranges per dimension to build from
   * @param results stored here
   * @param index recursive index position
   * @param current current partial subarray
   */
  public static void generateAllSubarrays(
      List<List<Range>> ranges, List<SubArrayRanges> results, int index, List<Range> current) {
    if (index == ranges.size()) {
      results.add(new SubArrayRanges(current, current.get(0).dataClassType()));
      return;
    }

    for (Range rangeForSingleDimension : ranges.get(index)) {
      List<Range> currentCopy = new ArrayList<>(current);
      currentCopy.add(rangeForSingleDimension);
      generateAllSubarrays(ranges, results, index + 1, currentCopy);
    }
  }
}
