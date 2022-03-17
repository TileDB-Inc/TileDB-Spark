package io.tiledb.spark;

import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.TileDBError;
import io.tiledb.java.api.Util;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.log4j.Logger;

public class util {
  private static Logger logger = Logger.getLogger(Util.class);
  /* Returns v + eps, where eps is the smallest value for the datatype such that v + eps > v. */
  public static Number addEpsilon(Number value, Datatype type) throws TileDBError {
    switch (type) {
      case TILEDB_CHAR:
      case TILEDB_INT8:
        return value.byteValue() < Byte.MAX_VALUE
            ? (byte) (value.byteValue() + 1)
            : value.byteValue();
      case TILEDB_INT16:
        return value.shortValue() < Short.MAX_VALUE
            ? (short) (value.shortValue() + 1)
            : value.byteValue();
      case TILEDB_INT32:
        return value.intValue() < Integer.MAX_VALUE ? (value.intValue() + 1) : value.intValue();
      case TILEDB_INT64:
        return value.longValue() < Long.MAX_VALUE ? (value.longValue() + 1) : value;
      case TILEDB_UINT8:
        return value.shortValue() < ((short) Byte.MAX_VALUE + 1)
            ? (short) (value.shortValue() + 1)
            : value.shortValue();
      case TILEDB_UINT16:
        return value.intValue() < ((int) Short.MAX_VALUE + 1)
            ? (value.intValue() + 1)
            : value.intValue();
      case TILEDB_UINT32:
      case TILEDB_UINT64:
        return value.longValue() < ((long) Integer.MAX_VALUE + 1)
            ? (value.longValue() + 1)
            : value.longValue();
      case TILEDB_FLOAT32:
        return value.floatValue() < Float.MAX_VALUE
            ? Math.nextUp(value.floatValue())
            : value.floatValue();
      case TILEDB_FLOAT64:
        return value.doubleValue() < Double.MAX_VALUE
            ? Math.nextUp(value.doubleValue())
            : value.doubleValue();
      default:
        throw new TileDBError("Unsupported TileDB Datatype enum: " + type);
    }
  }

  public static URI tryGetArrayURI(TileDBDataSourceOptions tiledbOptions) {
    Optional<URI> arrayURI;
    try {
      arrayURI = tiledbOptions.getArrayURI();
    } catch (URISyntaxException ex) {
      throw new RuntimeException("Error parsing array URI option: " + ex.getMessage());
    }
    if (!arrayURI.isPresent()) {
      throw new RuntimeException("TileDB URI option required");
    }
    return arrayURI.get();
  }

  /**
   * Returns the record size in bytes. Current implementation always returns 8, but it is intended
   * for future use.
   *
   * @param clazz The input ValueVector class
   * @return The size in bytes
   */
  public static long getDefaultRecordByteCount(Class<? extends ValueVector> clazz) {
    if (BaseVariableWidthVector.class.isAssignableFrom(clazz)) return 8;
    else if (BaseValueVector.class.isAssignableFrom(clazz)) return 8;

    logger.warn(
        "Did not found size of the class with name: "
            + clazz.getCanonicalName()
            + " returning 8 as the record size");

    return 8;
  }

  public static int longToInt(long num) throws ArithmeticException {

    if (num > Integer.MAX_VALUE)
      throw new ArithmeticException(
          String.format("Value %d exceeds the maximum integer value.", num));

    return (int) num;
  }

  /* Returns v - eps, where eps is the smallest value for the datatype such that v - eps < v. */
  public static Number subtractEpsilon(Number value, Datatype type) throws TileDBError {
    switch (type) {
      case TILEDB_CHAR:
      case TILEDB_INT8:
        return value.byteValue() > Byte.MIN_VALUE
            ? (byte) (value.byteValue() - 1)
            : value.byteValue();
      case TILEDB_INT16:
        return value.shortValue() > Short.MIN_VALUE
            ? (short) (value.shortValue() - 1)
            : value.shortValue();
      case TILEDB_INT32:
        return value.intValue() > Integer.MIN_VALUE ? (value.intValue() - 1) : value.intValue();
      case TILEDB_INT64:
        return value.longValue() > Long.MIN_VALUE ? (value.longValue() - 1) : value.longValue();
      case TILEDB_UINT8:
        return value.shortValue() > ((short) Byte.MIN_VALUE - 1)
            ? (short) (value.shortValue() - 1)
            : value.shortValue();
      case TILEDB_UINT16:
        return value.intValue() > ((int) Short.MIN_VALUE - 1)
            ? (value.intValue() - 1)
            : value.intValue();
      case TILEDB_UINT32:
      case TILEDB_UINT64:
        return value.longValue() > ((long) Integer.MIN_VALUE - 1)
            ? (value.longValue() - 1)
            : value.longValue();
      case TILEDB_FLOAT32:
        return (value.floatValue()) > Float.MIN_VALUE
            ? Math.nextDown(value.floatValue())
            : value.floatValue();
      case TILEDB_FLOAT64:
        return (value.doubleValue()) > Double.MIN_VALUE
            ? Math.nextDown(value.doubleValue())
            : value.doubleValue();
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
  public static Number addObjects(Number a, Number b, Class dataClassType) {
    if (dataClassType == Byte.class) {
      return a.byteValue() + b.byteValue();
    } else if (dataClassType == Short.class) {
      return a.shortValue() + b.shortValue();
    } else if (dataClassType == Integer.class) {
      return a.intValue() + b.intValue();
    } else if (dataClassType == Long.class) {
      return a.longValue() + b.longValue();
    } else if (dataClassType == Float.class) {
      return a.floatValue() + b.floatValue();
    }

    // Else assume double
    return a.doubleValue() + b.doubleValue();
  }

  /**
   * Helper function because java doesn't support template numeric operations
   *
   * @param a first value to operate on
   * @param b second value to operate on
   * @param dataClassType class type, used to cast objects
   * @return operatior results
   */
  public static Number subtractObjects(Number a, Number b, Class dataClassType) {
    if (dataClassType == Byte.class) {
      return a.byteValue() - b.byteValue();
    } else if (dataClassType == Short.class) {
      return a.shortValue() - b.shortValue();
    } else if (dataClassType == Integer.class) {
      return a.intValue() - b.intValue();
    } else if (dataClassType == Long.class) {
      return (Long) a - (Long) b;
    } else if (dataClassType == Float.class) {
      return a.floatValue() - b.floatValue();
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
  public static Number divideObjects(Number a, Number b, Class dataClassType) {
    if (dataClassType == Byte.class) {
      return a.byteValue() / b.byteValue();
    } else if (dataClassType == Short.class) {
      return a.shortValue() / b.shortValue();
    } else if (dataClassType == Integer.class) {
      return a.intValue() / b.intValue();
    } else if (dataClassType == Long.class) {
      return (Long) a / (Long) b;
    } else if (dataClassType == Float.class) {
      return a.floatValue() / b.floatValue();
    }

    // Else assume double
    return (Double) a / (Double) b;
  }

  /**
   * Division with ceiling property for non-floating point numbers For example, while usually a
   * division like 4/3 results into floor (1.333)=1, with this function, it will result into ceil
   * (1.333)=2.
   *
   * @param a first value to operate on
   * @param b second value to operate on
   * @param dataClassType class type, used to cast objects
   * @return operatior results
   */
  public static Object divideCeilingObjects(Number a, Number b, Class dataClassType) {
    if (dataClassType == Byte.class) {
      return a.byteValue() / b.byteValue();
    } else if (dataClassType == Short.class) {
      return a.shortValue() / b.shortValue();
    } else if (dataClassType == Integer.class) {
      return (int) Math.ceil(a.intValue() * 1.0 / b.intValue());
    } else if (dataClassType == Long.class) {
      return (long) Math.ceil((Long) a * 1.0 / (Long) b);
    } else if (dataClassType == Float.class) {
      return a.floatValue() / b.floatValue();
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
   * @return operation results
   */
  public static Object moduloObjects(Number a, Number b, Class dataClassType) {
    if (dataClassType == Byte.class) {
      return a.byteValue() % b.byteValue();
    } else if (dataClassType == Short.class) {
      return a.shortValue() % b.shortValue();
    } else if (dataClassType == Integer.class) {
      return a.intValue() % b.intValue();
    } else if (dataClassType == Long.class) {
      return (Long) a % (Long) b;
    } else if (dataClassType == Float.class) {
      return a.floatValue() % b.floatValue();
    }

    // Else assume double
    return (Double) a % (Double) b;
  }
  /**
   * Casts an input integer to a specific numeric type
   *
   * @param srcNum The integer to be casted
   * @param dataClassType class type, used to cast objects
   * @return The casted number
   */
  public static Object castInt(Integer srcNum, Class dataClassType) {
    if (dataClassType == Byte.class) {
      return srcNum.byteValue();
    } else if (dataClassType == Short.class) {
      return srcNum.shortValue();
    } else if (dataClassType == Long.class) {
      return srcNum.longValue();
    } else if (dataClassType == Float.class) {
      return srcNum.floatValue();
    } else if (dataClassType == Double.class) {
      return srcNum.doubleValue();
    }

    return srcNum;
  }

  /**
   * Casts an input integer to a specific numeric type
   *
   * @param srcNum The integer to be casted
   * @param dataClassType class type, used to cast objects
   * @return The casted number
   */
  public static Object castLong(long srcNum, Class dataClassType) {
    if (dataClassType == Byte.class) {
      return (byte) srcNum;
    } else if (dataClassType == Short.class) {
      return (short) srcNum;
    } else if (dataClassType == Integer.class) {
      return (int) srcNum;
    } else if (dataClassType == Float.class) {
      return (float) srcNum;
    } else if (dataClassType == Double.class) {
      return (double) srcNum;
    }

    return srcNum;
  }

  /**
   * Generate all combination of subarrays from a list of ranges per dimension That is if we have
   * the following ranges [[[1, 2], [10, 20]], [[100, 230]]] which translates to dim0 = [[1, 2],
   * [10, 20]] and dim1 = [100, 230]
   *
   * <p>This function produces the following output
   *
   * <p>[[[1, 2], [100, 230]], [[10, 20], [100, 230]]]
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

  public static boolean greaterThanOrEqual(Number a, Number b, Class dataClassType) {
    if (dataClassType == Byte.class) {
      return a.byteValue() >= b.byteValue();
    } else if (dataClassType == Short.class) {
      return a.shortValue() >= b.shortValue();
    } else if (dataClassType == Integer.class) {
      return a.intValue() >= b.intValue();
    } else if (dataClassType == Long.class) {
      return (Long) a >= (Long) b;
    } else if (dataClassType == Float.class) {
      return a.floatValue() >= b.floatValue();
    }

    // Else assume double
    return (Double) a >= (Double) b;
  }

  public static boolean lessThan(Number a, Number b, Class dataClassType) {
    if (dataClassType == Byte.class) {
      return a.byteValue() < b.byteValue();
    } else if (dataClassType == Short.class) {
      return a.shortValue() < b.shortValue();
    } else if (dataClassType == Integer.class) {
      return a.intValue() < b.intValue();
    } else if (dataClassType == Long.class) {
      return (Long) a < (Long) b;
    } else if (dataClassType == Float.class) {
      return a.floatValue() < b.floatValue();
    }

    return (Double) a < (Double) b;
  }

  /**
   * Finds the least multiple of x larger than n.
   *
   * @param n
   * @param x
   * @return
   */

  /**
   * Finds the least multiple of x that is greatest than or equal to n. For example, for n=55 and
   * x=16, the result will be 64.
   *
   * @param a The first number
   * @param b The second number
   * @param dataClassType The class of the input numbers
   * @return The least multiple
   */
  public static Object findClosestMultiple(Number a, Number b, Class dataClassType) {
    if (dataClassType == Byte.class) {
      return null;
    } else if (dataClassType == Short.class) {
      short initN = a.shortValue();
      short n = a.shortValue();
      short x = b.shortValue();

      if (x > n) return x;

      n = (short) (n + x / 2);
      n = (short) (n - (n % x));

      if (n < initN) return n + x;
      else return n;
    } else if (dataClassType == Integer.class) {
      int initN = (int) a;
      int n = (int) a;
      int x = (int) b;

      if (x > n) return x;

      n = (n + x / 2);
      n = (n - (n % x));

      if (n < initN) return n + x;
      else return n;
    } else if (dataClassType == Long.class) {
      long initN = (long) a;
      long n = (long) a;
      long x = (long) b;

      if (x > n) return x;

      n = (n + x / 2);
      n = (n - (n % x));

      if (n < initN) return n + x;
      else return n;
    } else if (dataClassType == Double.class) {
      double initN = (double) a;
      double n = (double) a;
      double x = (double) b;

      if (x > n) return x;

      n = (n + x / 2);
      n = (n - (n % x));

      if (n < initN) return n + x;
      else return n;
    } else if (dataClassType == Float.class) {
      float initN = (float) a;
      float n = (float) a;
      float x = (float) b;

      if (x > n) return x;

      n = (n + x / 2);
      n = (n - (n % x));

      if (n < initN) return n + x;
      else return n;
    }

    return null;
  }
}
