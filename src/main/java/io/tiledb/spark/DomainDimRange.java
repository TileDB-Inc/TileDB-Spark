package io.tiledb.spark;

import io.tiledb.java.api.Datatype;
import java.io.Serializable;

public class DomainDimRange implements Comparable<DomainDimRange>, Serializable {

  private final String name;
  private final Integer idx;
  private final Datatype dtype;
  private final Object start;
  private final Object end;

  public DomainDimRange(String name, Integer idx, Datatype dtype, Object start, Object end) {
    this.name = name;
    this.idx = idx;
    this.dtype = dtype;
    this.start = start;
    this.end = end;
  }

  public String getName() {
    return name;
  }

  public Integer getIdx() {
    return idx;
  }

  public Datatype getDatatype() {
    return dtype;
  }

  public Object getStart() {
    return start;
  }

  public Object getEnd() {
    return end;
  }

  protected boolean isUnaryRange() {
    switch (dtype) {
      case TILEDB_CHAR:
      case TILEDB_INT8:
        return ((byte) start == (byte) end);
      case TILEDB_UINT8:
      case TILEDB_INT16:
        return ((short) start == (short) end);
      case TILEDB_UINT16:
      case TILEDB_INT32:
        return ((int) start == (int) end);
      case TILEDB_UINT32:
      case TILEDB_INT64:
      case TILEDB_UINT64:
        return ((long) start == (long) end);
      case TILEDB_FLOAT32:
        return ((float) start == (float) end);
      case TILEDB_FLOAT64:
        return ((double) start == (double) end);
      default:
        throw new RuntimeException("Unknown domain type comparsion");
    }
  }

  protected DomainDimRange[] splitRange(Object val) {
    if (!inRange(val)) {
      return new DomainDimRange[] {};
    }
    if (isUnaryRange()) {
      return new DomainDimRange[] {new DomainDimRange(name, idx, dtype, start, end)};
    }
    return new DomainDimRange[] {
      new DomainDimRange(name, idx, dtype, start, val),
      new DomainDimRange(name, idx, dtype, addEpsilon(val, dtype), end)
    };
  }

  protected boolean inRange(Object val) {
    switch (dtype) {
      case TILEDB_CHAR:
      case TILEDB_INT8:
        return ((byte) val >= (byte) start) && ((byte) val <= (byte) end);
      case TILEDB_UINT8:
      case TILEDB_INT16:
        return ((short) val >= (short) start) && ((short) val <= (short) end);
      case TILEDB_UINT16:
      case TILEDB_INT32:
        return ((int) val >= (int) start) && ((int) val <= (int) end);
      case TILEDB_UINT32:
      case TILEDB_INT64:
      case TILEDB_UINT64:
        return ((long) val >= (long) start) && ((long) val <= (long) end);
      case TILEDB_FLOAT32:
        return ((float) val >= (float) start) && ((float) val <= (float) end);
      case TILEDB_FLOAT64:
        return ((double) val >= (double) start) && ((double) val <= (double) end);
      default:
        throw new RuntimeException("Unknown domain type comparsion");
    }
  }

  protected Object getSpan() {
    switch (dtype) {
      case TILEDB_CHAR:
      case TILEDB_INT8:
        return Math.subtractExact((byte) end, (byte) start);
      case TILEDB_UINT8:
      case TILEDB_INT16:
        return Math.subtractExact((short) end, (short) start);
      case TILEDB_UINT16:
      case TILEDB_INT32:
        return Math.subtractExact((int) end, (int) start);
      case TILEDB_UINT32:
      case TILEDB_INT64:
      case TILEDB_UINT64:
        return Math.subtractExact((long) end, (long) start);
      case TILEDB_FLOAT32:
        return (((float) end) - ((float) start));
      case TILEDB_FLOAT64:
        return (((double) end) - ((double) start));
      default:
        throw new RuntimeException("Unknown domain type comparsion");
    }
  }

  @Override
  public int compareTo(DomainDimRange otherRange) {
    if (name != otherRange.getName()) {
      return name.compareTo(otherRange.getName());
    }
    if (idx != otherRange.getIdx()) {
      return idx.compareTo(otherRange.getIdx());
    }
    assert dtype == otherRange.getDatatype();
    int comp = compareToDtype(getStart(), otherRange.getEnd());
    if (comp == 0) {
      comp = compareToDtype(getEnd(), otherRange.getEnd());
    }
    return comp;
  }

  private int compareToDtype(Object val, Object otherVal) {
    switch (dtype) {
      case TILEDB_CHAR:
      case TILEDB_INT8:
        return ((Byte) val).compareTo((Byte) otherVal);
      case TILEDB_UINT8:
      case TILEDB_INT16:
        return ((Short) val).compareTo((Short) otherVal);
      case TILEDB_UINT16:
      case TILEDB_INT32:
        return ((Integer) val).compareTo((Integer) otherVal);
      case TILEDB_UINT32:
      case TILEDB_INT64:
      case TILEDB_UINT64:
        return ((Long) val).compareTo((Long) otherVal);
      case TILEDB_FLOAT32:
        return ((Float) val).compareTo((Float) otherVal);
      case TILEDB_FLOAT64:
        return ((Double) val).compareTo((Double) otherVal);
      default:
        throw new RuntimeException("Unknown domain type comparsion");
    }
  }

  /** Returns v + eps, where eps is the smallest value for the datatype such that v + eps > v. */
  protected static Object addEpsilon(Object value, Datatype type) {
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
      case TILEDB_UINT64:
        return ((long) value) < ((long) Integer.MAX_VALUE + 1) ? ((long) value + 1) : value;
      case TILEDB_FLOAT32:
        return ((float) value) < Float.MAX_VALUE ? Math.nextUp((float) value) : value;
      case TILEDB_FLOAT64:
        return ((double) value) < Double.MAX_VALUE ? Math.nextUp((double) value) : value;
      default:
        throw new RuntimeException("Unsupported TileDB Datatype enum: " + type);
    }
  }

  /** Returns v - eps, where eps is the smallest value for the datatype such that v - eps < v. */
  protected static Object subtractEpsilon(Object value, Datatype type) {
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
      case TILEDB_UINT64:
        return ((long) value) > ((long) Integer.MIN_VALUE - 1) ? ((long) value - 1) : value;
      case TILEDB_FLOAT32:
        return ((float) value) > Float.MIN_VALUE ? Math.nextDown((float) value) : value;
      case TILEDB_FLOAT64:
        return ((double) value) > Double.MIN_VALUE ? Math.nextDown((double) value) : value;
      default:
        throw new RuntimeException("Unsupported TileDB Datatype enum: " + type);
    }
  }
}
