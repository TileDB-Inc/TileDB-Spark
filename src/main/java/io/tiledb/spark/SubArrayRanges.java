package io.tiledb.spark;

import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

public class SubArrayRanges implements Comparable<SubArrayRanges> {
  private List<Range> ranges;
  private Class datatype;

  SubArrayRanges(List<Range> ranges, Class datatype) {
    this.ranges = ranges;
    this.datatype = datatype;
  }

  public Class getDatatype() {
    return datatype;
  }

  public List<Range> getRanges() {
    return ranges;
  }

  /**
   * Finds the dimension with the largest width
   *
   * @return index of dimension
   */
  public int getDimensionWithLargestWidth() {
    if (datatype == Byte.class) {
      return IntStream.range(0, ranges.size())
          .boxed()
          .max(Comparator.comparing(e -> ranges.get(e).width().byteValue()))
          .get();
      //      ranges.stream()
      //              .max( Comparator.comparing( e -> e.width().byteValue()))
      //              .get();
    } else if (datatype == Short.class) {
      return IntStream.range(0, ranges.size())
          .boxed()
          .max(Comparator.comparing(e -> ranges.get(e).width().shortValue()))
          .get();
    } else if (datatype == Integer.class) {
      return IntStream.range(0, ranges.size())
          .boxed()
          .max(Comparator.comparing(e -> ranges.get(e).width().intValue()))
          .get();
    } else if (datatype == Long.class) {
      return IntStream.range(0, ranges.size())
          .boxed()
          .max(Comparator.comparing(e -> ranges.get(e).width().longValue()))
          .get();
    } else if (datatype == Float.class) {
      return IntStream.range(0, ranges.size())
          .boxed()
          .max(Comparator.comparing(e -> ranges.get(e).width().floatValue()))
          .get();
    } else if (datatype == Double.class) {
      return IntStream.range(0, ranges.size())
          .boxed()
          .max(Comparator.comparing(e -> ranges.get(e).width().doubleValue()))
          .get();
    }

    return 0;
  }

  public <T extends Number> T getVolume() {
    if (datatype == Byte.class) {
      return (T) volumeByte();
    } else if (datatype == Short.class) {
      return (T) volumeShort();
    } else if (datatype == Integer.class) {
      return (T) volumeInteger();
    } else if (datatype == Long.class) {
      return (T) volumeLong();
    } else if (datatype == Float.class) {
      return (T) volumeFloat();
    } else if (datatype == Double.class) {
      return (T) volumeDouble();
    }

    return (T) (Integer) (0);
  }

  private Byte volumeByte() {
    byte volume = 1;
    for (Range dimRange : ranges) {
      volume *= dimRange.width().byteValue();
    }
    return volume;
  }

  private Short volumeShort() {
    short volume = 1;
    for (Range dimRange : ranges) {
      volume *= dimRange.width().shortValue();
    }
    return volume;
  }

  private Integer volumeInteger() {
    int volume = 1;
    for (Range dimRange : ranges) {
      volume *= dimRange.width().intValue();
    }
    return volume;
  }

  private Long volumeLong() {
    long volume = 1;
    for (Range dimRange : ranges) {
      volume *= dimRange.width().longValue();
    }
    return volume;
  }

  private Float volumeFloat() {
    float volume = 1;
    for (Range dimRange : ranges) {
      volume *= dimRange.width().floatValue();
    }
    return volume;
  }

  private Double volumeDouble() {
    double volume = 1;
    for (Range dimRange : ranges) {
      volume *= dimRange.width().doubleValue();
    }
    return volume;
  }

  /*private Byte volumeByte() {
    byte volume = 1;
    for (List<Range> dimRange : ranges) {
      byte width = 0;
      for (Range range : dimRange) {
        width += range.width().byteValue();
      }
      volume *= width;
    }
    return volume;
  }

  private Short volumeShort() {
    short volume = 1;
    for (List<Range> dimRange : ranges) {
      short width = 0;
      for (Range range : dimRange) {
        width += range.width().shortValue();
      }

      volume *= width;
    }
    return volume;
  }

  private Integer volumeInteger() {
    int volume = 1;
    for (List<Range> dimRange : ranges) {
      int width = 0;
      for (Range range : dimRange) {
        width += range.width().intValue();
      }
      volume *= width;
    }
    return volume;
  }

  private Long volumeLong() {
    long volume = 1;
    for (List<Range> dimRange : ranges) {
      long width = 0;
      for (Range range : dimRange) {
        width += range.width().longValue();
      }
      volume *= width;
    }
    return volume;
  }

  private Float volumeFloat() {
    float volume = 1;
    for (List<Range> dimRange : ranges) {
      float width = 0;
      for (Range range : dimRange) {
        width += range.width().floatValue();
      }
      volume *= width;
    }
    return volume;
  }

  private Double volumeDouble() {
    double volume = 1;
    for (List<Range> dimRange : ranges) {
      double width = 0;
      for (Range range : dimRange) {
        width += range.width().doubleValue();
      }
      volume *= width;
    }
    return volume;
  }*/

  @Override
  public int compareTo(SubArrayRanges other) {
    // compareTo should return < 0 if this is supposed to be
    // less than other, > 0 if this is supposed to be greater than
    // other and 0 if they are supposed to be equal
    if (datatype == Byte.class) {
      return volumeByte().compareTo(other.volumeByte());
    } else if (datatype == Short.class) {
      return volumeShort().compareTo(other.volumeShort());
    } else if (datatype == Integer.class) {
      return volumeInteger().compareTo(other.volumeInteger());
    } else if (datatype == Long.class) {
      return volumeLong().compareTo(other.volumeLong());
    } else if (datatype == Float.class) {
      return volumeFloat().compareTo(other.volumeFloat());
    } else if (datatype == Double.class) {
      return volumeDouble().compareTo(other.volumeDouble());
    }

    return 0;
  }
}
