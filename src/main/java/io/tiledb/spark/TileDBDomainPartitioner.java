package io.tiledb.spark;

import io.tiledb.java.api.*;
import java.util.ArrayList;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.sources.Filter;

public class TileDBDomainPartitioner {

  private DomainDimRange[] nonEmptyDimRange;
  private ArrayList<Integer> partitions;
  private ArrayList<ArrayList<DomainDimRange>> pushDownDimRanges;

  public TileDBDomainPartitioner(DomainDimRange[] nonEmptyDomain) {
    this.nonEmptyDimRange = nonEmptyDomain;
    this.pushDownDimRanges = new ArrayList<>(nonEmptyDimRange.length);
    this.partitions = new ArrayList<>(nonEmptyDimRange.length);
    for (int i = 0; i < nonEmptyDimRange.length; i++) {
      this.pushDownDimRanges.add(new ArrayList<>());
      this.partitions.add(1);
    }
  }

  public TileDBDomainPartitioner setPushdownFilters(Filter[] filters) throws TileDBError {
    for (ArrayList dimRanges : pushDownDimRanges) {
      dimRanges.clear();
    }
    for (Filter f : filters) {
      addDomainDimRangeFromFilter(f);
    }
    return this;
  }

  public TileDBDomainPartitioner setDimPartitions(Integer idx, Integer nParitions) {
    partitions.set(idx, nParitions);
    return this;
  }

  public TileDBDomainPartitioner setDimPartitions(String name, Integer nPartitions) {
    return setDimPartitions(getDimIdx(name), nPartitions);
  }

  public DomainDimRange[] getDomainDimRanges(int partition) {
    ArrayList<DomainDimRange> ranges = new ArrayList<>();
    for (int i = 0; i < pushDownDimRanges.size(); i++) {
      if (pushDownDimRanges.get(i).size() == 0) {
        ranges.add(nonEmptyDimRange[i]);
      } else {
        ranges.addAll(pushDownDimRanges.get(i));
      }
    }
    return ranges.toArray(new DomainDimRange[ranges.size()]);
  }

  private int getDimIdx(String name) {
    for (DomainDimRange d : nonEmptyDimRange) {
      if (d.getName().equals(name)) {
        return d.getIdx();
      }
    }
    return -1;
  }

  private Object getNonEmptyDomainStart(int idx) {
    return nonEmptyDimRange[idx].getStart();
  }

  private Object getNonEmptyDomainEnd(int idx) {
    return nonEmptyDimRange[idx].getEnd();
  }

  private Datatype getDimDtype(int idx) {
    return nonEmptyDimRange[idx].getDatatype();
  }

  private void addDomainRange(Integer idx, DomainDimRange domainDimRange) {
    this.pushDownDimRanges.get(idx).add(domainDimRange);
  }

  private void addDomainDimRangeFromFilter(Filter filter) throws TileDBError {
    // First handle filter that are equal so `dim = 1`
    String name;
    Integer idx;
    Datatype dtype;
    Object start;
    Object end;
    if (filter instanceof EqualNullSafe) {
      EqualNullSafe f = (EqualNullSafe) filter;
      name = f.attribute();
      idx = getDimIdx(name);
      dtype = getDimDtype(idx);
      start = f.value();
      end = f.value();
      addDomainRange(idx, new DomainDimRange(name, idx, dtype, start, end));

    } else if (filter instanceof EqualTo) {
      EqualTo f = (EqualTo) filter;
      name = f.attribute();
      idx = getDimIdx(name);
      dtype = getDimDtype(idx);
      start = f.value();
      end = f.value();
      addDomainRange(idx, new DomainDimRange(name, idx, dtype, start, end));

      // GreaterThan is ranges which are in the form of `dim > 1`
    } else if (filter instanceof GreaterThan) {
      GreaterThan f = (GreaterThan) filter;
      name = f.attribute();
      idx = getDimIdx(name);
      dtype = getDimDtype(idx);
      start = DomainDimRange.addEpsilon(f.value(), dtype);
      end = getNonEmptyDomainEnd(idx);
      addDomainRange(idx, new DomainDimRange(name, idx, dtype, start, end));

      // GreaterThanOrEqual is ranges which are in the form of `dim >= 1`
    } else if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
      name = f.attribute();
      idx = getDimIdx(name);
      dtype = getDimDtype(idx);
      start = f.value();
      end = getNonEmptyDomainEnd(idx);
      addDomainRange(idx, new DomainDimRange(name, idx, dtype, start, end));

      // For in filters we will add every value as ranges of 1. `dim IN (1, 2, 3)`
    } else if (filter instanceof In) {
      In f = (In) filter;
      name = f.attribute();
      idx = getDimIdx(name);
      dtype = getDimDtype(idx);
      // Add every value as a new range, TileDB will collapse into super ranges for us
      for (Object value : f.values()) {
        addDomainRange(idx, new DomainDimRange(name, idx, dtype, value, value));
      }

      // LessThan is ranges which are in the form of `dim < 1`
    } else if (filter instanceof LessThan) {
      LessThan f = (LessThan) filter;
      name = f.attribute();
      idx = getDimIdx(name);
      dtype = getDimDtype(idx);
      start = getNonEmptyDomainStart(idx);
      end = DomainDimRange.subtractEpsilon(f.value(), dtype);
      addDomainRange(idx, new DomainDimRange(name, idx, dtype, start, end));

      // LessThanOrEqual is ranges which are in the form of `dim <= 1`
    } else if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual f = (LessThanOrEqual) filter;
      name = f.attribute();
      idx = getDimIdx(name);
      dtype = getDimDtype(idx);
      start = getNonEmptyDomainStart(idx);
      end = f.value();
      addDomainRange(idx, new DomainDimRange(name, idx, dtype, start, end));

    } else {
      throw new IllegalArgumentException("Unsupported domain pushdown filter: " + filter);
    }
  }
}
