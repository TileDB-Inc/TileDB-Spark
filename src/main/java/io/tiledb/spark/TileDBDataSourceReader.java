package io.tiledb.spark;

import static org.apache.log4j.Priority.ERROR;

import io.tiledb.java.api.Array;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.TileDBError;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class TileDBDataSourceReader
    implements DataSourceReader,
        SupportsPushDownRequiredColumns,
        SupportsScanColumnarBatch,
        SupportsPushDownFilters {

  static Logger log = Logger.getLogger(TileDBDataSourceReader.class.getName());

  private URI uri;
  private TileDBReadSchema tileDBReadSchema;
  private TileDBDataSourceOptions tiledbOptions;
  private Filter[] pushedFilters;

  public TileDBDataSourceReader(URI uri, TileDBDataSourceOptions options) {
    this.uri = uri;
    this.tiledbOptions = options;
    this.tileDBReadSchema = new TileDBReadSchema(uri, options);
  }

  @Override
  public StructType readSchema() {
    log.trace("Reading schema for " + uri);
    StructType schema = tileDBReadSchema.getSparkSchema();
    log.trace("Read schema for " + uri + ": " + schema);
    return schema;
  }

  @Override
  public void pruneColumns(StructType pushDownSchema) {
    log.trace("Set pushdown columns for " + uri + ": " + pushDownSchema);
    tileDBReadSchema.setPushDownSchema(pushDownSchema);
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    log.info("size of filters " + filters.length);
    ArrayList<Filter> pushedFiltersList = new ArrayList<>();
    ArrayList<Filter> leftOverFilters = new ArrayList<>();

    // Loop through all filters and check if they are support type and on a domain. If so push them
    // down
    for (Filter filter : filters) {
      if (filter instanceof EqualNullSafe) {
        EqualNullSafe f = (EqualNullSafe) filter;
        if (this.tileDBReadSchema.dimensionIndexes.containsKey(f.attribute())) {
          pushedFiltersList.add(filter);
        }
      } else if (filter instanceof EqualTo) {
        EqualTo f = (EqualTo) filter;
        if (this.tileDBReadSchema.dimensionIndexes.containsKey(f.attribute())) {
          pushedFiltersList.add(filter);
        }
      } else if (filter instanceof GreaterThan) {
        GreaterThan f = (GreaterThan) filter;
        if (this.tileDBReadSchema.dimensionIndexes.containsKey(f.attribute())) {
          pushedFiltersList.add(filter);
        }
      } else if (filter instanceof GreaterThanOrEqual) {
        GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
        if (this.tileDBReadSchema.dimensionIndexes.containsKey(f.attribute())) {
          pushedFiltersList.add(filter);
        }
      } else if (filter instanceof In) {
        In f = (In) filter;
        if (this.tileDBReadSchema.dimensionIndexes.containsKey(f.attribute())) {
          pushedFiltersList.add(filter);
        }
      } else if (filter instanceof LessThan) {
        LessThan f = (LessThan) filter;
        if (this.tileDBReadSchema.dimensionIndexes.containsKey(f.attribute())) {
          pushedFiltersList.add(filter);
        }
      } else if (filter instanceof LessThanOrEqual) {
        LessThanOrEqual f = (LessThanOrEqual) filter;
        if (this.tileDBReadSchema.dimensionIndexes.containsKey(f.attribute())) {
          pushedFiltersList.add(filter);
        }
      } else {
        leftOverFilters.add(filter);
      }
    }

    this.pushedFilters = new Filter[pushedFiltersList.size()];
    this.pushedFilters = pushedFiltersList.toArray(this.pushedFilters);

    Filter[] leftOvers = new Filter[leftOverFilters.size()];
    leftOvers = leftOverFilters.toArray(leftOvers);
    return leftOvers;
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public boolean enableBatchRead() {
    // always read in batch mode
    return true;
  }

  @Override
  public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {
    ArrayList<InputPartition<ColumnarBatch>> readerPartitions = new ArrayList<>();

    try (Context ctx = new Context(tiledbOptions.getTileDBConfigMap());
        // fetch and load the array to get nonEmptyDomain
        Array array = new Array(ctx, uri.toString()); ) {
      HashMap<String, Pair> nonEmptyDomain = array.nonEmptyDomain();
      List<List<Pair>> ranges = new ArrayList<>();
      for (Filter filter : pushedFilters) {
        List<List<Pair>> dimRanges =
            buildRangeFromFilter(filter, this.tileDBReadSchema.domainType, nonEmptyDomain);

        for (int i = 0; i < dimRanges.size(); i++) {
          while (ranges.size() < i + 1) {
            ranges.add(new ArrayList<>());
          }
          ranges.get(i).addAll(dimRanges.get(i));
        }
      }

      // TODO: multiple subarray partitioning
      readerPartitions.add(
          new TileDBDataReaderPartition(uri, tileDBReadSchema, tiledbOptions, ranges));
    } catch (TileDBError tileDBError) {
      log.log(ERROR, tileDBError.getMessage());
      return readerPartitions;
    }
    // foo
    return readerPartitions;
  }

  /**
   * Creates range(s) from a filter that has been pushed down
   *
   * @param filter
   * @param domainType
   * @param nonEmptyDomain
   * @throws TileDBError
   */
  private List<List<Pair>> buildRangeFromFilter(
      Filter filter, Datatype domainType, HashMap<String, Pair> nonEmptyDomain) throws TileDBError {
    // Map<String, Integer> dimensionIndexing = new HashMap<>();
    List<List<Pair>> ranges = new ArrayList<>();
    // Build mapping for dimension name to index
    for (int i = 0; i < this.tileDBReadSchema.dimensionIndexes.size(); i++) {
      ranges.add(new ArrayList<>());
    }
    // First handle filter that are equal so `dim = 1`
    if (filter instanceof EqualNullSafe) {
      EqualNullSafe f = (EqualNullSafe) filter;
      int dimIndex = this.tileDBReadSchema.dimensionIndexes.get(f.attribute());
      ranges.get(dimIndex).add(new Pair<>(f.value(), f.value()));
    } else if (filter instanceof EqualTo) {
      EqualTo f = (EqualTo) filter;
      int dimIndex = this.tileDBReadSchema.dimensionIndexes.get(f.attribute());
      ranges.get(dimIndex).add(new Pair<>(f.value(), f.value()));

      // GreaterThan is ranges which are in the form of `dim > 1`
    } else if (filter instanceof GreaterThan) {
      GreaterThan f = (GreaterThan) filter;
      int dimIndex = this.tileDBReadSchema.dimensionIndexes.get(f.attribute());
      ranges
          .get(dimIndex)
          .add(
              new Pair<>(
                  addEpsilon(f.value(), domainType),
                  nonEmptyDomain.get(f.attribute()).getSecond()));
      // GreaterThanOrEqual is ranges which are in the form of `dim >= 1`
    } else if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
      int dimIndex = this.tileDBReadSchema.dimensionIndexes.get(f.attribute());
      ranges
          .get(dimIndex)
          .add(new Pair<>(f.value(), nonEmptyDomain.get(f.attribute()).getSecond()));

      // For in filters we will add every value as ranges of 1. `dim IN (1, 2, 3)`
    } else if (filter instanceof In) {
      In f = (In) filter;
      // Add every value as a new range, TileDB will collapse into super ranges for us
      for (Object value : f.values()) {
        int dimIndex = this.tileDBReadSchema.dimensionIndexes.get(f.attribute());
        ranges.get(dimIndex).add(new Pair<>(value, value));
      }

      // LessThanl is ranges which are in the form of `dim < 1`
    } else if (filter instanceof LessThan) {
      LessThan f = (LessThan) filter;
      int dimIndex = this.tileDBReadSchema.dimensionIndexes.get(f.attribute());
      ranges
          .get(dimIndex)
          .add(
              new Pair<>(
                  nonEmptyDomain.get(f.attribute()).getFirst(),
                  subtractEpsilon(f.value(), domainType)));
      // LessThanOrEqual is ranges which are in the form of `dim <= 1`
    } else if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual f = (LessThanOrEqual) filter;
      int dimIndex = this.tileDBReadSchema.dimensionIndexes.get(f.attribute());
      ranges.get(dimIndex).add(new Pair<>(nonEmptyDomain.get(f.attribute()).getFirst(), f.value()));
    } else {
      throw new TileDBError("Unsupported filter type");
    }
    return ranges;
  }

  /** Returns v + eps, where eps is the smallest value for the datatype such that v + eps > v. */
  private static Object addEpsilon(Object value, Datatype type) throws TileDBError {
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

  /** Returns v - eps, where eps is the smallest value for the datatype such that v - eps < v. */
  private static Object subtractEpsilon(Object value, Datatype type) throws TileDBError {
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
}
