package io.tiledb.spark;

import static io.tiledb.spark.util.addEpsilon;
import static io.tiledb.spark.util.generateAllSubarrays;
import static io.tiledb.spark.util.subtractEpsilon;
import static org.apache.log4j.Priority.ERROR;

import io.tiledb.java.api.Array;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.TileDBError;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.log4j.Logger;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Or;
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
      if (checkFilterIsDimensionOnly(filter)) {
        pushedFiltersList.add(filter);
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

  private boolean checkFilterIsDimensionOnly(Filter filter) {
    if (filter instanceof Or) {
      Or f = (Or) filter;
      if (checkFilterIsDimensionOnly(f.left()) && checkFilterIsDimensionOnly(f.right())) {
        return true;
      }
    } else if (filter instanceof EqualNullSafe) {
      EqualNullSafe f = (EqualNullSafe) filter;
      if (this.tileDBReadSchema.dimensionIndexes.containsKey(f.attribute())) {
        return true;
      }
    } else if (filter instanceof EqualTo) {
      EqualTo f = (EqualTo) filter;
      if (this.tileDBReadSchema.dimensionIndexes.containsKey(f.attribute())) {
        return true;
      }
    } else if (filter instanceof GreaterThan) {
      GreaterThan f = (GreaterThan) filter;
      if (this.tileDBReadSchema.dimensionIndexes.containsKey(f.attribute())) {
        return true;
      }
    } else if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
      if (this.tileDBReadSchema.dimensionIndexes.containsKey(f.attribute())) {
        return true;
      }
    } else if (filter instanceof In) {
      In f = (In) filter;
      if (this.tileDBReadSchema.dimensionIndexes.containsKey(f.attribute())) {
        return true;
      }
    } else if (filter instanceof LessThan) {
      LessThan f = (LessThan) filter;
      if (this.tileDBReadSchema.dimensionIndexes.containsKey(f.attribute())) {
        return true;
      }
    } else if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual f = (LessThanOrEqual) filter;
      if (this.tileDBReadSchema.dimensionIndexes.containsKey(f.attribute())) {
        return true;
      }
    }

    return false;
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
      List<List<Range>> ranges = new ArrayList<>();
      // Populate initial range list
      for (int i = 0; i < nonEmptyDomain.size(); i++) {
        ranges.add(new ArrayList<>());
      }

      for (Filter filter : pushedFilters) {
        List<List<Range>> dimRanges =
            buildRangeFromFilter(filter, this.tileDBReadSchema.domainType, nonEmptyDomain);

        for (int i = 0; i < dimRanges.size(); i++) {
          ranges.get(i).addAll(dimRanges.get(i));
        }
      }

      // Add nonEmptyDomain to any dimension that does not have a range from pushdown
      // For any existing ranges we try to merge into super ranges
      for (int i = 0; i < ranges.size(); i++) {
        List<Range> range = ranges.get(i);
        if (range.isEmpty()) {
          int finalI = i;
          String dimensionName =
              this.tileDBReadSchema
                  .dimensionIndexes
                  .entrySet()
                  .stream()
                  .filter(entry -> Objects.equals(entry.getValue(), finalI))
                  .map(Map.Entry::getKey)
                  .findFirst()
                  .get();
          range.add(new Range(nonEmptyDomain.get(dimensionName)));
        } else {
          List<Range> mergedRanges = checkAndMergeRanges(range);
          ranges.set(i, mergedRanges);
        }
      }

      List<SubArrayRanges> subarrays = new ArrayList<>();

      generateAllSubarrays(ranges, subarrays, 0, new ArrayList<>());

      int availablePartitions = tiledbOptions.getPartitionCount() - subarrays.size();
      if (availablePartitions > 1) {
        // Base case where we don't have any (or just single) pushdown per dimension
        if (subarrays.size() == 1) {
          // Split the single subarray into the available partitions
          subarrays = subarrays.get(0).split(availablePartitions);
        } else {
          // Sort subarrays based on volume so largest volume is first
          subarrays.sort(Collections.reverseOrder());
          // Find median volume of subarrays;

          SubArrayRanges medianSubarray = subarrays.get(subarrays.size() / 2);
          Number medianVolume = medianSubarray.getVolume();

          List<Integer> neededSplitsToReduceToMedianVolume =
              computeNeededSplitsToReduceToMedianVolume(
                  subarrays.subList(0, subarrays.size() / 2),
                  medianVolume,
                  medianSubarray.getDatatype());

          //        for (SubArrayRanges subArrayRanges : subarrays) {
          for (int i = 0; i < neededSplitsToReduceToMedianVolume.size(); i++) {
            SubArrayRanges subarray = subarrays.get(i);
            List<SubArrayRanges> splitSubarray =
                subarray.split(neededSplitsToReduceToMedianVolume.get(i));

            subarrays.remove(i);
            subarrays.addAll(splitSubarray);
          }
        }
      }

      // TODO: multiple subarray partitioning
      for (SubArrayRanges subarray : subarrays) {
        // In the future we will be smarter about combining ranges to have partitions work on more
        // than one range
        // I.E. don't over partition like we probably are doing now
        List<List<Range>> subarrayRanges = new ArrayList<>();
        subarrayRanges.add(subarray.getRanges());
        readerPartitions.add(
            new TileDBDataReaderPartition(uri, tileDBReadSchema, tiledbOptions, subarrayRanges));
      }
    } catch (TileDBError tileDBError) {
      log.log(ERROR, tileDBError.getMessage());
      return readerPartitions;
    }
    // foo
    return readerPartitions;
  }

  private List<Integer> computeNeededSplitsToReduceToMedianVolume(
      List<SubArrayRanges> subArrayRanges, Number medianVolume, Class datatype) {
    List<Integer> neededSplits = new ArrayList<>();
    for (SubArrayRanges subArrayRange : subArrayRanges) {
      Number volume = subArrayRange.getVolume();
      if (datatype == Byte.class) {
        neededSplits.add(volume.byteValue() / medianVolume.byteValue());
      } else if (datatype == Short.class) {
        neededSplits.add(volume.shortValue() / medianVolume.shortValue());
      } else if (datatype == Integer.class) {
        neededSplits.add(volume.intValue() / medianVolume.intValue());
      } else if (datatype == Long.class) {
        neededSplits.add(((Long) (volume.longValue() / medianVolume.longValue())).intValue());
      } else if (datatype == Float.class) {
        neededSplits.add(((Float) (volume.floatValue() / medianVolume.floatValue())).intValue());
      } else if (datatype == Double.class) {
        neededSplits.add(((Double) (volume.doubleValue() / medianVolume.doubleValue())).intValue());
      }
    }
    return neededSplits;
  }

  private List<Range> checkAndMergeRanges(List<Range> range) throws TileDBError {
    List<Range> rangesToBeMerged = new ArrayList<>(range);
    rangesToBeMerged.sort(
        new Comparator<Range>() {
          @Override
          public int compare(Range range, Range t1) {
            if (range.dataClassType() == Byte.class) {
              Pair<Byte, Byte> rangeByte = range.getRange();
              Pair<Byte, Byte> t1Byte = t1.getRange();
              return Byte.compare(rangeByte.getFirst(), t1Byte.getFirst());
            } else if (range.dataClassType() == Short.class) {
              Pair<Short, Short> rangeShort = range.getRange();
              Pair<Short, Short> t1Short = t1.getRange();
              return Short.compare(rangeShort.getFirst(), t1Short.getFirst());
            } else if (range.dataClassType() == Integer.class) {
              Pair<Integer, Integer> rangeInteger = range.getRange();
              Pair<Integer, Integer> t1Integer = t1.getRange();
              return Integer.compare(rangeInteger.getFirst(), t1Integer.getFirst());
            } else if (range.dataClassType() == Long.class) {
              Pair<Long, Long> rangeLong = range.getRange();
              Pair<Long, Long> t1Long = t1.getRange();
              return Long.compare(rangeLong.getFirst(), t1Long.getFirst());
            } else if (range.dataClassType() == Float.class) {
              Pair<Float, Float> rangeFloat = range.getRange();
              Pair<Float, Float> t1Float = t1.getRange();
              return Float.compare(rangeFloat.getFirst(), t1Float.getFirst());
            } else if (range.dataClassType() == Range.class) {
              Pair<Double, Double> rangeDouble = range.getRange();
              Pair<Double, Double> t1Double = t1.getRange();
              return Double.compare(rangeDouble.getFirst(), t1Double.getFirst());
            }

            return 0;
          }
        });

    boolean mergeable = true;
    while (mergeable) {
      List<Range> mergedRange = new ArrayList<>();
      for (int i = 0; i < rangesToBeMerged.size(); i++) {

        // If we are at the last range in the list it means the last_range - 1 and last_range were
        // not mergeable
        // OR it means we have a list of 1, either way the only thing to do is add this last range
        // to the list
        // and break
        if (i == rangesToBeMerged.size() - 1) {
          mergedRange.add(rangesToBeMerged.get(i));
          break;
        }

        Range left = rangesToBeMerged.get(i);
        Range right = rangesToBeMerged.get(i + 1);
        if (left.canMerge(right)) {
          mergedRange.add(left.merge(right));
          i++;
        } else {
          mergedRange.add(left);
        }
      }

      // If the merged ranges is the same size as the unmerged ranges it means there is was no
      // merges possible
      // and we have completed the merge process
      if (mergedRange.size() == rangesToBeMerged.size()) {
        mergeable = false;
      }

      // Override original ranges with new merged Ranges
      rangesToBeMerged = new ArrayList<>(mergedRange);
    }

    return rangesToBeMerged;
  }

  /**
   * Creates range(s) from a filter that has been pushed down
   *
   * @param filter
   * @param domainType
   * @param nonEmptyDomain
   * @throws TileDBError
   */
  private List<List<Range>> buildRangeFromFilter(
      Filter filter, Datatype domainType, HashMap<String, Pair> nonEmptyDomain) throws TileDBError {
    // Map<String, Integer> dimensionIndexing = new HashMap<>();
    List<List<Range>> ranges = new ArrayList<>();
    // Build mapping for dimension name to index
    for (int i = 0; i < this.tileDBReadSchema.dimensionIndexes.size(); i++) {
      ranges.add(new ArrayList<>());
    }
    // First handle filter that are equal so `dim = 1`
    if (filter instanceof Or) {
      List<List<Range>> left =
          buildRangeFromFilter(((Or) filter).left(), domainType, nonEmptyDomain);
      List<List<Range>> right =
          buildRangeFromFilter(((Or) filter).right(), domainType, nonEmptyDomain);
      for (int i = 0; i < left.size(); i++) {
        while (right.size() < i) {
          right.add(new ArrayList<>());
        }

        right.get(i).addAll(left.get(i));
      }

      return right;
    } else if (filter instanceof EqualNullSafe) {
      EqualNullSafe f = (EqualNullSafe) filter;
      int dimIndex = this.tileDBReadSchema.dimensionIndexes.get(f.attribute());
      ranges.get(dimIndex).add(new Range(new Pair<>(f.value(), f.value())));
    } else if (filter instanceof EqualTo) {
      EqualTo f = (EqualTo) filter;
      int dimIndex = this.tileDBReadSchema.dimensionIndexes.get(f.attribute());
      ranges.get(dimIndex).add(new Range(new Pair<>(f.value(), f.value())));

      // GreaterThan is ranges which are in the form of `dim > 1`
    } else if (filter instanceof GreaterThan) {
      GreaterThan f = (GreaterThan) filter;
      int dimIndex = this.tileDBReadSchema.dimensionIndexes.get(f.attribute());
      ranges
          .get(dimIndex)
          .add(
              new Range(
                  new Pair<>(
                      addEpsilon(f.value(), domainType),
                      nonEmptyDomain.get(f.attribute()).getSecond())));
      // GreaterThanOrEqual is ranges which are in the form of `dim >= 1`
    } else if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
      int dimIndex = this.tileDBReadSchema.dimensionIndexes.get(f.attribute());
      ranges
          .get(dimIndex)
          .add(new Range(new Pair<>(f.value(), nonEmptyDomain.get(f.attribute()).getSecond())));

      // For in filters we will add every value as ranges of 1. `dim IN (1, 2, 3)`
    } else if (filter instanceof In) {
      In f = (In) filter;
      // Add every value as a new range, TileDB will collapse into super ranges for us
      for (Object value : f.values()) {
        int dimIndex = this.tileDBReadSchema.dimensionIndexes.get(f.attribute());
        ranges.get(dimIndex).add(new Range(new Pair<>(value, value)));
      }

      // LessThan is ranges which are in the form of `dim < 1`
    } else if (filter instanceof LessThan) {
      LessThan f = (LessThan) filter;
      int dimIndex = this.tileDBReadSchema.dimensionIndexes.get(f.attribute());
      ranges
          .get(dimIndex)
          .add(
              new Range(
                  new Pair<>(
                      nonEmptyDomain.get(f.attribute()).getFirst(),
                      subtractEpsilon(f.value(), domainType))));
      // LessThanOrEqual is ranges which are in the form of `dim <= 1`
    } else if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual f = (LessThanOrEqual) filter;
      int dimIndex = this.tileDBReadSchema.dimensionIndexes.get(f.attribute());
      ranges
          .get(dimIndex)
          .add(new Range(new Pair<>(nonEmptyDomain.get(f.attribute()).getFirst(), f.value())));
    } else {
      throw new TileDBError("Unsupported filter type");
    }
    return ranges;
  }
}
