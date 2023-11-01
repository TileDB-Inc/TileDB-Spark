package io.tiledb.spark;

import static io.tiledb.libtiledb.tiledb_query_condition_combination_op_t.TILEDB_AND;
import static io.tiledb.libtiledb.tiledb_query_condition_combination_op_t.TILEDB_OR;
import static io.tiledb.libtiledb.tiledb_query_condition_op_t.*;
import static io.tiledb.spark.util.addEpsilon;
import static io.tiledb.spark.util.generateAllSubarrays;
import static io.tiledb.spark.util.subtractEpsilon;
import static org.apache.spark.metrics.TileDBMetricsSource.dataSourceBuildRangeFromFilterTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.dataSourceCheckAndMergeRangesTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.dataSourceComputeNeededSplitsToReduceToMedianVolumeTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.dataSourcePlanBatchInputPartitionsTimerName;

import io.tiledb.java.api.*;
import io.tiledb.libtiledb.tiledb_query_condition_op_t;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.IntStream;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.metrics.TileDBReadMetricsUpdater;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Or;

public class TileDBBatch implements Batch {
  private final TileDBReadSchema tileDBReadSchema;
  private final TileDBDataSourceOptions tileDBDataSourceOptions;
  private final TileDBReadMetricsUpdater metricsUpdater;
  private final Filter[] pushedFilters;
  private final Context ctx;

  private Array array;

  private ArraySchema arraySchema;

  private Domain domain;

  static Logger log = Logger.getLogger(TileDBBatch.class.getName());

  /**
   * This is the master query condition which is calculated after combining all filters based on
   * their priority.
   */
  public static QueryCondition finalQueryCondition;

  public TileDBBatch(
      TileDBReadSchema tileDBReadSchema, TileDBDataSourceOptions options, Filter[] pushedFilters)
      throws TileDBError, URISyntaxException {
    this.tileDBReadSchema = tileDBReadSchema;
    this.tileDBDataSourceOptions = options;
    this.metricsUpdater = new TileDBReadMetricsUpdater(TaskContext.get());
    this.pushedFilters = pushedFilters;
    ctx = new Context(tileDBDataSourceOptions.getTileDBConfigMap(true));
    array = new Array(ctx, options.getArrayURI().get(), QueryType.TILEDB_READ);
    arraySchema = array.getSchema();
    domain = arraySchema.getDomain();
    finalQueryCondition = null;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    metricsUpdater.startTimer(dataSourcePlanBatchInputPartitionsTimerName);
    ArrayList<InputPartition> readerPartitions = new ArrayList<>();

    try {
      // Fetch the array and load its metadata
      Array array = new Array(ctx, util.tryGetArrayURI(tileDBDataSourceOptions));
      HashMap<String, Pair> nonEmptyDomain = array.nonEmptyDomain();

      List<List<Range>> ranges = new ArrayList<>();
      // Populate initial range list
      for (int i = 0; i < domain.getNDim(); i++) {
        ranges.add(new ArrayList<>());
      }

      for (int i = 0; i < arraySchema.getAttributeNum(); i++) {
        ranges.add(new ArrayList<>());
      }

      // Build range from all pushed filters
      if (pushedFilters != null) {
        for (Filter filter : pushedFilters) {
          // apply filter and return the ranges and the corresponding Query Condition
          Pair<Pair<List<List<Range>>, Class>, QueryCondition> appliedConditions =
              buildRangeFromFilter(filter, nonEmptyDomain);
          List<List<Range>> allRanges = appliedConditions.getFirst().getFirst();

          // grab the Query Condition
          QueryCondition currentQueryCondition = appliedConditions.getSecond();

          // Combine the query condition from this filter with all previous Query Conditions. In
          // this case the new condition will be combined with TILEDB_AND. This is because Spark can
          // sometimes parse an AND (e.g. a1 > 5 AND a2 > 10) filter as two separate filters instead
          // of a single AND filter. In the case of ORs Spark always returns one filter, the OR
          // filter. Thus, when there are two filters we are certain that they should be combined
          // with an AND.
          if (finalQueryCondition == null) finalQueryCondition = currentQueryCondition;
          else if (currentQueryCondition != null) {
            finalQueryCondition = currentQueryCondition.combine(finalQueryCondition, TILEDB_AND);
            currentQueryCondition.close();
          }

          for (int i = 0; i < allRanges.size(); i++) {
            ranges.get(i).addAll(allRanges.get(i));
          }
        }
      }

      // Add nonEmptyDomain to any dimension that does not have a range from pushdown
      // For any existing ranges we try to merge into super ranges
      for (int i = 0; i < domain.getNDim() + arraySchema.getAttributeNum(); i++) {
        List<Range> range = ranges.get(i);
        if (range.isEmpty()) {
          String columnName = this.tileDBReadSchema.getColumnName(i).get();
          if (this.tileDBReadSchema.hasDimension(columnName)) {
            range.add(new Range(nonEmptyDomain.get(columnName)));
          } else {
            range.add(new Range(true, new Pair(null, null)));
          }
        } else {
          List<Range> mergedRanges = checkAndMergeRanges(range);
          ranges.set(i, mergedRanges);
        }
      }

      List<SubArrayRanges> subarrays = new ArrayList<>();

      generateAllSubarrays(
          ranges.subList(0, (int) (domain.getNDim())), subarrays, 0, new ArrayList<>());

      int availablePartitions = tileDBDataSourceOptions.getPartitionCount();
      if (availablePartitions > 1) {
        // Base case where we don't have any (or just single) pushdown per dimension
        if (subarrays.size() == 1 && subarrays.get(0).splittable()) {
          subarrays = subarrays.get(0).splitToPartitions(availablePartitions);
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

          int sumOfNeededSplitsForEvenDistributed =
              neededSplitsToReduceToMedianVolume.stream().mapToInt(Integer::intValue).sum();

          for (int i = 0; i < neededSplitsToReduceToMedianVolume.size(); i++) {
            SubArrayRanges subarray = subarrays.get(i);

            // Don't try to split unsplittable subarrays
            if (!subarray.splittable()) {
              continue;
            }

            /*
             Imprecision with double division don't matter here we just want close enough percentages;

             The weights are computed based on the percentage of needed splits to reduce to the median.
             For example if we have 5 subarrays each with volumes of 10, 15, 50, 200, 400
             The median is 50
             The number of splits to reduce to the median is 400 / 50 = 8 AND 200 / 50 = 4
             The weights are computed to be
             8 / (8+4) = 0.66 and 4 / (8+4) = 0.33 for subarrays 400 and 200 respectively.
             If the number of available splits is 3 (thus we can not give the full 8 + 4 splits needed for the median)
             We do 0.66 * 3 = 2 splits to the 400 and 3 * 0.33 = 1 splits to the 200 subarray.
             This results in a final subarray volumes of 10, 15, 50, 100, 100, 100, 100, 100, 100
            */
            int numberOfWeightedSplits =
                (int)
                    Math.ceil(
                        neededSplitsToReduceToMedianVolume.get(i).doubleValue()
                            / sumOfNeededSplitsForEvenDistributed
                            * availablePartitions);
            List<SubArrayRanges> splitSubarray = subarray.split(numberOfWeightedSplits);

            subarrays.remove(i);
            subarrays.addAll(splitSubarray);
          }
        }
      }

      List<List<Range>> attributeRanges = new ArrayList<>();
      for (int i = ((int) domain.getNDim());
          i < domain.getNDim() + arraySchema.getAttributeNum();
          i++) {
        attributeRanges.add(ranges.get(i));
      } // TODO this is not needed after the QC changes from Dimitris. Will be removed in a future
      // PR

      for (SubArrayRanges subarray : subarrays) {
        // In the future we will be smarter about combining ranges to have partitions work on more
        // than one range
        // I.E. don't over partition like we probably are doing now
        List<List<Range>> subarrayRanges = new ArrayList<>();
        subarrayRanges.add(subarray.getRanges());
        readerPartitions.add(
            new TileDBDataInputPartition(
                util.tryGetArrayURI(tileDBDataSourceOptions),
                tileDBReadSchema,
                tileDBDataSourceOptions,
                subarrayRanges,
                attributeRanges));
      }
      metricsUpdater.finish(dataSourcePlanBatchInputPartitionsTimerName);
      InputPartition[] partitionsArray = new InputPartition[readerPartitions.size()];
      partitionsArray = readerPartitions.toArray(partitionsArray);
      array.close();
      return partitionsArray;
    } catch (Exception e) {
      e.printStackTrace();
      metricsUpdater.finish(dataSourcePlanBatchInputPartitionsTimerName);
    }
    return null;
  }

  /**
   * Creates range(s) from a filter that has been pushed down
   *
   * @param filter
   * @param nonEmptyDomain
   * @throws TileDBError
   */
  private Pair<Pair<List<List<Range>>, Class>, QueryCondition> buildRangeFromFilter(
      Filter filter, HashMap<String, Pair> nonEmptyDomain) throws TileDBError {
    metricsUpdater.startTimer(dataSourceBuildRangeFromFilterTimerName);
    String[] filterReferences = filter.references();
    QueryCondition finalQc = null; // the query condition created by each filter
    Class filterType = filter.getClass();
    // Map<String, Integer> dimensionIndexing = new HashMap<>();
    List<List<Range>> ranges = new ArrayList<>();
    // Build mapping for dimension name to index
    for (int i = 0; i < this.tileDBReadSchema.dimensionIndex.size(); i++) {
      ranges.add(new ArrayList<>());
    }

    for (int i = 0; i < this.tileDBReadSchema.attributeIndex.size(); i++) {
      ranges.add(new ArrayList<>());
    }
    // First handle filter that are AND this is something like dim1 >= 1 AND dim1 <= 10
    // Could also be dim1 between 1 and 10
    if (filter instanceof And) {
      Pair<Pair<List<List<Range>>, Class>, QueryCondition> left =
          buildRangeFromFilter(((And) filter).left(), nonEmptyDomain);
      Pair<Pair<List<List<Range>>, Class>, QueryCondition> right =
          buildRangeFromFilter(((And) filter).right(), nonEmptyDomain);

      // Get the Query Conditions that consist each branch of the expression. If they are not null
      // (they should never be null) combine them and create a new Query Condition to return.
      QueryCondition leftQc = left.getSecond();
      QueryCondition rightQc = right.getSecond();
      if (leftQc != null && rightQc != null) {
        finalQc = leftQc.combine(rightQc, TILEDB_AND);
        // close unneeded query conditions
        leftQc.close();
        rightQc.close();
      }

      // Get the index from the list of ranges to see where is the range for the left part of the
      // expression
      int leftIndex =
          IntStream.range(0, left.getFirst().getFirst().size())
              .filter(e -> left.getFirst().getFirst().get(e).size() > 0)
              .findFirst()
              .getAsInt();

      // Get the index from the list of ranges to see where is the range for the right part of the
      // expression
      int rightIndex =
          IntStream.range(0, right.getFirst().getFirst().size())
              .filter(e -> right.getFirst().getFirst().get(e).size() > 0)
              .findFirst()
              .getAsInt();

      // Create return constructed ranges
      List<List<Range>> constructedRanges = new ArrayList<>();
      for (int i = 0;
          i < Math.max(left.getFirst().getFirst().size(), right.getFirst().getFirst().size());
          i++) constructedRanges.add(new ArrayList<>());

      // Switch on the left side to see if it is the greater than or less than clause and set
      // appropriate position
      Pair<Object, Object> newPair = new Pair<>(null, null);
      if (left.getFirst().getSecond() == GreaterThan.class
          || left.getFirst().getSecond() == GreaterThanOrEqual.class) {
        newPair.setFirst(left.getFirst().getFirst().get(leftIndex).get(0).getFirst());
      } else if (left.getFirst().getSecond() == LessThan.class
          || left.getFirst().getSecond() == LessThanOrEqual.class) {
        newPair.setSecond(left.getFirst().getFirst().get(leftIndex).get(0).getSecond());
      }

      // Next switch on the right side to see if it is the greater than or less than clause and set
      // appropriate position
      if (right.getFirst().getSecond() == GreaterThan.class
          || right.getFirst().getSecond() == GreaterThanOrEqual.class) {
        newPair.setFirst(right.getFirst().getFirst().get(rightIndex).get(0).getFirst());
      } else if (right.getFirst().getSecond() == LessThan.class
          || right.getFirst().getSecond() == LessThanOrEqual.class) {
        newPair.setSecond(right.getFirst().getFirst().get(rightIndex).get(0).getSecond());
      }

      // Set the range
      List<Range> constructedRange = new ArrayList<Range>();
      constructedRange.add(new Range(newPair));

      constructedRanges.set(leftIndex, constructedRange);

      Pair<List<List<Range>>, Class> pair = new Pair<>(ranges, filterType);
      return new Pair<>(pair, finalQc);
      // Handle Or clauses as recursive calls
    } else if (filter instanceof Or) {
      Pair<Pair<List<List<Range>>, Class>, QueryCondition> left =
          buildRangeFromFilter(((Or) filter).left(), nonEmptyDomain);
      Pair<Pair<List<List<Range>>, Class>, QueryCondition> right =
          buildRangeFromFilter(((Or) filter).right(), nonEmptyDomain);

      // Get the Query Conditions that consist each branch of the expression. If they are not null
      // (they should never be null) combine them and create a new Query Condition to return.
      QueryCondition leftQc = left.getSecond();
      QueryCondition rightQc = right.getSecond();
      if (leftQc != null && rightQc != null) {
        finalQc = leftQc.combine(rightQc, TILEDB_OR);
        // close unneeded query conditions
        leftQc.close();
        rightQc.close();
      }

      for (int i = 0; i < left.getFirst().getFirst().size(); i++) {
        while (right.getFirst().getFirst().size() < i) {
          right.getFirst().getFirst().add(new ArrayList<>());
        }

        right.getFirst().getFirst().get(i).addAll(left.getFirst().getFirst().get(i));
      }
      right.setSecond(finalQc);

      return right;
      // Equal and EqualNullSafe are just straight dim1 = 1 fields. Set both side of range to single
      // value
    } else if (filter instanceof EqualNullSafe) {
      EqualNullSafe f = (EqualNullSafe) filter;
      int columnIndex = this.tileDBReadSchema.getColumnId(f.attribute()).get();
      ranges.get(columnIndex).add(new Range(new Pair<>(f.value(), f.value())));

      finalQc = createBaseQueryCondition(filterReferences[0], f.value(), TILEDB_EQ);

    } else if (filter instanceof EqualTo) {
      EqualTo f = (EqualTo) filter;
      int columnIndex = this.tileDBReadSchema.getColumnId(f.attribute()).get();
      ranges.get(columnIndex).add(new Range(new Pair<>(f.value(), f.value())));

      finalQc = createBaseQueryCondition(filterReferences[0], f.value(), TILEDB_EQ);

      // GreaterThan is ranges which are in the form of `dim > 1`
    } else if (filter instanceof GreaterThan) {
      GreaterThan f = (GreaterThan) filter;
      Object second;
      if (nonEmptyDomain.get(f.attribute()) != null)
        second = nonEmptyDomain.get(f.attribute()).getSecond();
      else second = getMaxValue(f.value().getClass());
      int columnIndex = this.tileDBReadSchema.getColumnId(f.attribute()).get();
      Number lowerBound =
          addEpsilon((Number) f.value(), this.tileDBReadSchema.columnTypes.get(columnIndex));
      ranges.get(columnIndex).add(new Range(new Pair<>(lowerBound, second)));

      finalQc = createBaseQueryCondition(filterReferences[0], f.value(), TILEDB_GT);

    } else if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
      Object second;
      if (nonEmptyDomain.get(f.attribute()) != null)
        second = nonEmptyDomain.get(f.attribute()).getSecond();
      else second = getMaxValue(f.value().getClass());
      int columnIndex = this.tileDBReadSchema.getColumnId(f.attribute()).get();
      ranges.get(columnIndex).add(new Range(new Pair<>(f.value(), second)));

      finalQc = createBaseQueryCondition(filterReferences[0], f.value(), TILEDB_GE);

      // For in filters we will add every value as ranges of 1. `dim IN (1, 2, 3)`
    } else if (filter instanceof In) {
      In f = (In) filter;
      // Add every value as a new range, TileDB will collapse into super ranges for us
      for (Object value : f.values()) {
        int dimIndex = this.tileDBReadSchema.getColumnId(f.attribute()).get();
        ranges.get(dimIndex).add(new Range(new Pair<>(value, value)));
      }

      // LessThan is ranges which are in the form of `dim < 1`
    } else if (filter instanceof LessThan) {
      LessThan f = (LessThan) filter;
      Object first;
      if (nonEmptyDomain.get(f.attribute()) != null)
        first = nonEmptyDomain.get(f.attribute()).getSecond();
      else first = getMinValue(f.value().getClass());
      int columnIndex = this.tileDBReadSchema.getColumnId(f.attribute()).get();
      ranges
          .get(columnIndex)
          .add(
              new Range(
                  new Pair<>(
                      first,
                      subtractEpsilon(
                          (Number) f.value(),
                          this.tileDBReadSchema.columnTypes.get(columnIndex)))));

      finalQc = createBaseQueryCondition(filterReferences[0], f.value(), TILEDB_LT);

      // LessThanOrEqual is ranges which are in the form of `dim <= 1`
    } else if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual f = (LessThanOrEqual) filter;
      Object first;
      if (nonEmptyDomain.get(f.attribute()) != null)
        first = nonEmptyDomain.get(f.attribute()).getSecond();
      else first = getMinValue(f.value().getClass());
      int columnIndex = this.tileDBReadSchema.getColumnId(f.attribute()).get();
      ranges.get(columnIndex).add(new Range(new Pair<>(first, f.value())));

      finalQc = createBaseQueryCondition(filterReferences[0], f.value(), TILEDB_LE);

    } else {
      throw new TileDBError("Unsupported filter type");
    }
    metricsUpdater.finish(dataSourceBuildRangeFromFilterTimerName);
    Pair<List<List<Range>>, Class> pair = new Pair<>(ranges, filterType);
    return new Pair<>(pair, finalQc);
  }

  /**
   * Create the base query condition. When this method is called we have reached the bottom of the
   * condition tree.
   *
   * @param attributeName the attributeName to apply the QC
   * @param filterValue the value to use for the QC
   * @param OP The QC OP
   * @return The base QC
   */
  public QueryCondition createBaseQueryCondition(
      String attributeName, Object filterValue, tiledb_query_condition_op_t OP) throws TileDBError {
    // for qc
    try {
      if (arraySchema.hasAttribute(attributeName)) {
        Attribute att = arraySchema.getAttribute(attributeName);
        QueryCondition finalQc =
            new QueryCondition(ctx, att.getName(), filterValue, att.getType().javaClass(), OP);
        att.close();
        return finalQc;
      }
    } catch (TileDBError e) {
      throw new RuntimeException(e);
    }

    if (domain.hasDimension(attributeName)) {
      throw new TileDBError(
          "You are applying a filter in a non existing attribute: " + attributeName);
    }
    return null;
  }

  /**
   * Returns the minimum value for the given datatype
   *
   * @param datatype
   * @return
   */
  private Object getMinValue(Class datatype) {
    if (datatype == Byte.class) {
      return Byte.MIN_VALUE;
    } else if (datatype == Short.class) {
      return Short.MIN_VALUE;
    } else if (datatype == Integer.class) {
      return Integer.MIN_VALUE;
    } else if (datatype == Long.class) {
      return Long.MIN_VALUE;
    } else if (datatype == Float.class) {
      return Float.MIN_VALUE;
    } else {
      return null;
    }
  }

  /**
   * Returns the maximum value for the given datatype
   *
   * @param datatype
   * @return
   */
  private Object getMaxValue(Class datatype) {
    if (datatype == Byte.class) {
      return Byte.MAX_VALUE;
    } else if (datatype == Short.class) {
      return Short.MAX_VALUE;
    } else if (datatype == Integer.class) {
      return Integer.MAX_VALUE;
    } else if (datatype == Long.class) {
      return Long.MAX_VALUE;
    } else if (datatype == Float.class) {
      return Float.MAX_VALUE;
    } else {
      return null;
    }
  }

  /**
   * Check and merge any and all ranges
   *
   * @param range
   * @return
   * @throws TileDBError
   */
  private List<Range> checkAndMergeRanges(List<Range> range) throws TileDBError {
    metricsUpdater.startTimer(dataSourceCheckAndMergeRangesTimerName);
    List<Range> rangesToBeMerged = new ArrayList<>(range);
    Collections.sort(rangesToBeMerged);

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

    metricsUpdater.finish(dataSourceCheckAndMergeRangesTimerName);
    return rangesToBeMerged;
  }

  /**
   * Computes the number of splits needed to reduce a subarray to a given size
   *
   * @param subArrayRanges
   * @param medianVolume
   * @param datatype
   * @return
   */
  private List<Integer> computeNeededSplitsToReduceToMedianVolume(
      List<SubArrayRanges> subArrayRanges, Number medianVolume, Class datatype) {
    metricsUpdater.startTimer(dataSourceComputeNeededSplitsToReduceToMedianVolumeTimerName);
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
    metricsUpdater.finish(dataSourceComputeNeededSplitsToReduceToMedianVolumeTimerName);
    return neededSplits;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    closeResources();
    return new TileDBPartitionReaderFactory();
  }

  private void closeResources() {
    array.close();
    try {
      ctx.close();
    } catch (TileDBError e) {
      // do nothing
    }
  }
}
