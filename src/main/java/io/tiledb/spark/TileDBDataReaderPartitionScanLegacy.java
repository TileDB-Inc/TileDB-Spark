package io.tiledb.spark;

import static io.tiledb.java.api.Datatype.*;
import static io.tiledb.java.api.QueryStatus.TILEDB_COMPLETED;
import static io.tiledb.java.api.QueryStatus.TILEDB_INCOMPLETE;
import static io.tiledb.java.api.QueryStatus.TILEDB_UNINITIALIZED;
import static io.tiledb.libtiledb.tiledb_query_condition_combination_op_t.TILEDB_AND;
import static io.tiledb.libtiledb.tiledb_query_condition_op_t.TILEDB_GE;
import static io.tiledb.libtiledb.tiledb_query_condition_op_t.TILEDB_LE;
import static org.apache.spark.metrics.TileDBMetricsSource.queryAllocBufferTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryCloseNativeArraysTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryGetDimensionTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryGetScalarAttributeTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryGetTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryGetVariableLengthAttributeTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryInitTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryNextTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryReadTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryReadTimerTaskName;
import static org.apache.spark.metrics.TileDBMetricsSource.tileDBReadQuerySubmitTimerName;

import io.tiledb.java.api.*;
import java.net.URI;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.metrics.TileDBReadMetricsUpdater;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import oshi.hardware.HardwareAbstractionLayer;

@Deprecated
public class TileDBDataReaderPartitionScanLegacy implements InputPartitionReader<ColumnarBatch> {

  static Logger log = Logger.getLogger(TileDBDataReaderPartitionScanLegacy.class.getName());

  // Filter pushdown to this partition
  private final List<List<Range>> allRanges;

  private final int dimensionRangesNum;
  // HAL for getting memory details about doubling buffers
  private final HardwareAbstractionLayer hardwareAbstractionLayer;
  private final TileDBReadMetricsUpdater metricsUpdater;

  // read buffer size
  private long read_query_buffer_size;

  // array resource URI (dense or sparse)
  private URI arrayURI;

  // spark options
  private TileDBDataSourceOptions options;

  // TileDB API resources for this array parition at the given URI
  private Context ctx;
  private ArraySchema arraySchema;
  private Array array;
  private Query query;
  private Domain domain;

  // Spark schema object associated with this projection (if any) for the query
  private StructType sparkSchema;

  // Spark columnar batch object to return from batch column iterator
  private ColumnarBatch resultBatch;

  // Spark batch column vectors
  private OnHeapColumnVector[] resultVectors;

  // Query status
  private QueryStatus queryStatus;

  private TaskContext task;

  private List<String> fieldNames;

  private static final OffsetDateTime zeroDateTime =
      new Timestamp(0).toInstant().atOffset(ZoneOffset.UTC).toInstant().atOffset(ZoneOffset.UTC);

  /**
   * List of NativeArray buffers used in the query object. This is indexed based on columnHandles
   * indexing (aka query field indexes)
   */
  private ArrayList<Pair<NativeArray, NativeArray>> queryBuffers;

  public TileDBDataReaderPartitionScanLegacy(
      URI uri,
      TileDBReadSchema schema,
      TileDBDataSourceOptions options,
      List<List<Range>> dimensionRanges,
      List<List<Range>> attributeRanges) {

    this.arrayURI = uri;
    this.sparkSchema = schema.getSparkSchema();
    this.options = options;
    this.queryStatus = TILEDB_UNINITIALIZED;
    this.dimensionRangesNum = dimensionRanges.size();
    this.allRanges = dimensionRanges;
    this.allRanges.addAll(attributeRanges);

    log.info("Using legacy reader");

    this.task = TaskContext.get();

    metricsUpdater = new TileDBReadMetricsUpdater(task);
    metricsUpdater.startTimer(queryReadTimerName);
    metricsUpdater.startTimer(queryReadTimerTaskName);

    task.addTaskCompletionListener(
        context -> {
          double duration = metricsUpdater.finish(queryReadTimerTaskName) / 1000000000d;
          log.debug("duration of read task " + task.toString() + " : " + duration + "s");
        });

    this.read_query_buffer_size = options.getReadBufferSizes();

    oshi.SystemInfo systemInfo = new oshi.SystemInfo();

    this.hardwareAbstractionLayer = systemInfo.getHardware();

    try {
      // Init TileDB resources
      ctx = new Context(options.getTileDBConfigMap(false));
      array = new Array(ctx, arrayURI.toString(), QueryType.TILEDB_READ);
      arraySchema = array.getSchema();
      domain = arraySchema.getDomain();

      if (sparkSchema.fields().length != 0)
        fieldNames =
            Arrays.stream(sparkSchema.fields())
                .map(field -> field.name())
                .collect(Collectors.toList());
      else {
        fieldNames =
            domain.getDimensions().stream()
                .map(
                    dimension -> {
                      try {
                        return dimension.getName();
                      } catch (TileDBError error) {
                        return null;
                      }
                    })
                .collect(Collectors.toList());
      }

      this.queryBuffers = new ArrayList<>(Collections.nCopies(fieldNames.size(), null));

      // init query
      this.initQuery();
    } catch (TileDBError tileDBError) {
      tileDBError.printStackTrace();
    }
  }

  @Override
  public boolean next() {
    metricsUpdater.startTimer(queryNextTimerName);
    try {
      // first submission initialize the query and see if we can fast fail;
      if (query == null) {
        initQuery();
      }

      // If the query was completed, and we have exhausted all records then we should close the
      // cursor
      if (queryStatus == TILEDB_COMPLETED) {
        metricsUpdater.finish(queryNextTimerName);
        return false;
      }

      do {
        metricsUpdater.startTimer(tileDBReadQuerySubmitTimerName);
        query.submit();
        metricsUpdater.finish(tileDBReadQuerySubmitTimerName);

        queryStatus = query.getQueryStatus();

        // Compute the number of cells (records) that were returned by the query.
        HashMap<String, Pair<Long, Long>> queryResultBufferElements = query.resultBufferElements();
        long currentNumRecords;

        String fieldName = fieldNames.get(0);
        boolean isVar;
        if (domain.hasDimension(fieldName)) isVar = domain.getDimension(fieldName).isVar();
        else isVar = arraySchema.getAttribute(fieldName).isVar();

        if (isVar) currentNumRecords = queryResultBufferElements.get(fieldNames.get(0)).getFirst();
        else currentNumRecords = queryResultBufferElements.get(fieldNames.get(0)).getSecond();

        // Increase the buffer allocation and resubmit if necessary.
        if (queryStatus == TILEDB_INCOMPLETE && currentNumRecords == 0) { // VERY IMPORTANT!!
          if (options.getAllowReadBufferReallocation()) {
            reallocateQueryBuffers();
          } else {
            throw new RuntimeException(
                "Incomplete query with no more records means the buffers are too small but allow_read_buffer_realloc is set to false!");
          }
        } else if (currentNumRecords > 0) {
          // Break out of resubmit loop as we have some results.
          metricsUpdater.finish(queryNextTimerName);
          return true;
        }
      } while (queryStatus == TILEDB_INCOMPLETE);
    } catch (TileDBError err) {
      throw new RuntimeException(err.getMessage());
    }

    metricsUpdater.finish(queryNextTimerName);
    return true;
  }

  @Override
  public ColumnarBatch get() {
    metricsUpdater.startTimer(queryGetTimerName);
    try {
      int colIdx = 0;
      int nRows = 0;
      // This is a special case for COUNT() on the table where no columns are materialized
      if (sparkSchema.fields().length == 0) {
        // TODO: materialize the first dimension and count the result set size
        try (Dimension dim = domain.getDimension(0)) {
          if (dim.isVar()) {
            nRows =
                getVarLengthAttributeColumn(
                    dim.getName(), dim.getType(), dim.isVar(), dim.getCellValNum(), 0);
          } else {
            nRows = getScalarValueColumn(dim.getName(), dim.getType(), 0);
          }
        }
      } else {
        // loop over all Spark attributes (DataFrame columns) and copy the query result set
        for (StructField field : sparkSchema.fields()) {
          nRows = getColumnBatch(field, colIdx);
          colIdx++;
        }
      }
      // set the number of rows for the batch result set this enables sharing the columnar batch
      // columns
      // across iterations and the total number of rows allocated will be the high water number of
      // rows over all batches
      resultBatch.setNumRows(nRows);
      // Note that calculateNativeArrayByteSizes() might not be
      this.metricsUpdater.updateTaskMetrics(nRows, calculateResultByteSize());
    } catch (TileDBError err) {
      throw new RuntimeException(err.getMessage());
    }
    metricsUpdater.finish(queryGetTimerName);
    return resultBatch;
  }

  /**
   * calculates the actual byte sizes of the results from the last invocation of query.submit()
   *
   * @return size in bytes of results
   * @throws TileDBError on error
   */
  private long calculateResultByteSize() throws TileDBError {
    long resultBytes = 0;
    HashMap<String, Pair<Long, Long>> resultBufferElements = query.resultBufferSizes();
    for (Map.Entry<String, Pair<Long, Long>> elementCount : resultBufferElements.entrySet()) {
      if (elementCount.getValue().getFirst() != null) {
        resultBytes += elementCount.getValue().getFirst();
      }

      if (elementCount.getValue().getSecond() != null) {
        resultBytes += elementCount.getValue().getSecond();
      }
    }
    return resultBytes;
  }

  @Override
  public void close() {
    if (resultBatch != null) {
      resultBatch.close();
    }

    closeQueryNativeArrays();
    queryBuffers.clear();

    if (query != null) {
      query.close();
    }
    if (arraySchema != null) {
      arraySchema.close();
    }
    if (array != null) {
      array.close();
    }
    if (ctx != null) {
      ctx.close();
    }

    // Close out spark buffers
    closeOnHeapColumnVectors();

    // Finish timer
    double duration = metricsUpdater.finish(queryReadTimerName) / 1000000000d;
    log.debug("duration of read-to-close" + task.toString() + " : " + duration + "s");
  }

  /**
   * Lazy initialize TileDB Query resources for this partition
   *
   * @return true if there are estimated to be results, false otherwise (fast fail)
   * @throws TileDBError A TileDB exception
   */
  private boolean initQuery() throws TileDBError {
    metricsUpdater.startTimer(queryInitTimerName);

    // TODO: Init with one subarray spanning the domain for now
    HashMap<String, Pair> nonEmptyDomain = array.nonEmptyDomain();

    // Create query and set the subarray for this partition
    query = new Query(array, QueryType.TILEDB_READ);

    // Pushdown any ranges
    QueryCondition finalCondition = null;
    if (allRanges.size() > 0) {
      // the first element of the allranges list is a list of the dimension ranges. The remaining
      // elements are singleton lists of the attribute ranges.
      List<Range> dimensionRanges = allRanges.get(0);
      List<List<Range>> attributeRanges = allRanges.subList(1, allRanges.size());

      int dimIndex = 0;
      for (Range range : dimensionRanges) {
        if (range.getFirst() == null || range.getSecond() == null) {
          continue;
        }
        if (arraySchema.getDomain().getDimension(dimIndex).isVar())
          query.addRangeVar(dimIndex, range.getFirst().toString(), range.getSecond().toString());
        else query.addRange(dimIndex, range.getFirst(), range.getSecond());
        dimIndex++;
      }

      int attIndex = 0;
      for (List<Range> ranges : attributeRanges) {
        for (Range range : ranges) {
          if (range.getFirst() == null || range.getSecond() == null) {
            continue;
          }
          Object lowBound;
          Object highBound;
          Attribute att = arraySchema.getAttribute(attIndex);
          boolean isString = att.getType().javaClass().equals(String.class);
          if (isString) {
            highBound = range.getSecond().toString().getBytes();
            lowBound = range.getFirst().toString().getBytes();
          } else {
            highBound = range.getSecond();
            lowBound = range.getFirst();
          }
          QueryCondition cond1 =
              new QueryCondition(
                  ctx, att.getName(), lowBound, att.getType().javaClass(), TILEDB_GE);
          QueryCondition cond2 =
              new QueryCondition(
                  ctx, att.getName(), highBound, att.getType().javaClass(), TILEDB_LE);
          QueryCondition cond3 = cond1.combine(cond2, TILEDB_AND);
          if (finalCondition == null) finalCondition = cond3;
          else finalCondition = finalCondition.combine(cond3, TILEDB_AND);
        }
        attIndex++;
      }

      if (finalCondition != null) query.setCondition(finalCondition);
    }

    // set query read layout
    setOptionQueryLayout(options.getArrayLayout());

    allocateQuerybuffers(this.read_query_buffer_size);

    // est that there are resuts, so perform a read for this partition
    metricsUpdater.finish(queryInitTimerName);
    return true;
  }

  /**
   * Function to calculate the bytes read based on the buffer sizes
   *
   * @return byte in current buffers
   */
  private long calculateNativeArrayByteSizes() {
    long totalBufferSizes = 0;
    long bufferCount = 0;
    long largestSingleBuffer = 0;
    for (Pair<NativeArray, NativeArray> bufferPair : queryBuffers) {
      NativeArray offsets = bufferPair.getFirst();
      NativeArray values = bufferPair.getSecond();
      if (values != null) {
        totalBufferSizes += values.getNBytes();
        if (values.getNBytes() > largestSingleBuffer) {
          largestSingleBuffer = values.getNBytes();
        }
        bufferCount++;
      }
      if (offsets != null) {
        totalBufferSizes += offsets.getNBytes();
        if (offsets.getNBytes() > largestSingleBuffer) {
          largestSingleBuffer = offsets.getNBytes();
        }
      }
      bufferCount++;
    }
    log.info(
        "Largest single buffer is "
            + largestSingleBuffer
            + " total buffer count is "
            + bufferCount);

    return totalBufferSizes;
  }

  /**
   * Check if we can double the buffer, or if there is not enough memory space
   *
   * @return
   */
  private boolean canReallocBuffers() {
    long freeMemory = this.hardwareAbstractionLayer.getMemory().getAvailable();

    long totalBufferSizes = calculateNativeArrayByteSizes();

    log.info(
        "Checking to realloc buffers from "
            + totalBufferSizes
            + " to "
            + 2 * totalBufferSizes
            + " with "
            + freeMemory
            + " memory free");

    // If we are going to double the buffers we need to make sure we have 4x space for
    // doubling the native buffer and copying to java arrays
    return freeMemory > (4 * totalBufferSizes);
  }

  private void reallocateQueryBuffers() throws TileDBError {
    if (!canReallocBuffers()) {
      throw new TileDBError("Not enough memory to complete query!");
    }

    if (resultBatch != null) {
      resultBatch.close();
    }

    // Reset
    query.resetBuffers();

    this.read_query_buffer_size *= 2;

    // Close out spark buffers
    closeOnHeapColumnVectors();

    allocateQuerybuffers(this.read_query_buffer_size);
  }

  private void allocateQuerybuffers(long readBufferSize) throws TileDBError {
    metricsUpdater.startTimer(queryAllocBufferTimerName);
    // Create coordinate buffers
    int minDimDize = Integer.MAX_VALUE;
    for (Dimension dimension : arraySchema.getDomain().getDimensions()) {
      int nativeSize = dimension.getType().getNativeSize();
      if (nativeSize < minDimDize) minDimDize = nativeSize;
    }

    int ncoords = Math.toIntExact(readBufferSize / minDimDize);

    // loop over all attributes and set the query buffers based on buffer size
    // the query object handles the lifetime of the allocated (offheap) NativeArrays
    int i = 0;

    for (String fieldName : fieldNames) {
      // get the spark column name and match to array schema
      String name = fieldName;
      Boolean isVar;
      Datatype type;

      if (domain.hasDimension(name)) {
        Dimension dim = domain.getDimension(name);
        type = dim.getType();
        isVar = dim.isVar();
      } else {
        Attribute attr = arraySchema.getAttribute(name);
        type = attr.getType();
        isVar = attr.isVar();
      }

      boolean nullable = false;

      if (this.arraySchema.hasAttribute(name)) {
        try (Attribute attr = this.arraySchema.getAttribute(name)) {
          nullable = attr.getNullable();
        }
      }

      int nvalues = Math.toIntExact(readBufferSize / type.getNativeSize());
      NativeArray data = new NativeArray(ctx, nvalues, type);
      // attribute is variable length, init the varlen result buffers using the est num offsets
      if (isVar) {
        int noffsets = Math.toIntExact(readBufferSize / TILEDB_UINT64.getNativeSize());
        NativeArray offsets = new NativeArray(ctx, noffsets, TILEDB_UINT64);
        if (nullable) {
          query.setBufferNullable(name, offsets, data, new NativeArray(ctx, nvalues, TILEDB_UINT8));
        } else {
          query.setBuffer(name, offsets, data);
        }

        queryBuffers.set(i++, new Pair<>(offsets, data));
      } else {
        // attribute is fixed length, use the result size estimate for allocation
        if (nullable) {
          query.setBufferNullable(
              name,
              new NativeArray(ctx, nvalues, type),
              new NativeArray(ctx, nvalues, TILEDB_UINT8));
        } else {
          query.setBuffer(name, new NativeArray(ctx, nvalues, type));
        }

        queryBuffers.set(i++, new Pair<>(null, data));
      }
    }

    // Allocate result set batch based on the estimated (upper bound) number of rows / cells
    resultVectors = OnHeapColumnVector.allocateColumns(ncoords, sparkSchema);
    resultBatch = new ColumnarBatch(resultVectors);

    metricsUpdater.finish(queryAllocBufferTimerName);
  }

  private void setOptionQueryLayout(Optional<Layout> layoutOption) throws TileDBError {
    if (arraySchema.isSparse()) {
      // sparse, set to array unordered (fastest) if not defined
      Layout defaultLayout = Layout.TILEDB_UNORDERED;
      if (layoutOption.isPresent()) {
        query.setLayout(layoutOption.get());
      } else {
        query.setLayout(defaultLayout);
      }
    } else {
      // dense, set default layout to array cell order (fastest)
      Layout defaultLayout = arraySchema.getCellOrder();
      if (layoutOption.isPresent()) {
        Layout layout = layoutOption.get();
        if (layout != Layout.TILEDB_UNORDERED) {
          query.setLayout(layoutOption.get());
        } else {
          query.setLayout(defaultLayout);
        }
      } else {
        query.setLayout(defaultLayout);
      }
    }
    return;
  }

  /**
   * For a given Spark field name, dispatch between attribute and dimension buffer copying
   *
   * @param field Spark field to copy query result set
   * @param index Spark field index in the projected schmema
   * @return number of values copied into the columnar batch result buffers
   * @throws TileDBError A TileDB exception
   */
  private int getColumnBatch(StructField field, int index) throws TileDBError {
    String name = field.name();

    Datatype dataType;
    long cellValNum;
    boolean isVar;

    if (arraySchema.hasAttribute(name)) {
      Attribute attribute = arraySchema.getAttribute(name);
      dataType = attribute.getType();
      cellValNum = attribute.getCellValNum();
      isVar = attribute.isVar();
    } else if (domain.hasDimension(name)) {
      Dimension dimension = domain.getDimension(name);
      dataType = dimension.getType();
      cellValNum = dimension.getCellValNum();
      isVar = dimension.isVar();
    } else {
      throw new TileDBError(
          "Array " + array.getUri() + " has no attribute/dimension with name " + name);
    }

    if (cellValNum > 1) {
      return getVarLengthAttributeColumn(name, dataType, isVar, cellValNum, index);
    } else {
      return getScalarValueColumn(name, dataType, index);
    }
  }

  /**
   * Helper method for the getScalarValueColumn() method. It puts attribute values in the
   * resultVector according to their datatype.
   *
   * @param name name of the attribute
   * @param dataType datatype of the attribute
   * @param index index of the resultVectorArray
   * @return number of values inserted
   * @throws TileDBError
   */
  private int putValuesOfAtt(String name, Datatype dataType, int index) throws TileDBError {
    int bufferLength;
    int numValues;
    OffsetDateTime ms;
    switch (dataType) {
      case TILEDB_FLOAT32:
        {
          float[] buff = (float[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putFloats(0, bufferLength, buff, 0);
          }
          break;
        }
      case TILEDB_FLOAT64:
        {
          double[] buff = (double[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putDoubles(0, bufferLength, buff, 0);
          }
          break;
        }
      case TILEDB_INT8:
      case TILEDB_CHAR:
        {
          byte[] buff = (byte[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putBytes(0, bufferLength, buff, 0);
          }
          break;
        }
      case TILEDB_INT16:
      case TILEDB_UINT8:
        {
          short[] buff = (short[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putShorts(0, bufferLength, buff, 0);
          }
          break;
        }
      case TILEDB_INT32:
      case TILEDB_UINT16:
        {
          int[] buff = (int[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putInts(0, bufferLength, buff, 0);
          }
          break;
        }
      case TILEDB_INT64:
      case TILEDB_UINT32:
      case TILEDB_UINT64:
      case TILEDB_DATETIME_US:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putLongs(0, bufferLength, buff, 0);
          }
          break;
        }
      case TILEDB_DATETIME_AS:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          long[] buffConverted = Arrays.stream(buff).map(i -> i / 1000000000000000L).toArray();
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putLongs(0, bufferLength, buffConverted, 0);
          }
          break;
        }
      case TILEDB_DATETIME_FS:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          long[] buffConverted = Arrays.stream(buff).map(i -> i / 1000000000).toArray();
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putLongs(0, bufferLength, buffConverted, 0);
          }
          break;
        }
      case TILEDB_DATETIME_PS:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          long[] buffConverted = Arrays.stream(buff).map(i -> i / 1000000).toArray();
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putLongs(0, bufferLength, buffConverted, 0);
          }
          break;
        }
      case TILEDB_DATETIME_NS:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          long[] buffConverted = Arrays.stream(buff).map(i -> i / 1000).toArray();
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putLongs(0, bufferLength, buffConverted, 0);
          }
          break;
        }
      case TILEDB_DATETIME_MS:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          long[] buffConverted = Arrays.stream(buff).map(i -> i * 1000).toArray();
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putLongs(0, bufferLength, buffConverted, 0);
          }
          break;
        }
      case TILEDB_DATETIME_SEC:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          long[] buffConverted = Arrays.stream(buff).map(i -> i * 1000000).toArray();
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putLongs(0, bufferLength, buffConverted, 0);
          }
          break;
        }
      case TILEDB_DATETIME_MIN:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          long[] buffConverted = Arrays.stream(buff).map(i -> i * 60 * 1000000).toArray();
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putLongs(0, bufferLength, buffConverted, 0);
          }
          break;
        }
      case TILEDB_DATETIME_HR:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          long[] buffConverted = Arrays.stream(buff).map(i -> i * 60 * 60 * 1000000).toArray();
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putLongs(0, bufferLength, buffConverted, 0);
          }
          break;
        }
      case TILEDB_DATETIME_DAY:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          for (int i = 0; i < buff.length; i++) {
            ms = zeroDateTime.plusDays(buff[i]);
            buff[i] = ChronoUnit.MICROS.between(zeroDateTime, ms);
          }
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putLongs(0, bufferLength, buff, 0);
          }
          break;
        }
      case TILEDB_DATETIME_WEEK:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          for (int i = 0; i < buff.length; i++) {
            ms = zeroDateTime.plusWeeks(buff[i]);
            buff[i] = ChronoUnit.MICROS.between(zeroDateTime, ms);
          }
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putLongs(0, bufferLength, buff, 0);
          }
          break;
        }
      case TILEDB_DATETIME_MONTH:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          for (int i = 0; i < buff.length; i++) {
            ms = zeroDateTime.plusMonths(buff[i]);
            buff[i] = ChronoUnit.MICROS.between(zeroDateTime, ms);
          }
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putLongs(0, bufferLength, buff, 0);
          }
          break;
        }
      case TILEDB_DATETIME_YEAR:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          for (int i = 0; i < buff.length; i++) {
            ms = zeroDateTime.plusYears(buff[i]);
            buff[i] = ChronoUnit.MICROS.between(zeroDateTime, ms);
          }
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putLongs(0, bufferLength, buff, 0);
          }
          break;
        }
      default:
        {
          throw new TileDBError("Not supported getDomain getType " + dataType);
        }
    }
    return numValues;
  }

  /**
   * This method is responsible for reading the values from the given attribute. It is only called
   * for attributes with fixed size.
   *
   * @param name name of the attribute.
   * @param dataType datatype of the attribute.
   * @param index index of the result vector that corresponds to this attribute.
   * @return
   * @throws TileDBError
   */
  private int getScalarValueColumn(String name, Datatype dataType, int index) throws TileDBError {

    metricsUpdater.startTimer(queryGetScalarAttributeTimerName);
    boolean nullable = false;
    int numValues;
    short[] validityByteMap;

    if (this.arraySchema.hasAttribute(name)) {
      try (Attribute attr = this.arraySchema.getAttribute(name)) {
        nullable = attr.getNullable();
      }
    }
    numValues = putValuesOfAtt(name, dataType, index);
    if (nullable) { // if the attribute is nullable, check the validity map and act accordingly.
      validityByteMap = query.getValidityByteMap(name);
      for (int i = 0; i < validityByteMap.length; i++) {
        if (validityByteMap[i] == 0) resultVectors[index].putNull(i);
      }
    }
    metricsUpdater.finish(queryGetScalarAttributeTimerName);
    return numValues;
  }

  /**
   * Helper method for the getVarLengthAttributeColumn() method. It puts attribute values in the
   * resultVector according to their datatype.
   *
   * @param name name of the attribute
   * @param dataType datatype of the attribute
   * @param index index of the resultVectorArray
   * @return number of values inserted
   * @throws TileDBError
   */
  private int putValuesOfAttVar(String name, Datatype dataType, int index) throws TileDBError {
    int bufferLength;
    OffsetDateTime ms;
    switch (dataType) {
      case TILEDB_FLOAT32:
        {
          float[] buff = (float[]) query.getBuffer(name);
          bufferLength = buff.length;
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putFloats(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_FLOAT64:
        {
          double[] buff = (double[]) query.getBuffer(name);
          bufferLength = buff.length;
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putDoubles(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_INT8:
      case TILEDB_CHAR:
        // string types that don't require any re-encoding to Spark UTF-8 representation supported
        // for now
      case TILEDB_STRING_ASCII:
      case TILEDB_STRING_UTF8:
        {
          byte[] buff = (byte[]) query.getBuffer(name);
          bufferLength = buff.length;
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putBytes(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_INT16:
      case TILEDB_UINT8:
        {
          short[] buff = (short[]) query.getBuffer(name);
          bufferLength = buff.length;
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putShorts(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_INT32:
      case TILEDB_UINT16:
        {
          int[] buff = (int[]) query.getBuffer(name);
          bufferLength = buff.length;
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putInts(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_INT64:
      case TILEDB_UINT32:
      case TILEDB_UINT64:
      case TILEDB_DATETIME_US:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putLongs(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_DATETIME_MS:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          long[] buffConverted = Arrays.stream(buff).map(i -> i * 1000).toArray();
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putLongs(0, bufferLength, buffConverted, 0);
          break;
        }
      case TILEDB_DATETIME_AS:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          long[] buffConverted = Arrays.stream(buff).map(i -> i / 1000000000000L).toArray();
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putLongs(0, bufferLength, buffConverted, 0);
          break;
        }
      case TILEDB_DATETIME_FS:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          long[] buffConverted = Arrays.stream(buff).map(i -> i / 1000000000).toArray();
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putLongs(0, bufferLength, buffConverted, 0);
          break;
        }
      case TILEDB_DATETIME_PS:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          long[] buffConverted = Arrays.stream(buff).map(i -> i / 1000000).toArray();
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putLongs(0, bufferLength, buffConverted, 0);
          break;
        }
      case TILEDB_DATETIME_NS:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          long[] buffConverted = Arrays.stream(buff).map(i -> i / 1000).toArray();
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putLongs(0, bufferLength, buffConverted, 0);
          break;
        }
      case TILEDB_DATETIME_SEC:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          long[] buffConverted = Arrays.stream(buff).map(i -> i * 1000000).toArray();
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putLongs(0, bufferLength, buffConverted, 0);
          break;
        }
      case TILEDB_DATETIME_MIN:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          long[] buffConverted = Arrays.stream(buff).map(i -> i * 1000000000).toArray();
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putLongs(0, bufferLength, buffConverted, 0);
          break;
        }
      case TILEDB_DATETIME_HR:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          long[] buffConverted = Arrays.stream(buff).map(i -> i * 1000000000000L).toArray();
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putLongs(0, bufferLength, buffConverted, 0);
          break;
        }
      case TILEDB_DATETIME_DAY:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          for (int i = 0; i < buff.length; i++) {
            ms = zeroDateTime.plusDays(buff[i]);
            buff[i] = ChronoUnit.MICROS.between(zeroDateTime, ms);
          }
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putLongs(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_DATETIME_WEEK:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          for (int i = 0; i < buff.length; i++) {
            ms = zeroDateTime.plusWeeks(buff[i]);
            buff[i] = ChronoUnit.MICROS.between(zeroDateTime, ms);
          }
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putLongs(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_DATETIME_MONTH:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          for (int i = 0; i < buff.length; i++) {
            ms = zeroDateTime.plusMonths(buff[i]);
            buff[i] = ChronoUnit.MICROS.between(zeroDateTime, ms);
          }
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putLongs(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_DATETIME_YEAR:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          for (int i = 0; i < buff.length; i++) {
            ms = zeroDateTime.plusYears(buff[i]);
            buff[i] = ChronoUnit.MICROS.between(zeroDateTime, ms);
          }
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putLongs(0, bufferLength, buff, 0);
          break;
        }
      default:
        {
          throw new TileDBError("Not supported getDomain getType " + dataType);
        }
    }
    return bufferLength;
  }

  /**
   * This method is responsible for reading the values from the given attribute. It is only called
   * for attributes with variable size.
   *
   * @param name name of the attribute.
   * @param dataType datatype of the attribute.
   * @param index index of the result vector that corresponds to this attribute.
   * @param isVar true if the attribute has variable length.
   * @return number of values inserted.
   * @throws TileDBError
   */
  private int getVarLengthAttributeColumn(
      String name, Datatype dataType, boolean isVar, long cellValNum, int index)
      throws TileDBError {
    metricsUpdater.startTimer(queryGetVariableLengthAttributeTimerName);
    boolean nullable = false;
    int numValues;
    int bufferLength = 0;
    short[] validityByteMap;
    // reset columnar batch start index

    if (this.arraySchema.hasAttribute(name)) {
      try (Attribute attr = this.arraySchema.getAttribute(name)) {
        nullable = attr.getNullable();
      }
    }

    resultVectors[index].reset();
    resultVectors[index].getChild(0).reset();
    bufferLength = putValuesOfAttVar(name, dataType, index);
    if (nullable) {
      validityByteMap = query.getValidityByteMap(name);
      for (int i = 0; i < validityByteMap.length; i++) { // check for null values.
        if (validityByteMap[i] == 0) resultVectors[index].putNull(i);
      }
    }

    if (isVar) {
      // add var length offsets
      long[] offsets = query.getVarBuffer(name);
      numValues = offsets.length;
      // number of bytes per (scalar) element in
      int typeSize = dataType.getNativeSize();
      long numBytes = bufferLength * typeSize;
      for (int j = 0; j < numValues; j++) {
        int off1 = Math.toIntExact(offsets[j] / typeSize);
        int off2 = Math.toIntExact((j < numValues - 1 ? offsets[j + 1] : numBytes) / typeSize);
        resultVectors[index].putArray(j, off1, off2 - off1);
      }
    } else {
      // fixed sized array attribute
      int cellNum = (int) cellValNum;
      numValues = bufferLength / cellNum;
      for (int j = 0; j < numValues; j++) {
        resultVectors[index].putArray(j, cellNum * j, cellNum);
      }
    }
    metricsUpdater.finish(queryGetVariableLengthAttributeTimerName);
    return numValues;
  }

  @Deprecated
  private int getDimensionColumn(String name, int index) throws TileDBError {
    metricsUpdater.startTimer(queryGetDimensionTimerName);
    int bufferLength = 0;
    Dimension dim = domain.getDimension(name);
    Datatype type = dim.getType();
    int ndim = Math.toIntExact(domain.getNDim());

    // perform a strided copy for dimension columnar buffers startng a dimIdx offset (slow path)
    switch (type) {
      case TILEDB_FLOAT32:
        {
          float[] buffer = (float[]) query.getBuffer(name);
          bufferLength = buffer.length;
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putFloats(0, bufferLength, buffer, 0);
          }
          break;
        }
      case TILEDB_FLOAT64:
        {
          double[] buffer = (double[]) query.getBuffer(name);
          bufferLength = buffer.length;
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putDoubles(0, bufferLength, buffer, 0);
          }
          break;
        }
      case TILEDB_INT8:
        {
          byte[] buffer = (byte[]) query.getBuffer(name);
          bufferLength = buffer.length;
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putBytes(0, bufferLength, buffer, 0);
          }
          break;
        }
      case TILEDB_INT16:
      case TILEDB_UINT8:
        {
          short[] buffer = (short[]) query.getBuffer(name);
          bufferLength = buffer.length;
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putShorts(0, bufferLength, buffer, 0);
          }
          break;
        }
      case TILEDB_UINT16:
      case TILEDB_INT32:
        {
          int[] buffer = (int[]) query.getBuffer(name);
          bufferLength = buffer.length;
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putInts(0, bufferLength, buffer, 0);
          }
          break;
        }
      case TILEDB_INT64:
      case TILEDB_UINT32:
      case TILEDB_UINT64:
      case TILEDB_DATETIME_MS:
        {
          long[] buffer = (long[]) query.getBuffer(name);
          bufferLength = buffer.length;
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putLongs(0, bufferLength, buffer, 0);
          }
          break;
        }
      case TILEDB_DATETIME_DAY:
        {
          long[] buffer = (long[]) query.getBuffer(name);
          bufferLength = bufferLength / ndim;
          if (resultVectors.length > 0) {
            resultVectors[index].reset();
            resultVectors[index].putLongs(0, bufferLength, buffer, 0);
          }
          break;
        }
      default:
        {
          throw new TileDBError("Unsupported dimension type for domain " + domain.getType());
        }
    }

    metricsUpdater.finish(queryGetDimensionTimerName);
    return bufferLength;
  }

  /** Close out onheap column vectors */
  private void closeOnHeapColumnVectors() {
    // Close the OnHeapColumnVector buffers
    for (OnHeapColumnVector buff : resultVectors) {
      buff.close();
    }
  }

  /** Close out all the NativeArray objects */
  private void closeQueryNativeArrays() {
    metricsUpdater.startTimer(queryCloseNativeArraysTimerName);
    for (Pair<NativeArray, NativeArray> bufferSet : queryBuffers) {
      if (bufferSet == null) {
        continue;
      }
      NativeArray offsetArray = bufferSet.getFirst();
      NativeArray valuesArray = bufferSet.getSecond();
      if (offsetArray != null) {
        offsetArray.close();
      }
      if (valuesArray != null) {
        valuesArray.close();
      }
    }
    metricsUpdater.finish(queryCloseNativeArraysTimerName);
  }
}
