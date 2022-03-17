package io.tiledb.spark;

import static io.tiledb.java.api.QueryStatus.TILEDB_COMPLETED;
import static io.tiledb.java.api.QueryStatus.TILEDB_INCOMPLETE;
import static io.tiledb.java.api.QueryStatus.TILEDB_UNINITIALIZED;
import static io.tiledb.libtiledb.tiledb_query_condition_combination_op_t.TILEDB_AND;
import static io.tiledb.libtiledb.tiledb_query_condition_op_t.TILEDB_GE;
import static io.tiledb.libtiledb.tiledb_query_condition_op_t.TILEDB_LE;
import static org.apache.spark.metrics.TileDBMetricsSource.queryAllocBufferTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryGetTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryInitTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryNextTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryReadTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryReadTimerTaskName;
import static org.apache.spark.metrics.TileDBMetricsSource.tileDBReadQuerySubmitTimerName;

import io.tiledb.java.api.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.metrics.TileDBReadMetricsUpdater;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import oshi.hardware.HardwareAbstractionLayer;

public class TileDBPartitionReader implements PartitionReader<ColumnarBatch> {

  static Logger log = Logger.getLogger(TileDBPartitionReader.class.getName());

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

  private List<Integer> fieldDataTypeSizes;

  private long currentNumRecords;

  private static final OffsetDateTime zeroDateTime =
      new Timestamp(0).toInstant().atOffset(ZoneOffset.UTC).toInstant().atOffset(ZoneOffset.UTC);

  private ArrayList<ByteBuffer> queryByteBuffers;

  private List<ValueVector> validityValueVectors;

  private List<ValueVector> valueVectors;

  public enum AttributeDatatype {
    CHAR,
    INT8,
    UINT8,
    INT16,
    INT32,
    FLOAT32,
    FlOAT64,
    UINT16,
    LONG,
    ASCII,
    DATE
  }

  public class TypeInfo {
    public AttributeDatatype datatype;
    public Datatype tileDBDataType;
    public boolean isVarLen;
    public boolean isArray;
    public boolean isNullable;
    public long multiplier;
    public boolean moreThanDay;

    public TypeInfo(
        AttributeDatatype datatype,
        Datatype tiledbDataType,
        boolean isVarLen,
        boolean isArray,
        boolean isNullable,
        long multiplier,
        boolean moreThanDay) {
      this.datatype = datatype;
      this.tileDBDataType = tiledbDataType;
      this.isVarLen = isVarLen;
      this.isArray = isArray;
      this.isNullable = isNullable;
      this.multiplier = multiplier;
      this.moreThanDay = moreThanDay;
    }
  }

  public TypeInfo getTypeInfo(String column) throws TileDBError {

    boolean isVarLen;
    boolean isArray;
    boolean isNullable;
    Datatype datatype;
    long multiplier = 1;

    if (arraySchema.hasAttribute(column)) {
      Attribute a = arraySchema.getAttribute(column);
      isVarLen = a.isVar();
      isArray = a.getCellValNum() > 1;
      isNullable = a.getNullable();
      datatype = a.getType();
    } else {
      Dimension d = arraySchema.getDomain().getDimension(column);
      isVarLen = d.isVar();
      isArray = d.getCellValNum() > 1;
      isNullable = false;
      datatype = d.getType();
    }

    switch (datatype) {
      case TILEDB_CHAR:
        return new TypeInfo(
            AttributeDatatype.CHAR, datatype, isVarLen, isArray, isNullable, multiplier, false);
      case TILEDB_STRING_ASCII:
        return new TypeInfo(
            AttributeDatatype.ASCII, datatype, isVarLen, isArray, isNullable, multiplier, false);
      case TILEDB_INT8:
        return new TypeInfo(
            AttributeDatatype.INT8, datatype, isVarLen, isArray, isNullable, multiplier, false);
      case TILEDB_INT32:
        return new TypeInfo(
            AttributeDatatype.INT32, datatype, isVarLen, isArray, isNullable, multiplier, false);
      case TILEDB_FLOAT32:
        return new TypeInfo(
            AttributeDatatype.FLOAT32, datatype, isVarLen, isArray, isNullable, multiplier, false);
      case TILEDB_FLOAT64:
        return new TypeInfo(
            AttributeDatatype.FlOAT64, datatype, isVarLen, isArray, isNullable, multiplier, false);
      case TILEDB_INT16:
        return new TypeInfo(
            AttributeDatatype.INT16, datatype, isVarLen, isArray, isNullable, multiplier, false);
      case TILEDB_UINT8:
        return new TypeInfo(
            AttributeDatatype.UINT8, datatype, isVarLen, isArray, isNullable, multiplier, false);
      case TILEDB_UINT16:
        return new TypeInfo(
            AttributeDatatype.UINT16, datatype, isVarLen, isArray, isNullable, multiplier, false);
      case TILEDB_INT64:
      case TILEDB_UINT32:
      case TILEDB_UINT64:
        return new TypeInfo(
            AttributeDatatype.LONG, datatype, isVarLen, isArray, isNullable, multiplier, false);
      case TILEDB_DATETIME_US:
        return new TypeInfo(
            AttributeDatatype.DATE, datatype, isVarLen, isArray, isNullable, multiplier, false);
      case TILEDB_DATETIME_MS:
        multiplier = 1000;
        return new TypeInfo(
            AttributeDatatype.DATE, datatype, isVarLen, isArray, isNullable, multiplier, false);
      case TILEDB_DATETIME_SEC:
        multiplier = 1000000;
        return new TypeInfo(
            AttributeDatatype.DATE, datatype, isVarLen, isArray, isNullable, multiplier, false);
      case TILEDB_DATETIME_MIN:
        multiplier = 60 * 1000000;
        return new TypeInfo(
            AttributeDatatype.DATE, datatype, isVarLen, isArray, isNullable, multiplier, false);
      case TILEDB_DATETIME_HR:
        multiplier = 60L * 60L * 1000000L;
        return new TypeInfo(
            AttributeDatatype.DATE, datatype, isVarLen, isArray, isNullable, multiplier, false);
      case TILEDB_DATETIME_NS:
        // negative number denotes that values need division
        multiplier = -1000;
        return new TypeInfo(
            AttributeDatatype.DATE, datatype, isVarLen, isArray, isNullable, multiplier, false);
      case TILEDB_DATETIME_DAY:
        return new TypeInfo(
            AttributeDatatype.DATE, datatype, isVarLen, isArray, isNullable, multiplier, true);
      case TILEDB_DATETIME_WEEK:
        multiplier = 7;
        return new TypeInfo(
            AttributeDatatype.DATE, datatype, isVarLen, isArray, isNullable, multiplier, true);
      case TILEDB_DATETIME_MONTH:
        // negative number with -moreThanDay- set to true means more than month.
        multiplier = -1;
        return new TypeInfo(
            AttributeDatatype.DATE, datatype, isVarLen, isArray, isNullable, multiplier, true);
      case TILEDB_DATETIME_YEAR:
        multiplier = -12;
        return new TypeInfo(
            AttributeDatatype.DATE, datatype, isVarLen, isArray, isNullable, multiplier, true);
      default:
        throw new RuntimeException("Unknown attribute datatype " + datatype);
    }
  }

  public TileDBPartitionReader(
      URI uri,
      TileDBReadSchema schema,
      TileDBDataSourceOptions options,
      List<List<Range>> dimensionRanges,
      List<List<Range>> attributeRanges) {
    this.arrayURI = uri;
    this.validityValueVectors = new ArrayList<>();
    this.valueVectors = new ArrayList<>();
    this.queryByteBuffers = new ArrayList<>();
    this.sparkSchema = schema.getSparkSchema();
    this.options = options;
    this.queryStatus = TILEDB_UNINITIALIZED;
    this.dimensionRangesNum = dimensionRanges.size();
    this.allRanges = dimensionRanges;
    this.allRanges.addAll(attributeRanges);

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
      ctx = new Context(options.getTileDBConfigMap(true));
      array = new Array(ctx, arrayURI.toString(), QueryType.TILEDB_READ);
      arraySchema = array.getSchema();
      domain = arraySchema.getDomain();

      if (sparkSchema.fields().length != 0) {
        fieldNames =
            Arrays.stream(sparkSchema.fields())
                .map(field -> field.name())
                .collect(Collectors.toList());

        fieldDataTypeSizes =
            Arrays.stream(sparkSchema.fields())
                .map(field -> field.dataType().defaultSize())
                .collect(Collectors.toList());
      } else {
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

        fieldDataTypeSizes =
            domain.getDimensions().stream()
                .map(
                    dimension -> {
                      try {
                        return dimension.getType().getNativeSize();
                      } catch (TileDBError error) {
                        return null;
                      }
                    })
                .collect(Collectors.toList());
      }

      this.queryByteBuffers = new ArrayList<>(Collections.nCopies(fieldNames.size(), null));

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

        // Compute the number of cells (records) that were returned by the query. The first field is
        // used.
        String fieldName = fieldNames.get(0);

        Pair<Long, Long> queryResultBufferElementsNIO =
            query.resultBufferElementsNIO(fieldName, fieldDataTypeSizes.get(0));

        boolean isVar;
        if (domain.hasDimension(fieldName)) isVar = domain.getDimension(fieldName).isVar();
        else isVar = arraySchema.getAttribute(fieldName).isVar();

        // Minus 1 for the extra element required by the arrow buffers
        if (isVar) currentNumRecords = queryResultBufferElementsNIO.getFirst() - 1;
        else currentNumRecords = queryResultBufferElementsNIO.getSecond();

        // if there are zero records and first field is varSize then this value needs to
        // change to 0, because of the minus 1 two lines above^
        if (currentNumRecords == -1) currentNumRecords = 0;

        // Increase the buffer allocation and resubmit if necessary.
        if (queryStatus == TILEDB_INCOMPLETE && currentNumRecords == 0) { // VERY IMPORTANT!!
          // todo reallocation is disabled at this point, more testing is needed.
          //          reallocateQueryBuffers();
          throw new TileDBError(
              "Read buffer size is too small. Please increase by using the -read_buffer_size- option");
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
      int nRows = (int) currentNumRecords;
      if (resultBatch == null) {
        ColumnVector[] colVecs = new ColumnVector[valueVectors.size()];
        for (int i = 0; i < valueVectors.size(); i++) {
          String name = fieldNames.get(i);
          TypeInfo typeInfo = getTypeInfo(name);
          boolean isDateType = typeInfo.multiplier != 1 || typeInfo.moreThanDay;

          // if nullable
          if (typeInfo.isNullable) {
            // If the attribute is nullable we need to set the validity buffer from the main value
            // vector in bitmap fashion.
            // TileDB handles the bitmap as a bytemap, thus the following conversion.
            ArrowBuf arrowBufValidity = valueVectors.get(i).getValidityBuffer();
            ArrowBuf validityByteBuffer = validityValueVectors.get(i).getDataBuffer();
            for (int j = 0; j < nRows; j++) {
              if (validityByteBuffer.getByte(j) == (byte) 0) {
                BitVectorHelper.setValidityBit(arrowBufValidity, j, 0);
              }
            }
          }

          if (isDateType) {
            if (typeInfo.isVarLen)
              throw new TileDBError(
                  "Var length attributes/dimensions of type TILEDB_DATETIME_* are not currently supported: "
                      + name);
            // it means that the datatype is Date and the values need filtering to
            // accommodate for the fewer datatypes that spark provides compared to TileDB.
            filterDataBufferForDateTypes(
                valueVectors.get(i).getDataBuffer(), currentNumRecords, typeInfo);
          }
          colVecs[i] = new ArrowColumnVector(valueVectors.get(i));
        }
        resultBatch = new ColumnarBatch(colVecs);
      }
      resultBatch.setNumRows(nRows);

      // Note that calculateNativeArrayByteSizes() might not be
      this.metricsUpdater.updateTaskMetrics(nRows, calculateResultByteSize());
    } catch (TileDBError err) {
      throw new RuntimeException(err.getMessage());
    }
    metricsUpdater.finish(queryGetTimerName);

    return resultBatch;
  }

  private void filterDataBufferForDateTypes(
      ArrowBuf dataBuffer, long currentNumRecords, TypeInfo typeInfo) {
    for (int i = 0; i < currentNumRecords; i++) {
      long newValue;
      if (typeInfo.moreThanDay) {
        OffsetDateTime ms;
        if (typeInfo.multiplier > 0) {
          ms = zeroDateTime.plusDays(dataBuffer.getLong(i) * typeInfo.multiplier);
        } else {
          // means that it is more than month and need different handling
          ms = zeroDateTime.plusMonths(dataBuffer.getLong(i) * Math.abs(typeInfo.multiplier));
        }
        newValue = ChronoUnit.MICROS.between(zeroDateTime, ms);

      } else {
        // negative multiplier means we need to divide
        if (typeInfo.multiplier > 0) newValue = dataBuffer.getLong(i) * typeInfo.multiplier;
        else newValue = dataBuffer.getLong(i) / Math.abs(typeInfo.multiplier);
      }
      dataBuffer.setLong(i, newValue);
    }
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
      resultBatch = null;
    }

    queryByteBuffers.clear();

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

    releaseArrowVectors();

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

    createValueVectors(this.read_query_buffer_size);

    // est that there are resuts, so perform a read for this partition
    metricsUpdater.finish(queryInitTimerName);
    return true;
  }

  /**
   * Function to calculate the bytes read based on the buffer sizes
   *
   * @return byte in current buffers
   */
  private long calculateByteSizes() {
    long totalBufferSize = 0;
    long bufferCount = 0;
    long largestSingleBuffer = 0;
    for (ByteBuffer byteBuffer : queryByteBuffers) {
      if (byteBuffer != null) {
        totalBufferSize += byteBuffer.capacity();
        if (byteBuffer.capacity() > largestSingleBuffer) {
          largestSingleBuffer = byteBuffer.capacity();
        }
        bufferCount++;
      }
    }
    log.info(
        "Largest single buffer is "
            + largestSingleBuffer
            + " total data buffer count is "
            + bufferCount);

    return totalBufferSize;
  }

  /**
   * Check if we can double the buffer, or if there is not enough memory space
   *
   * @return
   */
  private boolean canReallocBuffers() {
    long freeMemory = this.hardwareAbstractionLayer.getMemory().getAvailable();

    long totalBufferSizes = calculateByteSizes();

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

    if (resultBatch != null) resultBatch.close();

    // Reset
    query.resetBuffers();

    this.read_query_buffer_size *= 2;

    createValueVectors(this.read_query_buffer_size);
  }

  /**
   * Creates the value Vectors, later to be used to create the arrowBuffers for the query.
   *
   * @param readBufferSize the readBufferSize
   * @throws TileDBError
   */
  private void createValueVectors(long readBufferSize) throws TileDBError {
    metricsUpdater.startTimer(queryAllocBufferTimerName);
    // Create coordinate buffers
    int minDimDize = Integer.MAX_VALUE;
    for (Dimension dimension : arraySchema.getDomain().getDimensions()) {
      int nativeSize = dimension.getType().getNativeSize();
      if (nativeSize < minDimDize) minDimDize = nativeSize;
    }

    releaseArrowVectors();

    for (String fieldName : fieldNames) {
      // get the spark column name and match to array schema
      String name = fieldName;

      TypeInfo typeInfo = getTypeInfo(name);
      RootAllocator allocator = ArrowUtils.rootAllocator();
      ArrowType arrowType;
      ValueVector valueVector;

      // In theory we could try to replace the following UInt2Vector with Uint1Vector. However,
      // TileDB will throw an error that more validity cells are needed for the query. This
      // happens because apache-arrow rounds up the size of the data buffers, thus making it
      // necessary for us to provide more validity cells. This implementation provides double
      // the amount of validity cells necessary which makes it safe.
      ValueVector validityValueVector = new UInt2Vector(fieldName, allocator);

      switch (typeInfo.datatype) {
        case CHAR:
        case ASCII:
          if (!typeInfo.isVarLen)
            throw new RuntimeException(
                "Unhandled fixed-len char buffer for attribute " + fieldName);
          valueVector = new VarCharVector(fieldName, allocator);
          break;
        case UINT8:
        case INT8:
          arrowType = new ArrowType.Int(8, true);
          if (typeInfo.isVarLen) {
            ListVector lv = ListVector.empty(fieldName, allocator);
            lv.addOrGetVector(FieldType.nullable(arrowType));
            valueVector = lv;
          } else {
            valueVector = new TinyIntVector(fieldName, allocator);
          }
          break;
        case INT32:
          arrowType = new ArrowType.Int(32, true);
          if (typeInfo.isVarLen || typeInfo.isArray) {
            ListVector lv = ListVector.empty(fieldName, allocator);
            lv.addOrGetVector(FieldType.nullable(arrowType));
            valueVector = lv;
          } else {
            valueVector = new IntVector(fieldName, allocator);
          }
          break;
        case FLOAT32:
          arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
          if (typeInfo.isVarLen) {
            ListVector lv = ListVector.empty(fieldName, allocator);
            lv.addOrGetVector(FieldType.nullable(arrowType));
            valueVector = lv;
          } else {
            valueVector = new Float4Vector(fieldName, allocator);
          }
          break;
        case FlOAT64:
          arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
          if (typeInfo.isVarLen) {
            ListVector lv = ListVector.empty(fieldName, allocator);
            lv.addOrGetVector(FieldType.nullable(arrowType));
            valueVector = lv;
          } else {
            valueVector = new Float8Vector(fieldName, allocator);
          }
          break;
        case INT16:
        case UINT16:
          arrowType = new ArrowType.Int(16, true);
          if (typeInfo.isVarLen) {
            ListVector lv = ListVector.empty(fieldName, allocator);
            lv.addOrGetVector(FieldType.nullable(arrowType));
            valueVector = lv;
          } else {
            valueVector = new SmallIntVector(fieldName, allocator);
          }
          break;
        case LONG:
        case DATE:
          arrowType = new ArrowType.Int(64, true);
          if (typeInfo.isVarLen) {
            ListVector lv = ListVector.empty(fieldName, allocator);
            lv.addOrGetVector(FieldType.nullable(arrowType));
            valueVector = lv;
          } else {
            valueVector = new BigIntVector(fieldName, allocator);
          }
          break;
        default:
          throw new RuntimeException("Unhandled datatype for Arrow buffer, attribute " + fieldName);
      }

      // Max number of rows is nbytes / sizeof(int32_t), i.e. the max number of offsets that can be
      // stored.
      long maxRowsL = (readBufferSize / util.getDefaultRecordByteCount(valueVector.getClass()));

      int maxNumRows = util.longToInt(maxRowsL);

      // rare case when readbuffer size is set to a value smaller than the type
      if (maxNumRows == 0) maxNumRows = 1;

      if (valueVector instanceof ListVector) {
        ((ListVector) valueVector).setInitialCapacity(maxNumRows, 1);
      } else {
        valueVector.setInitialCapacity(maxNumRows);
      }
      validityValueVector.setInitialCapacity(maxNumRows);

      // The valueVector is the one holding the data and the corresponding validity and
      // offsetBuffers.
      // The validityValueVector is a help valueVector that holds the validity values in a byte
      // format which is the one expected from TileDB. The validity buffers in the main valueVector
      // is a bitmap instead!
      // A conversion between the two is needed when retrieving the data. See the code in the get()
      // method.
      valueVector.allocateNew();
      validityValueVector.allocateNew();

      createAndSetArrowBuffers(valueVector, validityValueVector, typeInfo, name);
    }
    metricsUpdater.finish(queryAllocBufferTimerName);
  }

  /**
   * Creates and sets the arrowBuffers to the query.
   *
   * @param valueVector The main valueVector
   * @param validityValueVector the helper valueVector for the validity map.
   * @param typeInfo the typeInfo
   * @param name current field name
   * @throws TileDBError
   */
  private void createAndSetArrowBuffers(
      ValueVector valueVector, ValueVector validityValueVector, TypeInfo typeInfo, String name)
      throws TileDBError {
    ByteBuffer data;
    if (valueVector instanceof ListVector) {
      ListVector lv = (ListVector) valueVector;
      ArrowBuf arrowData = lv.getDataVector().getDataBuffer();
      data = arrowData.nioBuffer(0, (int) arrowData.capacity());
      // For null list entries, TileDB-Spark will set the outer bitmap.
      // The inner (values) bitmap should be initialized to all non-null.
      ArrowBuf valueBitmap = lv.getDataVector().getValidityBuffer();
      int nbytes = (int) valueBitmap.capacity();
      for (int i = 0; i < nbytes; i++) {
        valueBitmap.setByte(i, 0xff);
      }
    } else {
      ArrowBuf arrowData = valueVector.getDataBuffer();
      data = arrowData.nioBuffer(0, (int) arrowData.capacity());
    }

    data.order(ByteOrder.LITTLE_ENDIAN);

    // for nulls
    ArrowBuf arrowBufBitMapValidity;
    arrowBufBitMapValidity = valueVector.getValidityBuffer();
    // necessary to populate with non-null values
    for (int j = 0; j < arrowBufBitMapValidity.capacity(); j++) {
      arrowBufBitMapValidity.setByte(j, 0xff);
    }

    ArrowBuf arrowBufByteMapValidity = validityValueVector.getDataBuffer();
    for (int j = 0; j < arrowBufByteMapValidity.capacity(); j++) {
      arrowBufByteMapValidity.setByte(j, 0xff);
    }
    ByteBuffer byteBufferValidity =
        arrowBufByteMapValidity.nioBuffer(0, (int) arrowBufByteMapValidity.capacity());
    byteBufferValidity.order(ByteOrder.LITTLE_ENDIAN);
    // end of nulls

    if (typeInfo.isVarLen) {
      // Set the offsets buffer.

      ArrowBuf arrowOffsets = valueVector.getOffsetBuffer();
      ByteBuffer offsets = arrowOffsets.nioBuffer(0, (int) arrowOffsets.capacity());

      if (typeInfo.isNullable) {
        query.setBufferNullableNIO(name, offsets, data, byteBufferValidity);
      } else {
        query.setBuffer(name, offsets, data);
      }
    } else {
      if (typeInfo.isNullable) {
        query.setBufferNullableNIO(name, data, byteBufferValidity);
      } else {
        query.setBuffer(name, data);
      }
    }
    this.queryByteBuffers.add(data);
    this.validityValueVectors.add(validityValueVector);
    this.valueVectors.add(valueVector);
  }

  /**
   * Sets the query layout.
   *
   * @param layoutOption the layout option.
   * @throws TileDBError
   */
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

  /** Closes any allocated Arrow vectors and clears the list. */
  private void releaseArrowVectors() {
    if (validityValueVectors != null) {
      for (ValueVector v : validityValueVectors) v.close();
      validityValueVectors.clear();
    }
    if (valueVectors != null) {
      for (ValueVector v : valueVectors) v.close();
      valueVectors.clear();
    }
  }
}
