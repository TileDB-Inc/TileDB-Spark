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

import io.netty.buffer.ArrowBuf;
import io.tiledb.java.api.*;
import java.lang.instrument.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.metrics.TileDBReadMetricsUpdater;
import org.apache.spark.sql.execution.arrow.ArrowUtils;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import oshi.hardware.HardwareAbstractionLayer;

public class TileDBDataReaderPartitionScan implements InputPartitionReader<ColumnarBatch> {

  private static Instrumentation instrumentation;

  static Logger log = Logger.getLogger(TileDBDataReaderPartitionScan.class.getName());

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

  private long currentNumRecords;

  private static final OffsetDateTime zeroDateTime =
      new Timestamp(0).toInstant().atOffset(ZoneOffset.UTC).toInstant().atOffset(ZoneOffset.UTC);

  /**
   * List of NativeArray buffers used in the query object. This is indexed based on columnHandles
   * indexing (aka query field indexes)
   */
  private ArrayList<Pair<NativeArray, NativeArray>> queryBuffers;

  private List<ValueVector> validityValueVectors;

  private List<ValueVector> valueValueVectors;

  private List<ByteBuffer> byteBuffers;

  private ArrayList<Pair<ValueVector, ValueVector>> queryArrowVectors;

  public enum AttributeDatatype {
    CHAR,
    UINT8,
    INT32,
    FLOAT32,
  }

  // todo think about lists
  public class TypeInfo {
    public AttributeDatatype datatype;
    public Datatype tileDBDataType;
    public boolean isVarLen;
    public boolean isNullable;
    public boolean isList;

    public TypeInfo(
        AttributeDatatype datatype,
        Datatype tiledbDataType,
        boolean isVarLen,
        boolean isNullable,
        boolean isList) {
      this.datatype = datatype;
      this.tileDBDataType = tiledbDataType;
      this.isVarLen = isVarLen;
      this.isNullable = isNullable;
      this.isList = isList;
    }
  }

  public TypeInfo getTypeInfo(String column) throws TileDBError {

    boolean isVarLen;
    boolean isNullable;
    boolean isList;
    Datatype datatype;

    if (arraySchema.hasAttribute(column)) {
      Attribute a = arraySchema.getAttribute(column);
      isVarLen = a.isVar();
      isNullable = a.getNullable();
      isList = false;
      datatype = a.getType();
    } else {
      Dimension d = arraySchema.getDomain().getDimension(column);
      isVarLen = d.isVar();
      isNullable = false;
      isList = false;
      datatype = d.getType();
    }

    switch (datatype) {
      case TILEDB_CHAR:
        return new TypeInfo(AttributeDatatype.CHAR, datatype, isVarLen, isNullable, isList);
      case TILEDB_INT8:
        return new TypeInfo(AttributeDatatype.UINT8, datatype, isVarLen, isNullable, isList);
      case TILEDB_INT32:
        return new TypeInfo(AttributeDatatype.INT32, datatype, isVarLen, isNullable, isList);
      case TILEDB_FLOAT32:
        return new TypeInfo(AttributeDatatype.FLOAT32, datatype, isVarLen, isNullable, isList);
      default:
        throw new RuntimeException("Unknown attribute datatype " + datatype);
        // TODO add all data types
    }
  }

  public TileDBDataReaderPartitionScan(
      URI uri,
      TileDBReadSchema schema,
      TileDBDataSourceOptions options,
      List<List<Range>> dimensionRanges,
      List<List<Range>> attributeRanges) {
    this.arrayURI = uri;
    this.validityValueVectors = new ArrayList<>();
    this.valueValueVectors = new ArrayList<>();
    this.byteBuffers = new ArrayList<>();
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
      ctx = new Context(options.getTileDBConfigMap());
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
        // //this returns something at least

        // Compute the number of cells (records) that were returned by the query.
        HashMap<String, Pair<Long, Long>> queryResultBufferElementsNIO =
            query.resultBufferElementsNIO();

        String fieldName = fieldNames.get(0); // TODO think again if it makes sense
        boolean isVar;
        if (domain.hasDimension(fieldName)) isVar = domain.getDimension(fieldName).isVar();
        else isVar = arraySchema.getAttribute(fieldName).isVar();

        if (isVar) currentNumRecords = queryResultBufferElementsNIO.get(fieldNames.get(0)).getFirst();
        else currentNumRecords = queryResultBufferElementsNIO.get(fieldNames.get(0)).getSecond();
        System.out.println(currentNumRecords + " Number of records:  ->>");

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
      int nRows = (int)currentNumRecords; // TODO fix. needs to be exact
      // This is a special case for COUNT() on the table where no columns are materialized
      //      if (sparkSchema.fields().length == 0) {
      //        // TODO: materialize the first dimension and count the result set size
      //        try (Dimension dim = domain.getDimension(0)) {
      //          if (dim.isVar()) {
      //            nRows =
      //                getVarLengthAttributeColumn(
      //                    dim.getName(), dim.getType(), dim.isVar(), dim.getCellValNum(), 0);
      //          } else {
      //            nRows = getScalarValueColumn(dim.getName(), dim.getType(), 0);
      //          }
      //        }
      //      } else {
      //        // loop over all Spark attributes (DataFrame columns) and copy the query result set
      //        for (StructField field : sparkSchema.fields()) {
      //          nRows = getColumnBatch(field, colIdx);
      //          colIdx++;
      //        }
      //      }
      //        Pair<ByteBuffer, ByteBuffer> a = query.getByteBuffer("a3");
      //        System.out.println(a.getSecond().get(0) + " <<<");

      if (resultBatch == null) {
        ColumnVector[] colVecs = new ColumnVector[valueValueVectors.size()];
        for (int i = 0; i < valueValueVectors.size(); i++) {
          String name = fieldNames.get(i);
          TypeInfo typeInfo = getTypeInfo(name);
          if (typeInfo.isNullable) {
            //todo explain logic in comments
            ArrowBuf arrowBufValidity = valueValueVectors.get(i).getValidityBuffer();
            for (int j = 0; j < arrowBufValidity.capacity(); j++) { //todo check if the limit can be nrows
              if (validityValueVectors.get(i).getDataBuffer().getByte(j) == (byte) 0){
                BitVectorHelper.setValidityBit(arrowBufValidity, j, 0);
              }
            }
          }
          colVecs[i] = new ArrowColumnVector(valueValueVectors.get(i));
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

    //    closeQueryNativeArrays();
    //    queryBuffers.clear();

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
    //    closeOnHeapColumnVectors();
    releaseArrowVectors();

    // force garbage collect
    System.gc();

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
    //    HashMap<String, Pair> nonEmptyDomain = array.nonEmptyDomain();

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

    //    // Close out spark buffers
    //    closeOnHeapColumnVectors();

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
    int i = 0;
    releaseArrowVectors();
    for (String fieldName : fieldNames) {
      // get the spark column name and match to array schema
      String name = fieldName;

      TypeInfo typeInfo = getTypeInfo(name);
      RootAllocator allocator = ArrowUtils.rootAllocator();
      ArrowType arrowType;
      ValueVector valueVector;
      ValueVector valueVectorValidity = new UInt1Vector(fieldName, allocator);
      switch (typeInfo.datatype) {
        case CHAR:
          if (!typeInfo.isVarLen)
            throw new RuntimeException(
                "Unhandled fixed-len char buffer for attribute " + fieldName);
          if (typeInfo.isList) {
            // Nested list (list of UTF8 which is already a list type)
            ListVector lv = ListVector.empty(fieldName, allocator);
            lv.addOrGetVector(FieldType.nullable(new ArrowType.Utf8()));
            valueVector = lv;
          } else {
            valueVector = new VarCharVector(fieldName, allocator);
          }
          break;
        case UINT8:
          // Because there are no unsigned datatypes, the uint8_t fields must be binary blobs, not
          // scalars.
          if (!typeInfo.isVarLen)
            throw new RuntimeException(
                "Unhandled fixed-len uint8_t buffer for attribute " + fieldName);
          // None of the attributes from TileDB-VCF currently can be a nested list except for
          // strings.
          if (typeInfo.isList)
            throw new RuntimeException("Unhandled nested list for attribute " + fieldName);
          valueVector = new VarBinaryVector(fieldName, allocator);
          break;
        case INT32:
          // None of the attributes from TileDB-VCF currently can be a nested list except for
          // strings.
          if (typeInfo.isVarLen && typeInfo.isList)
            throw new RuntimeException("Unhandled nested list for attribute " + fieldName);
          arrowType = new ArrowType.Int(32, true);
          if (typeInfo.isVarLen) {
            ListVector lv = ListVector.empty(fieldName, allocator);
            lv.addOrGetVector(FieldType.nullable(arrowType));
            valueVector = lv;
          } else {
            valueVector = new IntVector(fieldName, FieldType.nullable(arrowType), allocator);
          }
          break;
        case FLOAT32:
          // None of the attributes from TileDB-VCF currently can be a nested list except for
          // strings.
          if (typeInfo.isVarLen && typeInfo.isList)
            throw new RuntimeException("Unhandled nested list for attribute " + fieldName);
          arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
          if (typeInfo.isVarLen) {
            ListVector lv = ListVector.empty(fieldName, allocator);
            lv.addOrGetVector(FieldType.nullable(arrowType));
            valueVector = lv;
          } else {
            //            System.out.println(name + " floatvector");
            valueVector = new Float4Vector(fieldName, FieldType.nullable(arrowType), allocator);
          }
          break;
        default:
          throw new RuntimeException("Unhandled datatype for Arrow buffer, attribute " + fieldName);
      }

      // Max number of rows is nbytes / sizeof(int32_t), i.e. the max number of offsets that can be
      // stored.
      long maxRowsL =
          (readBufferSize
              / util.getDefaultRecordByteCount(
                  valueVector.getClass())); // TODO look if correct, seems ok

      int maxNumRows = util.longToInt(maxRowsL);


      if (valueVector instanceof ListVector) {
        ((ListVector) valueVector).setInitialCapacity(maxNumRows, 1); // TODO
      } else {
        valueVector.setInitialCapacity(maxNumRows);
      }
      valueVector.allocateNew();

      valueVectorValidity.setInitialCapacity(maxNumRows); //TODO doesent matter, can be smaller
      valueVectorValidity.allocateNew();

      ArrowBuf arrowValidity = valueVectorValidity.getDataBuffer();
      for (int j = 0; j < arrowValidity.capacity(); j++) {
        arrowValidity.setByte(j, 0xff);
      }
      ByteBuffer byteBufferValidity = arrowValidity.nioBuffer(0, arrowValidity.capacity());
      byteBufferValidity.order(ByteOrder.LITTLE_ENDIAN);

      ArrowBuf arrowData = valueVector.getDataBuffer();
      ByteBuffer data = arrowData.nioBuffer(0, arrowData.capacity());
      data.order(ByteOrder.LITTLE_ENDIAN); // necessary for arrow buffs

      ArrowBuf arrowBufValidity = valueVector.getValidityBuffer();
      for (int j = 0; j < arrowBufValidity.capacity(); j++) {
        arrowBufValidity.setByte(j, 0xff);
      }

      if (typeInfo.isVarLen) {
        // Set the offsets buffer.
        ArrowBuf arrowOffsets = valueVector.getOffsetBuffer();
        ByteBuffer offsets = arrowOffsets.nioBuffer(0, arrowOffsets.capacity());

        if (typeInfo.isNullable) {
          query.setBufferNullableNIO(
              name, offsets, data, byteBufferValidity); // /TODO add spark for bitmap
        } else {
          query.setBuffer(name, offsets, data);
        }

      } else {
        // attribute is fixed length, use the result size estimate for allocation
        if (typeInfo.isNullable) {
          query.setBufferNullableNIO(name, data, byteBufferValidity);
        } else {
          query.setBuffer(name, data);
        }
      }
      //      byteBuffers.add(data);
      //      this.arrowVectors.add(new ArrowColumnVector(valueVector));
      this.validityValueVectors.add(valueVectorValidity);
      this.valueValueVectors.add(valueVector);
      i++;
    }
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

  /** Closes any allocated Arrow vectors and clears the list. */
  private void releaseArrowVectors() {
    if (validityValueVectors != null) {
      for (ValueVector v : validityValueVectors) v.close();
      validityValueVectors.clear();
    }
    if (valueValueVectors != null) {
      for (ValueVector v : valueValueVectors) v.close();
      valueValueVectors.clear();
    }
  }
}
