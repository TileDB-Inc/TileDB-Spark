package io.tiledb.spark;

import static io.tiledb.java.api.Constants.TILEDB_COORDS;
import static io.tiledb.java.api.Datatype.TILEDB_UINT64;
import static io.tiledb.java.api.QueryStatus.TILEDB_COMPLETED;
import static io.tiledb.java.api.QueryStatus.TILEDB_INCOMPLETE;
import static io.tiledb.java.api.QueryStatus.TILEDB_UNINITIALIZED;

import io.tiledb.java.api.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import oshi.hardware.HardwareAbstractionLayer;
import scala.annotation.meta.field;

public class TileDBDataReaderPartitionScan implements InputPartitionReader<ColumnarBatch> {

  static Logger log = Logger.getLogger(TileDBDataReaderPartitionScan.class.getName());

  // Filter pushdown to this partition
  private final List<List<Range>> pushedRanges;

  // HAL for getting memory details about doubling buffers
  private final HardwareAbstractionLayer hardwareAbstractionLayer;

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

  // Spark schema object associated with this projection (if any) for the query
  private StructType sparkSchema;

  // Spark columnar batch object to return from batch column iterator
  private ColumnarBatch resultBatch;

  // Spark batch column vectors
  private OnHeapColumnVector[] resultVectors;

  // Query status
  private QueryStatus queryStatus;

  /**
   * List of NativeArray buffers used in the query object. This is indexed based on columnHandles
   * indexing (aka query field indexes)
   */
  private final ArrayList<Pair<NativeArray, NativeArray>> queryBuffers;

  public TileDBDataReaderPartitionScan(
      URI uri,
      TileDBReadSchema schema,
      TileDBDataSourceOptions options,
      List<List<Range>> pushedRanges) {
    this.arrayURI = uri;
    this.sparkSchema = schema.getSparkSchema();
    this.options = options;
    this.queryStatus = TILEDB_UNINITIALIZED;
    this.pushedRanges = pushedRanges;

    this.read_query_buffer_size = options.getReadBufferSizes();

    oshi.SystemInfo systemInfo = new oshi.SystemInfo();

    this.hardwareAbstractionLayer = systemInfo.getHardware();
    this.queryBuffers =
        new ArrayList<>(Collections.nCopies(schema.getSparkSchema().fields().length, null));

    try {
      // init query
      this.initQuery();
    } catch (TileDBError tileDBError) {
      tileDBError.printStackTrace();
    }
  }

  @Override
  public boolean next() {
    try (Domain domain = arraySchema.getDomain()) {
      // first submission initialize the query and see if we can fast fail;
      if (query == null) {
        initQuery();
      }

      // If the query was completed, and we have exhausted all records then we should close the
      // cursor
      if (queryStatus == TILEDB_COMPLETED) {
        return false;
      }

      do {
        query.submit();

        queryStatus = query.getQueryStatus();

        // Compute the number of cells (records) that were returned by the query.
        HashMap<String, Pair<Long, Long>> queryResultBufferElements = query.resultBufferElements();
        long currentNumRecords =
            queryResultBufferElements.get(TILEDB_COORDS).getSecond() / domain.getNDim();

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
          return true;
        }
      } while (queryStatus == TILEDB_INCOMPLETE);
    } catch (TileDBError err) {
      throw new RuntimeException(err.getMessage());
    }

    return true;
  }

  @Override
  public ColumnarBatch get() {
    try {
      int colIdx = 0;
      int nRows = 0;
      // This is a special case for COUNT() on the table where no columns are materialized
      if (sparkSchema.fields().length == 0) {
        // TODO: materialize the first dimension and count the result set size
        try (Domain domain = arraySchema.getDomain();
            Dimension dim = domain.getDimension(0)) {
          nRows = getDimensionColumn(dim.getName(), 0);
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
    } catch (TileDBError err) {
      throw new RuntimeException(err.getMessage());
    }
    return resultBatch;
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
  }

  /**
   * Lazy initialize TileDB Query resources for this partition
   *
   * @return true if there are estimated to be results, false otherwise (fast fail)
   * @throws TileDBError A TileDB exception
   */
  private boolean initQuery() throws TileDBError {
    ctx = new Context(options.getTileDBConfigMap());
    array = new Array(ctx, arrayURI.toString(), QueryType.TILEDB_READ);
    arraySchema = array.getSchema();
    try (Domain domain = arraySchema.getDomain();
        NativeArray nativeSubArray =
            new NativeArray(ctx, 2 * (int) domain.getNDim(), domain.getType())) {

      // TODO: Init with one subarray spanning the domain for now
      HashMap<String, Pair> nonEmptyDomain = array.nonEmptyDomain();

      // Create query and set the subarray for this partition
      query = new Query(array, QueryType.TILEDB_READ);

      // Pushdown any ranges
      if (pushedRanges.size() > 0) {
        for (List<Range> ranges : pushedRanges) {
          for (int i = 0; i < ranges.size(); i++) {
            query.addRange(i, ranges.get(i).getFirst(), ranges.get(i).getSecond());
          }
        }
      } else {
        // TODO: Remove this because it should be handled in the partitioning now
        // If there was no filter to pushdown, we must select the entire nonEmptyDomain
        for (int i = 0; i < domain.getNDim(); i++) {
          try (Dimension dim = domain.getDimension(i)) {
            Pair extent = nonEmptyDomain.get(dim.getName());
            nativeSubArray.setItem(i * 2, extent.getFirst());
            nativeSubArray.setItem(i * 2 + 1, extent.getSecond());
          }
        }
        query.setSubarray(nativeSubArray);
      }

      // set query read layout
      setOptionQueryLayout(options.getArrayLayout());

      allocateQuerybuffers(this.read_query_buffer_size);
    }
    // est that there are resuts, so perform a read for this partition
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
    log.log(
        Level.INFO,
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

    log.log(
        Level.INFO,
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

    query.resetBuffers();

    this.read_query_buffer_size *= 2;

    allocateQuerybuffers(this.read_query_buffer_size);
  }

  private void allocateQuerybuffers(long readBufferSize) throws TileDBError {

    try (Domain domain = arraySchema.getDomain(); ) {
      // Create coordinate buffers
      int ncoords = Math.toIntExact(readBufferSize / domain.getType().getNativeSize());
      NativeArray coordBuffer = new NativeArray(ctx, ncoords, domain.getType());

      // loop over all attributes and set the query buffers based on buffer size
      // the query object handles the lifetime of the allocated (offheap) NativeArrays
      int i = 0;
      for (StructField field : sparkSchema.fields()) {
        // get the spark column name and match to array schema
        String name = field.name();
        if (domain.hasDimension(name)) {
          queryBuffers.set(i++, new Pair<>(null, coordBuffer));
          // dimension column (coordinate buffer allocation handled at the end)
          continue;
        }
        try (Attribute attr = arraySchema.getAttribute(name)) {
          int nvalues = Math.toIntExact(readBufferSize / attr.getType().getNativeSize());
          NativeArray data = new NativeArray(ctx, nvalues, attr.getType());
          // attribute is variable length, init the varlen result buffers using the est num offsets
          if (attr.isVar()) {
            int noffsets = Math.toIntExact(readBufferSize / TILEDB_UINT64.getNativeSize());
            NativeArray offsets = new NativeArray(ctx, noffsets, TILEDB_UINT64);
            query.setBuffer(name, offsets, data);
            queryBuffers.set(i++, new Pair<>(offsets, data));
          } else {
            // attribute is fixed length, use the result size estimate for allocation
            query.setBuffer(name, new NativeArray(ctx, nvalues, attr.getType()));
            queryBuffers.set(i++, new Pair<>(null, data));
          }
        }
      }
      // set the coordinate buffer result buffer
      query.setCoordinates(coordBuffer);

      // Allocate result set batch based on the estimated (upper bound) number of rows / cells
      resultVectors =
          OnHeapColumnVector.allocateColumns(
              Math.toIntExact(ncoords / domain.getNDim()), sparkSchema);
      resultBatch = new ColumnarBatch(resultVectors);
    }
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
    if (arraySchema.hasAttribute(name)) {
      return getAttributeColumn(name, index);
    } else {
      // ith dimension column, need to special case zipped coordinate buffers
      return getDimensionColumn(name, index);
    }
  }

  /**
   * For a given attribute name, dispatch between variable length and scalar buffer copying
   *
   * @param name Attribute name
   * @param index The Attribute index in the columnar buffer array
   * @return number of values copied into the columnar batch result buffers
   * @throws TileDBError A TileDB exception
   */
  private int getAttributeColumn(String name, int index) throws TileDBError {
    try (Attribute attribute = arraySchema.getAttribute(name)) {
      if (attribute.getCellValNum() > 1) {
        // variable length values added as arrays
        return getVarLengthAttributeColumn(name, attribute, index);
      } else {
        // one value per cell
        return getScalarValueAttributeColumn(name, attribute, index);
      }
    }
  }

  private int getScalarValueAttributeColumn(String name, Attribute attribute, int index)
      throws TileDBError {
    int numValues;
    int bufferLength;
    switch (attribute.getType()) {
      case TILEDB_FLOAT32:
        {
          float[] buff = (float[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          resultVectors[index].reset();
          resultVectors[index].putFloats(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_FLOAT64:
        {
          double[] buff = (double[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          resultVectors[index].reset();
          resultVectors[index].putDoubles(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_INT8:
      case TILEDB_CHAR:
        {
          byte[] buff = (byte[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          resultVectors[index].reset();
          resultVectors[index].putBytes(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_INT16:
      case TILEDB_UINT8:
        {
          short[] buff = (short[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          resultVectors[index].reset();
          resultVectors[index].putShorts(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_INT32:
      case TILEDB_UINT16:
        {
          int[] buff = (int[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          resultVectors[index].reset();
          resultVectors[index].putInts(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_INT64:
      case TILEDB_UINT32:
      case TILEDB_UINT64:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          resultVectors[index].reset();
          resultVectors[index].putLongs(0, bufferLength, buff, 0);
          break;
        }
      default:
        {
          throw new TileDBError("Not supported getDomain getType " + attribute.getType());
        }
    }
    return numValues;
  }

  private int getVarLengthAttributeColumn(String name, Attribute attribute, int index)
      throws TileDBError {
    int numValues = 0;
    int bufferLength = 0;
    // reset columnar batch start index
    resultVectors[index].reset();
    resultVectors[index].getChild(0).reset();
    switch (attribute.getType()) {
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
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putLongs(0, bufferLength, buff, 0);
          break;
        }
      default:
        {
          throw new TileDBError("Not supported getDomain getType " + attribute.getType());
        }
    }
    if (attribute.isVar()) {
      // add var length offsets
      long[] offsets = query.getVarBuffer(name);
      // number of bytes per (scalar) element in
      int typeSize = attribute.getType().getNativeSize();
      for (int j = 0; j < offsets.length; j++) {
        // for every variable elgnth value, compute the Spark offset location from the number of
        // bytes / varlen cell
        int length =
            (j == offsets.length - 1)
                ? bufferLength * typeSize - (int) offsets[j]
                : (int) offsets[j + 1] - (int) offsets[j];
        resultVectors[index].putArray(j, ((int) offsets[j]) / typeSize, length / typeSize);
      }
      numValues = offsets.length;
    } else {
      // fixed sized array attribute
      int cellNum = (int) attribute.getCellValNum();
      numValues = bufferLength / cellNum;
      for (int j = 0; j < numValues; j++) {
        resultVectors[index].putArray(j, cellNum * j, cellNum);
      }
    }
    return numValues;
  }

  private int getDimensionColumn(String name, int index) throws TileDBError {
    int numValues = 0;
    int bufferLength = 0;
    try (Domain domain = arraySchema.getDomain()) {
      int ndim = Math.toIntExact(domain.getNDim());
      int dimIdx = 0;
      // map
      for (; dimIdx < ndim; dimIdx++) {
        try (Dimension dim = domain.getDimension(dimIdx)) {
          if (dim.getName().equals(name)) {
            break;
          }
        }
      }
      // perform a strided copy for dimension columnar buffers startng a dimIdx offset (slow path)
      switch (domain.getType()) {
        case TILEDB_FLOAT32:
          {
            float[] coords = (float[]) query.getCoordinates();
            bufferLength = coords.length;
            numValues = bufferLength / ndim;
            if (resultVectors.length > 0) {
              resultVectors[index].reset();
              for (int i = dimIdx, row = 0; i < bufferLength; i += ndim, row++) {
                resultVectors[index].putFloat(row, coords[i]);
              }
            }
            break;
          }
        case TILEDB_FLOAT64:
          {
            double[] coords = (double[]) query.getCoordinates();
            bufferLength = coords.length;
            numValues = bufferLength / ndim;
            if (resultVectors.length > 0) {
              resultVectors[index].reset();
              for (int i = dimIdx, row = 0; i < bufferLength; i += ndim, row++) {
                resultVectors[index].putDouble(row, coords[i]);
              }
            }
            break;
          }
        case TILEDB_INT8:
          {
            byte[] coords = (byte[]) query.getCoordinates();
            bufferLength = coords.length;
            numValues = bufferLength / ndim;
            if (resultVectors.length > 0) {
              resultVectors[index].reset();
              for (int i = dimIdx, row = 0; i < bufferLength; i += ndim, row++) {
                resultVectors[index].putByte(row, coords[i]);
              }
            }
            break;
          }
        case TILEDB_INT16:
        case TILEDB_UINT8:
          {
            short[] coords = (short[]) query.getCoordinates();
            bufferLength = coords.length;
            numValues = bufferLength / ndim;
            if (resultVectors.length > 0) {
              resultVectors[index].reset();
              for (int i = dimIdx, row = 0; i < bufferLength; i += ndim, row++) {
                resultVectors[index].putShort(row, coords[i]);
              }
            }
            break;
          }
        case TILEDB_UINT16:
        case TILEDB_INT32:
          {
            int[] coords = (int[]) query.getCoordinates();
            bufferLength = coords.length;
            numValues = bufferLength / ndim;
            if (resultVectors.length > 0) {
              resultVectors[index].reset();
              for (int i = dimIdx, row = 0; i < bufferLength; i += ndim, row++) {
                resultVectors[index].putInt(row, coords[i]);
              }
            }
            break;
          }
        case TILEDB_INT64:
        case TILEDB_UINT32:
        case TILEDB_UINT64:
          {
            long[] coords = (long[]) query.getCoordinates();
            bufferLength = coords.length;
            numValues = bufferLength / ndim;
            if (resultVectors.length > 0) {
              resultVectors[index].reset();
              for (int i = dimIdx, row = 0; i < bufferLength; i += ndim, row++) {
                resultVectors[index].putLong(row, coords[i]);
              }
            }
            break;
          }
        default:
          {
            throw new TileDBError("Unsupported dimension type for domain " + domain.getType());
          }
      }
    }
    return numValues;
  }

  /** Close out all the NativeArray objects */
  private void closeQueryNativeArrays() {
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
  }
}
