package io.tiledb.spark;

import static org.apache.spark.metrics.TileDBMetricsSource.queryResetWriteQueryAndBuffersTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryWriteCloseTileDBResourcesTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryWriteCommitTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryWriteFlushBuffersTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryWriteRecordToBufferTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryWriteRowTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryWriteTaskTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.queryWriteTimerName;

import io.tiledb.java.api.*;
import java.io.IOException;
import java.net.URI;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.metrics.TileDBWriteMetricsUpdater;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TileDBDataWriter implements DataWriter<InternalRow> {

  static Logger log = Logger.getLogger(TileDBDataWriter.class.getName());

  private final TileDBWriteMetricsUpdater metricsUpdater;
  private final TaskContext task;
  private URI uri;
  private StructType sparkSchema;

  private Context ctx;
  private Array array;
  private Query query;

  private final int nDims;
  // map struct fields / dataframe columns to array schema original order
  // coordinate buffers are first, attribute buffer positions are attrIdx + numDim
  private final int[] bufferIndex;
  private final String[] bufferNames;
  private final Datatype[] bufferDatatypes;
  private final long[] bufferValNum;

  // array holding native array buffers for row buffering
  // there are ndim + nattribute buffers to prepare for heterogenous domains
  private NativeArray[] nativeArrayOffsetBuffers;
  private NativeArray[] nativeArrayBuffers;
  private int[] nativeArrayOffsetElements;
  private int[] nativeArrayBufferElements;
  private long writeBufferSize;
  private int nRecordsBuffered;

  public TileDBDataWriter(URI uri, StructType schema, TileDBDataSourceOptions options) {
    this.uri = uri;
    this.sparkSchema = schema;
    // set write options
    writeBufferSize = options.getWriteBufferSize();
    this.metricsUpdater = new TileDBWriteMetricsUpdater(TaskContext.get());
    this.metricsUpdater.startTimer(queryWriteTimerName);
    this.metricsUpdater.startTimer(queryWriteTaskTimerName);

    task = TaskContext.get();
    task.addTaskCompletionListener(
        context -> {
          double duration = metricsUpdater.finish(queryWriteTaskTimerName) / 1000000000d;
          log.debug("duration of write task " + task.toString() + " : " + duration + "s");
        });

    // mapping of fields to dimension / attributes in TileDB schema
    StructField[] sparkSchemaFields = schema.fields();
    int nFields = sparkSchemaFields.length;
    bufferIndex = new int[nFields];

    bufferNames = new String[nFields];
    bufferValNum = new long[nFields];
    bufferDatatypes = new Datatype[nFields];
    nativeArrayOffsetBuffers = new NativeArray[nFields];
    nativeArrayOffsetElements = new int[nFields];
    nativeArrayBuffers = new NativeArray[nFields];
    nativeArrayBufferElements = new int[nFields];

    try {
      ctx = new Context(options.getTileDBConfigMap());
      array = new Array(ctx, uri.toString(), QueryType.TILEDB_WRITE);
      try (ArraySchema arraySchema = array.getSchema()) {
        assert arraySchema.isSparse();
        try (Domain domain = arraySchema.getDomain()) {
          nDims = Math.toIntExact(domain.getNDim());
          for (int i = 0; i < domain.getRank(); i++) {
            try (Dimension dim = domain.getDimension(i)) {
              String dimName = dim.getName();
              for (int di = 0; di < bufferIndex.length; di++) {
                if (sparkSchemaFields[di].name().equals(dimName)) {
                  bufferIndex[di] = i;
                  bufferNames[i] = dimName;
                  bufferDatatypes[i] = dim.getType();
                  bufferValNum[i] = 1;
                  break;
                }
              }
            }
          }
        }
        for (int i = 0; i < arraySchema.getAttributeNum(); i++) {
          try (Attribute attribute = arraySchema.getAttribute(i)) {
            String attrName = attribute.getName();
            for (int ai = 0; ai < bufferIndex.length; ai++) {
              if (sparkSchemaFields[ai].name().equals(attrName)) {
                int bufferIdx = nDims + i;
                bufferIndex[ai] = bufferIdx;
                bufferNames[bufferIdx] = attrName;
                bufferDatatypes[bufferIdx] = attribute.getType();
                bufferValNum[bufferIdx] = attribute.getCellValNum();
              }
            }
          }
        }
      }
      resetWriteQueryAndBuffers();
    } catch (TileDBError err) {
      err.printStackTrace();
      throw new RuntimeException(err.getMessage());
    }
  }

  private void resetWriteQueryAndBuffers() throws TileDBError {
    this.metricsUpdater.startTimer(queryResetWriteQueryAndBuffersTimerName);
    if (query != null) {
      query.close();
    }
    query = new Query(array, QueryType.TILEDB_WRITE);
    query.setLayout(Layout.TILEDB_UNORDERED);

    int bufferIdx = 0;
    try (ArraySchema arraySchema = array.getSchema()) {
      try (Domain domain = arraySchema.getDomain()) {
        int numElements = Math.toIntExact(writeBufferSize / domain.getType().getNativeSize());
        NativeArray coordsBuffer = new NativeArray(ctx, numElements, domain.getType());
        nativeArrayBuffers[bufferIdx] = coordsBuffer;
        nativeArrayBufferElements[bufferIdx] = numElements;
        query.setBuffer(Constants.TILEDB_COORDS, coordsBuffer);
        // we just skip over all dims for now (special case zipped coordinates)
        bufferIdx += nDims;
      }
      for (int i = 0; i < arraySchema.getAttributeNum(); i++) {
        try (Attribute attr = arraySchema.getAttribute(i)) {
          String attrName = attr.getName();
          if (attr.isVar()) {
            int numOffsets =
                Math.toIntExact(writeBufferSize / Datatype.TILEDB_UINT64.getNativeSize());
            NativeArray bufferOff = new NativeArray(ctx, numOffsets, Datatype.TILEDB_UINT64);
            nativeArrayOffsetBuffers[bufferIdx] = bufferOff;
            nativeArrayOffsetElements[bufferIdx] = 0;

            int numElements = Math.toIntExact(writeBufferSize / attr.getType().getNativeSize());
            NativeArray bufferData = new NativeArray(ctx, numElements, attr.getType());
            nativeArrayBuffers[bufferIdx] = bufferData;
            nativeArrayBufferElements[bufferIdx] = 0;

            query.setBuffer(attrName, bufferOff, bufferData);
            bufferIdx += 1;
          } else {
            int numElements = Math.toIntExact(writeBufferSize / attr.getType().getNativeSize());
            NativeArray bufferData = new NativeArray(ctx, numElements, attr.getType());
            nativeArrayBuffers[bufferIdx] = bufferData;
            nativeArrayBufferElements[bufferIdx] = 0;
            query.setBuffer(attrName, bufferData);
            bufferIdx += 1;
          }
        }
      }
    }
    nRecordsBuffered = 0;
    this.metricsUpdater.finish(queryResetWriteQueryAndBuffersTimerName);
    return;
  }

  private boolean bufferDimensionValue(int dimIdx, InternalRow record, int ordinal)
      throws TileDBError {
    // special case zipped coordinate for now
    int bufferIdx = 0;
    int bufferElements = (nRecordsBuffered * nDims) + dimIdx;
    return writeRecordToBuffer(bufferIdx, bufferElements, record, ordinal);
  }

  private boolean bufferAttributeValue(int attrIdx, InternalRow record, int ordinal)
      throws TileDBError {
    int bufferIdx = nDims + attrIdx;
    int bufferElements = nRecordsBuffered;
    return writeRecordToBuffer(bufferIdx, bufferElements, record, ordinal);
  }

  private boolean writeRecordToBuffer(
      int bufferIdx, int bufferElement, InternalRow record, int ordinal) throws TileDBError {
    this.metricsUpdater.startTimer(queryWriteRecordToBufferTimerName);
    Datatype dtype = bufferDatatypes[bufferIdx];
    NativeArray buffer = nativeArrayBuffers[bufferIdx];
    NativeArray offsets = nativeArrayOffsetBuffers[bufferIdx];
    boolean isArray = bufferValNum[bufferIdx] > 1l;
    int maxBufferElements = buffer.getSize();
    if (bufferElement >= maxBufferElements) {
      this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
      return true;
    }
    if (isArray) {
      // rare, would have to be a repeat of zero sized values
      int maxOffsetElements = offsets.getSize();
      if (bufferElement >= maxOffsetElements) {
        this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
        return true;
      }
    }
    switch (dtype) {
      case TILEDB_INT8:
        {
          if (isArray) {
            byte[] array = record.getArray(ordinal).toByteArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            if ((bufferOffset + array.length) > maxBufferElements) {
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.setItem(bufferOffset + i, array[i]);
            }
            offsets.setItem(bufferElement, (long) bufferOffset);
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            if ((bufferElement + 1) > maxBufferElements) {}
            buffer.setItem(bufferElement, record.getByte(ordinal));
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          break;
        }
      case TILEDB_UINT8:
      case TILEDB_INT16:
        {
          if (isArray) {
            short[] array = record.getArray(ordinal).toShortArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            if ((bufferOffset + array.length) > maxBufferElements) {
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.setItem(bufferOffset + i, array[i]);
            }
            offsets.setItem(bufferElement, (long) bufferOffset);
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;

          } else {
            buffer.setItem(bufferElement, record.getShort(ordinal));
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          break;
        }
      case TILEDB_UINT16:
      case TILEDB_INT32:
        {
          if (isArray) {
            int[] array = record.getArray(ordinal).toIntArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            if ((bufferOffset + array.length) > maxBufferElements) {
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.setItem(bufferOffset + i, array[i]);
            }
            offsets.setItem(bufferElement, (long) bufferOffset);
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.setItem(bufferElement, record.getInt(ordinal));
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          break;
        }
      case TILEDB_UINT32:
      case TILEDB_UINT64:
      case TILEDB_INT64:
        {
          if (isArray) {
            long[] array = record.getArray(ordinal).toLongArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            if ((bufferOffset + array.length) > maxBufferElements) {
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.setItem(bufferOffset + i, array[i]);
            }
            offsets.setItem(bufferElement, (long) bufferOffset);
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.setItem(bufferElement, record.getLong(ordinal));
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          break;
        }
      case TILEDB_FLOAT32:
        {
          if (isArray) {
            float[] array = record.getArray(ordinal).toFloatArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            if ((bufferOffset + array.length) > maxBufferElements) {
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.setItem(bufferOffset + i, array[i]);
            }
            offsets.setItem(bufferElement, (long) bufferOffset);
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.setItem(bufferElement, record.getFloat(ordinal));
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          break;
        }
      case TILEDB_FLOAT64:
        {
          if (isArray) {
            double[] array = record.getArray(ordinal).toDoubleArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            if ((bufferOffset + array.length) > maxBufferElements) {
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.setItem(bufferOffset + i, array[i]);
            }
            offsets.setItem(bufferElement, (long) bufferOffset);
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.setItem(bufferElement, record.getDouble(ordinal));
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          break;
        }
      case TILEDB_CHAR:
      case TILEDB_STRING_ASCII:
      case TILEDB_STRING_UTF8:
        {
          String val = record.getString(ordinal);
          int bytesLen = val.getBytes().length;
          int bufferOffset = nativeArrayBufferElements[bufferIdx];
          if ((bufferOffset + bytesLen) > maxBufferElements) {
            this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
            return true;
          }
          buffer.setItem(bufferOffset, val);
          offsets.setItem(bufferElement, (long) bufferOffset);
          nativeArrayOffsetElements[bufferIdx] += 1;
          nativeArrayBufferElements[bufferIdx] += bytesLen;
          break;
        }
        // Handle spark date fields
      case TILEDB_DATETIME_DAY:
        {
          if (isArray) {
            int[] array = record.getArray(ordinal).toIntArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            if ((bufferOffset + array.length) > maxBufferElements) {
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.setItem(bufferOffset + i, ((Integer) array[i]).longValue());
            }
            offsets.setItem(bufferElement, (long) bufferOffset);
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.setItem(bufferElement, ((Integer) record.getInt(ordinal)).longValue());
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          break;
        }
        // Handle spark timestamp fields
      case TILEDB_DATETIME_MS:
        {
          if (isArray) {
            long[] array = record.getArray(ordinal).toLongArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            if ((bufferOffset + array.length) > maxBufferElements) {
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.setItem(bufferOffset + i, array[i]);
            }
            offsets.setItem(bufferElement, (long) bufferOffset);
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.setItem(bufferElement, record.getLong(ordinal));
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          break;
        }
      default:
        this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
        throw new TileDBError("Unimplemented attribute type for Spark writes: " + dtype);
    }
    this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
    return false;
  }

  @Override
  public void write(InternalRow record) throws IOException {
    this.metricsUpdater.startTimer(queryWriteRowTimerName);
    try {
      for (int flushAttempts = 0; flushAttempts < 2; flushAttempts++) {
        boolean retryAfterFlush = false;
        for (int ordinal = 0; ordinal < record.numFields(); ordinal++) {
          int buffIdx = bufferIndex[ordinal];
          if (buffIdx < nDims) {
            int dimIdx = buffIdx - 0;
            retryAfterFlush = bufferDimensionValue(dimIdx, record, ordinal);
          } else {
            int attrIdx = buffIdx - nDims;
            retryAfterFlush = bufferAttributeValue(attrIdx, record, ordinal);
          }
          if (retryAfterFlush) {
            // don't write any more parts of the record
            break;
          }
        }
        if (!retryAfterFlush) {
          // record written
          break;
        }
        if (nRecordsBuffered == 0 || flushAttempts == 1) {
          // nothing can fit abort, buffers are not big enough to hold varlen data for write
          throw new TileDBError(
              "Allocated buffer sizes are too small to write Spark varlen data, increase max buffer size");
        }
        if (nRecordsBuffered > 0 && flushAttempts == 0) {
          // some prev records were written but one of current varlen values exceeded the max size
          // flush and reset, trying again
          flushBuffers();
          resetWriteQueryAndBuffers();
        }
      }
      nRecordsBuffered++;
    } catch (TileDBError err) {
      this.metricsUpdater.finish(queryWriteRowTimerName);
      throw new IOException(err.getMessage());
    }
    this.metricsUpdater.finish(queryWriteRowTimerName);
  }

  private void flushBuffers() throws TileDBError {
    this.metricsUpdater.startTimer(queryWriteFlushBuffersTimerName);
    long buffersInBytes = 0;
    query.setBufferElements(Constants.TILEDB_COORDS, nRecordsBuffered * nDims);
    // Calculate bytes we are writing for metrics starting with dimension
    buffersInBytes += nRecordsBuffered * nDims * bufferDatatypes[nDims - 1].getNativeSize();
    for (int i = nDims; i < bufferNames.length; i++) {
      String name = bufferNames[i];
      // Calculate bytes we are writing for metrics
      buffersInBytes += nRecordsBuffered * nDims * bufferDatatypes[i].getNativeSize();

      boolean isVar = (bufferValNum[i] == Constants.TILEDB_VAR_NUM);
      if (isVar) {
        query.setBufferElements(name, nativeArrayOffsetElements[i], nativeArrayBufferElements[i]);
      } else {
        query.setBufferElements(name, nativeArrayBufferElements[i]);
      }
    }
    QueryStatus status = query.submit();
    if (status != QueryStatus.TILEDB_COMPLETED) {
      this.metricsUpdater.finish(queryWriteFlushBuffersTimerName);
      throw new TileDBError("Query write error: " + status);
    }

    this.metricsUpdater.appendTaskMetrics(nRecordsBuffered, buffersInBytes);
    this.metricsUpdater.finish(queryWriteFlushBuffersTimerName);
  }

  private void closeTileDBResources() {
    this.metricsUpdater.startTimer(queryWriteCloseTileDBResourcesTimerName);
    query.close();
    array.close();
    ctx.close();
    this.metricsUpdater.finish(queryWriteCloseTileDBResourcesTimerName);
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    this.metricsUpdater.startTimer(queryWriteCommitTimerName);
    try {
      // flush remaining records
      if (nRecordsBuffered >= 1) {
        flushBuffers();
      }
    } catch (TileDBError err) {
      this.metricsUpdater.finish(queryWriteCommitTimerName);
      this.metricsUpdater.finish(queryWriteTimerName);
      throw new IOException(err.getMessage());
    }

    this.closeTileDBResources();
    this.metricsUpdater.finish(queryWriteCommitTimerName);
    double duration = this.metricsUpdater.finish(queryWriteTimerName) / 1000000000d;
    log.debug("duration of write-to-commit " + task.toString() + " : " + duration + "s");
    return null;
  }

  @Override
  public void abort() throws IOException {
    // clean up buffered resources
    closeTileDBResources();
    this.metricsUpdater.finish(queryWriteTimerName);
  };
}
