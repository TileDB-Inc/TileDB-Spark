package io.tiledb.spark;

import io.tiledb.java.api.*;

import java.io.File;
import java.io.FileWriter;
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

import static org.apache.spark.metrics.TileDBMetricsSource.*;

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

  private long[][] javaArrayOffsetBuffers;
  private JavaArray[] javaArrayBuffers;
  private int[] bufferSizes;

  private int[] nativeArrayOffsetElements;
  private int[] nativeArrayBufferElements;
  private long writeBufferSize;
  private int nRecordsBuffered;

  private double writeTime = 0.0;
  private double writeRecordToBufferTime = 0.0;

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
          long durationNanos = metricsUpdater.finish(queryWriteTaskTimerName);
          double duration = durationNanos / 1000000000d;
          log.info("duration of write task " + task.taskAttemptId() + " : " + duration + "s");

          try {
            File f = new File("/Users/victor/Dev/sparktiledb-playground/perf_metrics");
            FileWriter fw = new FileWriter(f, true);
            String s1 = String.format("write,%d,%f\n", task.taskAttemptId(), writeTime);
            String s2 = String.format("writeRecord,%d,%f\n", task.taskAttemptId(), writeRecordToBufferTime);
            fw.append(s1);
            fw.append(s2);
            System.out.println("Appended: "+s1);
            System.out.println("Appended: "+s2);
            fw.close();
          }
          catch (IOException ioe) {
            System.out.println("error: "+ioe);
          }
        });

    // mapping of fields to dimension / attributes in TileDB schema
    StructField[] sparkSchemaFields = schema.fields();
    int nFields = sparkSchemaFields.length;
    bufferIndex = new int[nFields];

    bufferNames = new String[nFields];
    bufferValNum = new long[nFields];
    bufferDatatypes = new Datatype[nFields];
    javaArrayOffsetBuffers = new long[nFields][];
    nativeArrayOffsetElements = new int[nFields];
    javaArrayBuffers = new JavaArray[nFields];
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
        bufferSizes = new int[arraySchema.getAttributes().size() + nDims];
        int numElements = Math.toIntExact(writeBufferSize / domain.getType().getNativeSize());
        javaArrayBuffers[bufferIdx] = new JavaArray(domain.getType(), numElements);
        bufferSizes[bufferIdx] = numElements;
        nativeArrayBufferElements[bufferIdx] = numElements;
        // we just skip over all dims for now (special case zipped coordinates)
        bufferIdx += nDims;
      }
      for (int i = 0; i < arraySchema.getAttributeNum(); i++) {
        try (Attribute attr = arraySchema.getAttribute(i)) {
          String attrName = attr.getName();
          if (attr.isVar()) {
            int numOffsets =
                Math.toIntExact(writeBufferSize / Datatype.TILEDB_UINT64.getNativeSize());
            javaArrayOffsetBuffers[bufferIdx] = new long[numOffsets];
            nativeArrayOffsetElements[bufferIdx] = 0;

            int numElements = Math.toIntExact(writeBufferSize / attr.getType().getNativeSize());
            javaArrayBuffers[bufferIdx] = new JavaArray(attr.getType(), numElements);
            bufferSizes[bufferIdx] = numElements;
            nativeArrayBufferElements[bufferIdx] = 0;

            bufferIdx += 1;
          } else {
            int numElements = Math.toIntExact(writeBufferSize / attr.getType().getNativeSize());
            javaArrayBuffers[bufferIdx] = new JavaArray(attr.getType(), numElements);
            bufferSizes[bufferIdx] = numElements;
            nativeArrayBufferElements[bufferIdx] = 0;
            bufferIdx += 1;
          }
        }
      }
    }
    nRecordsBuffered = 0;
    this.metricsUpdater.finish(queryResetWriteQueryAndBuffersTimerName);
    return;
  }

  class JavaArray {
    private Object array;
    private Datatype dataType;

    public JavaArray(Datatype dt, int size) throws TileDBError {
      this.dataType = dt;

      Class c = Types.getJavaType(dt);

      if (c == Integer.class)
        this.array = new int[size];
      else if (c == Long.class)
        this.array = new long[size];
      else if (c == Double.class)
        this.array = new double[size];
      else if (c == Float.class)
        this.array = new float[size];
      else if (c == Short.class)
        this.array = new float[size];
      else if (c == String.class) {
        this.array = new byte[size];
      }
    }

    public Object get() {
      if (this.dataType.equals(Datatype.TILEDB_CHAR)) {
        return new String((byte[])array);
      }
      return this.array;
    }

    public void set(int position, int o) {
      ((int[])array)[position] = o;
    }

    public void set(int position, long o) {
      ((long[])array)[position] = o;
    }

    public void set(int position, float o) {
      ((float[])array)[position] = o;
    }

    public void set(int position, double o) {
      ((double[])array)[position] = o;
    }

    public void set(int position, short o) {
      ((short[])array)[position] = o;
    }

    public void set(int position, byte[] o) {
      int curr = position;
      for (byte b : o) {
        ((byte[])array)[curr] = b;
        ++curr;
      }
    }

    public Datatype getDataType() { return this.dataType; }
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

    JavaArray buffer = javaArrayBuffers[bufferIdx];
    long[] offsets = javaArrayOffsetBuffers[bufferIdx];

    boolean isArray = bufferValNum[bufferIdx] > 1l;
    int maxBufferElements = bufferSizes[bufferIdx];
    if (bufferElement >= maxBufferElements) {
      double tmp = this.metricsUpdater.finish(queryWriteRecordToBufferTimerName) / 1000000000d ;
      return true;
    }
    if (isArray) {
      // rare, would have to be a repeat of zero sized values
      int maxOffsetElements = offsets.length;
      if (bufferElement >= maxOffsetElements) {
        double tmp = this.metricsUpdater.finish(queryWriteRecordToBufferTimerName) / 1000000000d ;
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
              double tmp = this.metricsUpdater.finish(queryWriteRecordToBufferTimerName) / 1000000000d ;
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.set(bufferOffset + i, array[i]);
            }
            offsets[bufferElement] = (long) bufferOffset;
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            if ((bufferElement + 1) > maxBufferElements) {}
            buffer.set(bufferElement, record.getByte(ordinal));
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
              double tmp = this.metricsUpdater.finish(queryWriteRecordToBufferTimerName) / 1000000000d; writeRecordToBufferTime += tmp;
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.set(bufferOffset + i, array[i]);
            }
            offsets[bufferElement] = (long) bufferOffset;
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;

          } else {
            buffer.set(bufferElement, record.getShort(ordinal));
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
              double tmp = this.metricsUpdater.finish(queryWriteRecordToBufferTimerName) / 1000000000d; writeRecordToBufferTime += tmp;
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.set(bufferOffset + i, array[i]);
            }
            offsets[bufferElement] = (long) bufferOffset;
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.set(bufferElement, record.getInt(ordinal));
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
              double tmp = this.metricsUpdater.finish(queryWriteRecordToBufferTimerName) / 1000000000d; writeRecordToBufferTime += tmp;
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.set(bufferOffset + i, array[i]);
            }
            offsets[bufferElement] = (long) bufferOffset;
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.set(bufferElement, record.getLong(ordinal));
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
              double tmp = this.metricsUpdater.finish(queryWriteRecordToBufferTimerName) / 1000000000d; writeRecordToBufferTime += tmp;
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.set(bufferOffset + i, array[i]);
            }
            offsets[bufferElement] = (long) bufferOffset;
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.set(bufferElement, record.getFloat(ordinal));
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
              double tmp = this.metricsUpdater.finish(queryWriteRecordToBufferTimerName) / 1000000000d; writeRecordToBufferTime += tmp;
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.set(bufferOffset + i, array[i]);
            }
            offsets[bufferElement] = (long) bufferOffset;
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.set(bufferElement, record.getDouble(ordinal));
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
            double tmp = this.metricsUpdater.finish(queryWriteRecordToBufferTimerName) / 1000000000d; writeRecordToBufferTime += tmp;
            return true;
          }

          buffer.set(bufferOffset, val.getBytes());
          offsets[bufferElement] = (long) bufferOffset;

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
              double tmp = this.metricsUpdater.finish(queryWriteRecordToBufferTimerName) / 1000000000d; writeRecordToBufferTime += tmp;
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.set(bufferOffset + i, ((Integer) array[i]).longValue());
            }
            offsets[bufferElement] = (long) bufferOffset;
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.set(bufferElement, ((Integer) record.getInt(ordinal)).longValue());
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
              double tmp = this.metricsUpdater.finish(queryWriteRecordToBufferTimerName) / 1000000000d; writeRecordToBufferTime += tmp;
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.set(bufferOffset + i, array[i]);
            }
            offsets[bufferElement] = (long) bufferOffset;
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.set(bufferElement, record.getLong(ordinal));
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          break;
        }
      default:
        double tmp = this.metricsUpdater.finish(queryWriteRecordToBufferTimerName) / 1000000000d; writeRecordToBufferTime += tmp;
        throw new TileDBError("Unimplemented attribute type for Spark writes: " + dtype);
    }
    double tmp = this.metricsUpdater.finish(queryWriteRecordToBufferTimerName) / 1000000000d; writeRecordToBufferTime += tmp;
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
    JavaArray jArray;
    long buffersInBytes = 0;

    query.setBuffer(Constants.TILEDB_COORDS, new NativeArray(ctx, javaArrayBuffers[0].get(), javaArrayBuffers[0].dataType), nRecordsBuffered * nDims);
    // Calculate bytes we are writing for metrics starting with dimension
    buffersInBytes += nRecordsBuffered * nDims * bufferDatatypes[nDims - 1].getNativeSize();
    for (int i = nDims; i < bufferNames.length; i++) {
      String name = bufferNames[i];
      // Calculate bytes we are writing for metrics
      buffersInBytes += nRecordsBuffered * nDims * bufferDatatypes[i].getNativeSize();

      boolean isVar = (bufferValNum[i] == Constants.TILEDB_VAR_NUM);
      if (isVar) {
        query.setBuffer(
            name,
            new NativeArray(ctx, javaArrayOffsetBuffers[i], Datatype.TILEDB_UINT64),
            new NativeArray(ctx, javaArrayBuffers[i].get(), javaArrayBuffers[i].dataType),
            nativeArrayOffsetElements[i],
            nativeArrayBufferElements[i]);
      } else {
        query.setBuffer(name, new NativeArray(ctx, javaArrayBuffers[i].get(), javaArrayBuffers[i].dataType), nativeArrayBufferElements[i]);
      }
    }
    QueryStatus status = query.submit();
    if (status != QueryStatus.TILEDB_COMPLETED) {
      this.metricsUpdater.finish(queryWriteFlushBuffersTimerName);
      throw new TileDBError("Query write error: " + status);
    }

    this.metricsUpdater.appendTaskMetrics(nRecordsBuffered, buffersInBytes);
    this.writeTime += this.metricsUpdater.finish(queryWriteFlushBuffersTimerName) / 1000000000d;
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
