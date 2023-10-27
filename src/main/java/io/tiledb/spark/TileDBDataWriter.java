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
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.metrics.TileDBWriteMetricsUpdater;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TileDBDataWriter implements DataWriter<InternalRow> {

  static Logger log = Logger.getLogger(TileDBDataWriter.class.getName());

  private final TileDBWriteMetricsUpdater metricsUpdater;
  private final TaskContext task;
  private String uri;
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
  private boolean[] bufferNullable;

  private short[][] nativeArrayValidityByteMap;
  private long[][] javaArrayOffsetBuffers;
  private JavaArray[] javaArrayBuffers;
  private int[] bufferSizes;

  private int[] nativeArrayOffsetElements;
  private int[] nativeArrayBufferElements;
  private long writeBufferSize;
  private int nRecordsBuffered;

  private static final OffsetDateTime zeroDateTime =
      new Timestamp(0).toLocalDateTime().atOffset(ZoneOffset.UTC);

  public TileDBDataWriter(String uri, StructType schema, TileDBDataSourceOptions options) {
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
    bufferNullable = new boolean[nFields];
    bufferNames = new String[nFields];
    bufferValNum = new long[nFields];
    bufferDatatypes = new Datatype[nFields];
    nativeArrayValidityByteMap = new short[nFields][];
    javaArrayOffsetBuffers = new long[nFields][];
    nativeArrayOffsetElements = new int[nFields];
    javaArrayBuffers = new JavaArray[nFields];
    nativeArrayBufferElements = new int[nFields];

    try {
      ctx = new Context(options.getTileDBConfigMap(false));
      array = new Array(ctx, uri.toString(), QueryType.TILEDB_WRITE);
      try (ArraySchema arraySchema = array.getSchema()) {
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
                  bufferValNum[i] = dim.getCellValNum();
                  bufferNullable[i] = false;
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
                bufferNullable[bufferIdx] = attribute.getNullable();
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
      List<String> columnNames = new ArrayList<>();
      for (int i = 0; i < arraySchema.getDomain().getNDim(); i++)
        columnNames.add(arraySchema.getDomain().getDimension(i).getName());
      for (int i = 0; i < arraySchema.getAttributeNum(); i++)
        columnNames.add(arraySchema.getAttribute(i).getName());

      bufferSizes = new int[columnNames.size()];

      for (String attributeName : columnNames) {
        boolean isVar;
        boolean nullable = false;
        Datatype datatype;

        if (arraySchema.hasAttribute(attributeName)) {
          try (Attribute attr = arraySchema.getAttribute(attributeName)) {
            nullable = attr.getNullable();
            isVar = attr.isVar();
            datatype = attr.getType();
          }
        } else {
          Dimension dimension = arraySchema.getDomain().getDimension(attributeName);
          isVar = dimension.isVar();
          datatype = dimension.getType();
        }

        if (isVar) {
          int numOffsets =
              Math.toIntExact(writeBufferSize / Datatype.TILEDB_UINT64.getNativeSize());
          javaArrayOffsetBuffers[bufferIdx] = new long[numOffsets];
          nativeArrayOffsetElements[bufferIdx] = 0;

          int numElements = Math.toIntExact(writeBufferSize / datatype.getNativeSize());
          javaArrayBuffers[bufferIdx] = new JavaArray(datatype, numElements);
          bufferSizes[bufferIdx] = numElements;
          nativeArrayBufferElements[bufferIdx] = 0;
          if (nullable) {
            nativeArrayValidityByteMap[bufferIdx] = new short[numElements];
            Arrays.fill(nativeArrayValidityByteMap[bufferIdx], (short) 1);
          }
        } else {
          int numElements = Math.toIntExact(writeBufferSize / datatype.getNativeSize());
          javaArrayBuffers[bufferIdx] = new JavaArray(datatype, numElements);
          bufferSizes[bufferIdx] = numElements;
          nativeArrayBufferElements[bufferIdx] = 0;
          if (nullable) {
            nativeArrayValidityByteMap[bufferIdx] = new short[numElements];
            Arrays.fill(nativeArrayValidityByteMap[bufferIdx], (short) 1);
          }
        }
        ++bufferIdx;
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
    int bufferElements = nRecordsBuffered;
    return writeRecordToBuffer(attrIdx, bufferElements, record, ordinal);
  }

  private boolean writeRecordToBuffer(
      int bufferIdx, int bufferElement, InternalRow record, int ordinal) throws TileDBError {
    boolean isNull = false;
    OffsetDateTime dt;
    long value;
    this.metricsUpdater.startTimer(queryWriteRecordToBufferTimerName);
    Datatype dtype = bufferDatatypes[bufferIdx];
    JavaArray buffer = javaArrayBuffers[bufferIdx];
    long[] offsets = javaArrayOffsetBuffers[bufferIdx];
    short[] validityByteMap = nativeArrayValidityByteMap[bufferIdx];

    boolean isArray = bufferValNum[bufferIdx] > 1l;
    int maxBufferElements = bufferSizes[bufferIdx];
    if (bufferElement >= maxBufferElements) {
      this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
      return true;
    }
    if (isArray) {
      // rare, would have to be a repeat of zero sized values
      int maxOffsetElements = offsets.length;
      if (bufferElement >= maxOffsetElements) {
        this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
        return true;
      }
    }

    if (record.isNullAt(ordinal)) {
      validityByteMap[bufferElement] = 0;
      isNull = true;
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
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
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
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
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
        // Handle spark timestamp fields
      case TILEDB_DATETIME_US:
        {
          if (isArray) {
            long[] array = record.getArray(ordinal).toLongArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            if ((bufferOffset + array.length) > maxBufferElements) {
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
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
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
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
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
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
          String val = " "; // necessary if val is null
          if (!isNull) val = record.getString(ordinal);
          int bytesLen = val.getBytes().length;
          int bufferOffset = nativeArrayBufferElements[bufferIdx];
          if ((bufferOffset + bytesLen) > maxBufferElements) {
            this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
            return true;
          }
          buffer.set(bufferOffset, val.getBytes());
          offsets[bufferElement] = (long) bufferOffset;

          nativeArrayOffsetElements[bufferIdx] += 1;
          nativeArrayBufferElements[bufferIdx] += bytesLen;
          break;
        }
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
      case TILEDB_DATETIME_AS:
        {
          if (isArray) {
            long[] array = record.getArray(ordinal).toLongArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            if ((bufferOffset + array.length) > maxBufferElements) {
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.set(bufferOffset + i, array[i] * 1000000000000L);
            }
            offsets[bufferElement] = (long) bufferOffset;
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.set(bufferElement, record.getLong(ordinal) * 1000000000000L);
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          break;
        }
      case TILEDB_DATETIME_FS:
        {
          if (isArray) {
            long[] array = record.getArray(ordinal).toLongArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            if ((bufferOffset + array.length) > maxBufferElements) {
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              buffer.set(bufferOffset + i, array[i] * 1000000000);
            }
            offsets[bufferElement] = (long) bufferOffset;
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.set(bufferElement, record.getLong(ordinal) * 1000000000);
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          break;
        }
      case TILEDB_DATETIME_PS:
        {
          {
            if (isArray) {
              long[] array = record.getArray(ordinal).toLongArray();
              int bufferOffset = nativeArrayBufferElements[bufferElement];
              if ((bufferOffset + array.length) > maxBufferElements) {
                this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
                return true;
              }
              for (int i = 0; i < array.length; i++) {
                buffer.set(bufferOffset + i, array[i] * 1000000);
              }
              offsets[bufferElement] = (long) bufferOffset;
              nativeArrayOffsetElements[bufferIdx] += 1;
              nativeArrayBufferElements[bufferIdx] += array.length;
            } else {
              buffer.set(bufferElement, record.getLong(ordinal) * 1000000);
              nativeArrayBufferElements[bufferIdx] += 1;
            }
            break;
          }
        }
      case TILEDB_DATETIME_NS:
        {
          {
            if (isArray) {
              long[] array = record.getArray(ordinal).toLongArray();
              int bufferOffset = nativeArrayBufferElements[bufferElement];
              if ((bufferOffset + array.length) > maxBufferElements) {
                this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
                return true;
              }
              for (int i = 0; i < array.length; i++) {
                buffer.set(bufferOffset + i, array[i] * 1000);
              }
              offsets[bufferElement] = (long) bufferOffset;
              nativeArrayOffsetElements[bufferIdx] += 1;
              nativeArrayBufferElements[bufferIdx] += array.length;
            } else {
              buffer.set(bufferElement, record.getLong(ordinal) * 1000);
              nativeArrayBufferElements[bufferIdx] += 1;
            }
            break;
          }
        }
      case TILEDB_DATETIME_MS:
        {
          {
            if (isArray) {
              long[] array = record.getArray(ordinal).toLongArray();
              int bufferOffset = nativeArrayBufferElements[bufferElement];
              if ((bufferOffset + array.length) > maxBufferElements) {
                this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
                return true;
              }
              for (int i = 0; i < array.length; i++) {
                buffer.set(bufferOffset + i, array[i] / 1000);
              }
              offsets[bufferElement] = (long) bufferOffset;
              nativeArrayOffsetElements[bufferIdx] += 1;
              nativeArrayBufferElements[bufferIdx] += array.length;
            } else {
              buffer.set(bufferElement, record.getLong(ordinal) / 1000);
              nativeArrayBufferElements[bufferIdx] += 1;
            }
            break;
          }
        }
      case TILEDB_DATETIME_SEC:
        {
          {
            if (isArray) {
              long[] array = record.getArray(ordinal).toLongArray();
              int bufferOffset = nativeArrayBufferElements[bufferElement];
              if ((bufferOffset + array.length) > maxBufferElements) {
                this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
                return true;
              }
              for (int i = 0; i < array.length; i++) {
                buffer.set(bufferOffset + i, array[i] / 1000000);
              }
              offsets[bufferElement] = (long) bufferOffset;
              nativeArrayOffsetElements[bufferIdx] += 1;
              nativeArrayBufferElements[bufferIdx] += array.length;
            } else {
              buffer.set(bufferElement, record.getLong(ordinal) / 1000000);
              nativeArrayBufferElements[bufferIdx] += 1;
            }
            break;
          }
        }
      case TILEDB_DATETIME_MIN:
        {
          {
            if (isArray) {
              long[] array = record.getArray(ordinal).toLongArray();
              int bufferOffset = nativeArrayBufferElements[bufferElement];
              if ((bufferOffset + array.length) > maxBufferElements) {
                this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
                return true;
              }
              for (int i = 0; i < array.length; i++) {
                buffer.set(bufferOffset + i, array[i] / (60 * 1000000));
              }
              offsets[bufferElement] = (long) bufferOffset;
              nativeArrayOffsetElements[bufferIdx] += 1;
              nativeArrayBufferElements[bufferIdx] += array.length;
            } else {
              buffer.set(bufferElement, record.getLong(ordinal) / (60 * 1000000));
              nativeArrayBufferElements[bufferIdx] += 1;
            }
            break;
          }
        }
      case TILEDB_DATETIME_HR:
        {
          {
            if (isArray) {
              long[] array = record.getArray(ordinal).toLongArray();
              int bufferOffset = nativeArrayBufferElements[bufferElement];
              if ((bufferOffset + array.length) > maxBufferElements) {
                this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
                return true;
              }
              for (int i = 0; i < array.length; i++) {
                buffer.set(bufferOffset + i, array[i] / (60 * 60 * 1000000L));
              }
              offsets[bufferElement] = (long) bufferOffset;
              nativeArrayOffsetElements[bufferIdx] += 1;
              nativeArrayBufferElements[bufferIdx] += array.length;
            } else {
              buffer.set(bufferElement, record.getLong(ordinal) / (60 * 60 * 1000000L));
              nativeArrayBufferElements[bufferIdx] += 1;
            }
            break;
          }
        }
      case TILEDB_DATETIME_WEEK:
        {
          if (isArray) {
            int[] array = record.getArray(ordinal).toIntArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            if ((bufferOffset + array.length) > maxBufferElements) {
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              dt =
                  new Timestamp(record.getLong(ordinal) / 1000)
                      .toLocalDateTime()
                      .atOffset(ZoneOffset.UTC);
              value = ChronoUnit.WEEKS.between(zeroDateTime, dt);
              buffer.set(bufferOffset + i, value);
            }
            offsets[bufferElement] = (long) bufferOffset;
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            dt =
                new Timestamp(record.getLong(ordinal) / 1000)
                    .toLocalDateTime()
                    .atOffset(ZoneOffset.UTC);
            value = ChronoUnit.WEEKS.between(zeroDateTime, dt);
            buffer.set(bufferElement, value);
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          break;
        }
      case TILEDB_DATETIME_MONTH:
        {
          if (isArray) {
            int[] array = record.getArray(ordinal).toIntArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            if ((bufferOffset + array.length) > maxBufferElements) {
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              dt =
                  new Timestamp(record.getLong(ordinal) / 1000)
                      .toLocalDateTime()
                      .atOffset(ZoneOffset.UTC);
              value = ChronoUnit.MONTHS.between(zeroDateTime, dt);
              buffer.set(bufferOffset + i, value);
            }
            offsets[bufferElement] = (long) bufferOffset;
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            dt =
                new Timestamp(record.getLong(ordinal) / 1000)
                    .toLocalDateTime()
                    .atOffset(ZoneOffset.UTC);
            value = ChronoUnit.MONTHS.between(zeroDateTime, dt);
            buffer.set(bufferElement, value);
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          break;
        }
      case TILEDB_DATETIME_YEAR:
        {
          if (isArray) {
            int[] array = record.getArray(ordinal).toIntArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            if ((bufferOffset + array.length) > maxBufferElements) {
              this.metricsUpdater.finish(queryWriteRecordToBufferTimerName);
              return true;
            }
            for (int i = 0; i < array.length; i++) {
              dt =
                  new Timestamp(record.getLong(ordinal) / 1000)
                      .toLocalDateTime()
                      .atOffset(ZoneOffset.UTC);
              value = ChronoUnit.YEARS.between(zeroDateTime, dt);
              buffer.set(bufferOffset + i, value);
            }
            offsets[bufferElement] = (long) bufferOffset;
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            dt =
                new Timestamp(record.getLong(ordinal) / 1000)
                    .toLocalDateTime()
                    .atOffset(ZoneOffset.UTC);
            value = ChronoUnit.YEARS.between(zeroDateTime, dt);
            buffer.set(bufferElement, value);
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
          retryAfterFlush = bufferAttributeValue(buffIdx, record, ordinal);
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
    int dimSizes = 0;

    for (Dimension dimension : array.getSchema().getDomain().getDimensions())
      dimSizes += dimension.getType().getNativeSize();

    buffersInBytes += nRecordsBuffered * nDims * dimSizes;
    // Calculate bytes we are writing for metrics starting with dimension
    for (int i = 0; i < bufferNames.length; i++) {
      String name = bufferNames[i];
      // Calculate bytes we are writing for metrics
      buffersInBytes += nRecordsBuffered * nDims * bufferDatatypes[i].getNativeSize();
      boolean isVar = (bufferValNum[i] == Constants.TILEDB_VAR_NUM);
      // code referring to nullable attributes
      boolean nullable = bufferNullable[i];

      Datatype bufferDataType = javaArrayBuffers[i].getDataType();
      Object bufferData =
          javaArrayBuffers[i].getDataType() == Datatype.TILEDB_CHAR
                  || javaArrayBuffers[i].getDataType() == Datatype.TILEDB_STRING_ASCII
              ? new String((byte[]) javaArrayBuffers[i].get())
              : javaArrayBuffers[i].get();

      query.setDataBuffer(
          name, new NativeArray(ctx, bufferData, bufferDataType, nativeArrayBufferElements[i]));

      if (isVar)
        query.setOffsetsBuffer(
            name,
            new NativeArray(
                ctx,
                javaArrayOffsetBuffers[i],
                Datatype.TILEDB_UINT64,
                nativeArrayOffsetElements[i]));
      if (nullable)
        query.setValidityBuffer(
            name,
            new NativeArray(
                ctx,
                nativeArrayValidityByteMap[i],
                Datatype.TILEDB_UINT8,
                nativeArrayBufferElements[i]));
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
    try {
      ctx.close();
    } catch (TileDBError e) {
      // do nothing
    }
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
  }

  @Override
  public void close() throws IOException {
    closeTileDBResources();
  }
}
