package io.tiledb.spark;

import io.tiledb.java.api.*;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TileDBDataWriter implements DataWriter<InternalRow> {

  private static final int DEFAULT_BATCH_SIZE = 10000;

  private URI uri;
  private StructType sparkSchema;
  private TileDBDataSourceOptions options;

  private Context ctx;
  private Array array;
  private Query query;

  private final StructField[] sparkSchemaFields;
  private final int[] bufferIndex;

  private final int nDims;
  private final String[] bufferNames;

  private final long[] bufferValNum;
  private final Datatype[] bufferDatatypes;
  private NativeArray[] nativeArrayOffsetBuffers;
  private int[] nativeArrayOffsetElements;
  private NativeArray[] nativeArrayBuffers;
  private int[] nativeArrayBufferElements;
  private int nRecordsBuffered;

  public TileDBDataWriter(URI uri, StructType schema, TileDBDataSourceOptions options) {
    this.uri = uri;
    this.sparkSchema = schema;
    this.sparkSchemaFields = schema.fields();
    this.options = options;

    // mapping of fields to dimension / attributes in TileDB schema
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

  public void resetWriteQueryAndBuffers() throws TileDBError {
    if (query != null) {
      query.close();
    }
    query = new Query(array, QueryType.TILEDB_WRITE);
    query.setLayout(Layout.TILEDB_UNORDERED);

    int bufferIdx = 0;
    try (ArraySchema arraySchema = array.getSchema()) {
      try (Domain domain = arraySchema.getDomain()) {
        NativeArray coordsBuffer =
            new NativeArray(ctx, DEFAULT_BATCH_SIZE * (int) domain.getNDim(), domain.getType());
        nativeArrayBuffers[bufferIdx] = coordsBuffer;
        nativeArrayBufferElements[bufferIdx] = 0;
        query.setBuffer(Constants.TILEDB_COORDS, coordsBuffer);
        bufferIdx += nDims;
      }
      for (int i = 0; i < arraySchema.getAttributeNum(); i++) {
        try (Attribute attr = arraySchema.getAttribute(i)) {
          String attrName = attr.getName();
          if (attr.isVar()) {
            NativeArray bufferOff =
                new NativeArray(ctx, DEFAULT_BATCH_SIZE, Datatype.TILEDB_UINT64);
            nativeArrayOffsetBuffers[bufferIdx] = bufferOff;
            nativeArrayOffsetElements[bufferIdx] = 0;
            NativeArray bufferData = new NativeArray(ctx, DEFAULT_BATCH_SIZE, attr.getType());
            nativeArrayBuffers[bufferIdx] = bufferData;
            nativeArrayBufferElements[bufferIdx] = 0;

            query.setBuffer(attrName, bufferOff, bufferData);
            bufferIdx += 1;
          } else {
            NativeArray bufferData =
                new NativeArray(
                    ctx, DEFAULT_BATCH_SIZE * (int) attr.getCellValNum(), attr.getType());
            nativeArrayBuffers[bufferIdx] = bufferData;
            nativeArrayBufferElements[bufferIdx] = 0;
            query.setBuffer(attrName, bufferData);
            bufferIdx += 1;
          }
        }
      }
    }
    nRecordsBuffered = 0;
    return;
  }

  private void bufferDimensionValue(int dimIdx, InternalRow record, int ordinal)
      throws TileDBError {
    int bufferIdx = 0;
    int bufferElements = (nRecordsBuffered * nDims) + dimIdx;
    writeRecordToBuffer(bufferIdx, bufferElements, record, ordinal);
  }

  private void bufferAttributeValue(int attrIdx, InternalRow record, int ordinal)
      throws TileDBError {
    int bufferIdx = nDims + attrIdx;
    int bufferElements = nRecordsBuffered;
    writeRecordToBuffer(bufferIdx, bufferElements, record, ordinal);
  }

  private void writeRecordToBuffer(
      int bufferIdx, int bufferElement, InternalRow record, int ordinal) throws TileDBError {
    Datatype dtype = bufferDatatypes[bufferIdx];
    boolean isArray = bufferValNum[bufferIdx] > 1l;
    NativeArray buffer = nativeArrayBuffers[bufferIdx];
    switch (dtype) {
      case TILEDB_INT8:
        {
          if (isArray) {
            byte[] array = record.getArray(ordinal).toByteArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            for (int i = 0; i < array.length; i++) {
              buffer.setItem(bufferOffset + i, array[i]);
            }
            nativeArrayOffsetBuffers[bufferIdx].setItem(bufferElement, (long) bufferOffset);
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.setItem(bufferElement, record.getByte(ordinal));
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          return;
        }
      case TILEDB_UINT8:
      case TILEDB_INT16:
        {
          if (isArray) {
            short[] array = record.getArray(ordinal).toShortArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            for (int i = 0; i < array.length; i++) {
              buffer.setItem(bufferOffset + i, array[i]);
            }
            nativeArrayOffsetBuffers[bufferIdx].setItem(bufferElement, (long) bufferOffset);
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;

          } else {
            buffer.setItem(bufferElement, record.getShort(ordinal));
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          return;
        }
      case TILEDB_UINT16:
      case TILEDB_INT32:
        {
          if (isArray) {
            int[] array = record.getArray(ordinal).toIntArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            for (int i = 0; i < array.length; i++) {
              buffer.setItem(bufferOffset + i, array[i]);
            }
            nativeArrayOffsetBuffers[bufferIdx].setItem(bufferElement, (long) bufferOffset);
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.setItem(bufferElement, record.getInt(ordinal));
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          return;
        }
      case TILEDB_UINT32:
      case TILEDB_UINT64:
      case TILEDB_INT64:
        {
          if (isArray) {
            long[] array = record.getArray(ordinal).toLongArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            for (int i = 0; i < array.length; i++) {
              buffer.setItem(bufferOffset + i, array[i]);
            }
            nativeArrayOffsetBuffers[bufferIdx].setItem(bufferElement, (long) bufferOffset);
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.setItem(bufferElement, record.getLong(ordinal));
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          return;
        }
      case TILEDB_FLOAT32:
        {
          if (isArray) {
            float[] array = record.getArray(ordinal).toFloatArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            for (int i = 0; i < array.length; i++) {
              buffer.setItem(bufferOffset + i, array[i]);
            }
            nativeArrayOffsetBuffers[bufferIdx].setItem(bufferElement, (long) bufferOffset);
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.setItem(bufferElement, record.getFloat(ordinal));
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          return;
        }
      case TILEDB_FLOAT64:
        {
          if (isArray) {
            double[] array = record.getArray(ordinal).toDoubleArray();
            int bufferOffset = nativeArrayBufferElements[bufferElement];
            for (int i = 0; i < array.length; i++) {
              buffer.setItem(bufferOffset + i, array[i]);
            }
            nativeArrayOffsetBuffers[bufferIdx].setItem(bufferElement, (long) bufferOffset);
            nativeArrayOffsetElements[bufferIdx] += 1;
            nativeArrayBufferElements[bufferIdx] += array.length;
          } else {
            buffer.setItem(bufferElement, record.getDouble(ordinal));
            nativeArrayBufferElements[bufferIdx] += 1;
          }
          return;
        }
      case TILEDB_CHAR:
      case TILEDB_STRING_ASCII:
      case TILEDB_STRING_UTF8:
        {
          String val = record.getString(ordinal);
          int bufferOffset = nativeArrayOffsetElements[bufferIdx];
          buffer.setItem(bufferOffset, val);
          nativeArrayOffsetBuffers[bufferIdx].setItem(bufferElement, (long) bufferOffset);
          nativeArrayOffsetElements[bufferIdx] += 1;
          nativeArrayBufferElements[bufferIdx] += val.getBytes().length;
          return;
        }
      default:
        throw new TileDBError("Unimplemented attribute type for Spark writes: " + dtype);
    }
  }

  @Override
  public void write(InternalRow record) throws IOException {
    try {
      for (int ordinal = 0; ordinal < record.numFields(); ordinal++) {
        int buffIdx = bufferIndex[ordinal];
        if (buffIdx < nDims) {
          int dimIdx = buffIdx - 0;
          bufferDimensionValue(dimIdx, record, ordinal);
        } else {
          int attrIdx = buffIdx - nDims;
          bufferAttributeValue(attrIdx, record, ordinal);
        }
      }
      nRecordsBuffered++;
      if (nRecordsBuffered > DEFAULT_BATCH_SIZE) {
        flushBuffers();
        resetWriteQueryAndBuffers();
      }
    } catch (TileDBError err) {
      throw new IOException(err.getMessage());
    }
  }

  private void flushBuffers() throws TileDBError {
    query.setBufferElements(Constants.TILEDB_COORDS, nRecordsBuffered * nDims);
    for (int i = nDims; i < bufferNames.length; i++) {
      String name = bufferNames[i];
      boolean isVar = (bufferValNum[i] == Constants.TILEDB_VAR_NUM);
      if (isVar) {
        query.setBufferElements(name, nativeArrayOffsetElements[i], nativeArrayBufferElements[i]);
      } else {
        query.setBufferElements(name, nativeArrayBufferElements[i]);
      }
    }
    QueryStatus status = query.submit();
    if (status != QueryStatus.TILEDB_COMPLETED) {
      throw new TileDBError("Query write error: " + status);
    }
  }

  private void closeTileDBResources() {
    query.close();
    array.close();
    ctx.close();
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    try {
      // flush remaining buffers
      flushBuffers();
    } catch (TileDBError err) {
      throw new IOException(err.getMessage());
    }
    return null;
  }

  @Override
  public void abort() throws IOException {
    closeTileDBResources();
  }
}
