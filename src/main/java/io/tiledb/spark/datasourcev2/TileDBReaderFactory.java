/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.tiledb.spark.datasourcev2;

import io.tiledb.java.api.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class TileDBReaderFactory
    implements DataReaderFactory<ColumnarBatch>, DataReader<ColumnarBatch> {
  private StructField[] attributes;
  private Object subarray;
  private boolean initilized;
  private boolean partitioning;

  private TileDBOptions options;

  private Context ctx;
  private Array array;
  private ArraySchema arraySchema;
  private Query query;

  private boolean hasNext;
  private OnHeapColumnVector[] vectors;
  private ColumnarBatch batch;

  TileDBReaderFactory(Object subarray, StructType requiredSchema, DataSourceOptions options) {
    this.subarray = subarray;
    this.attributes = requiredSchema.fields();
    this.initilized = false;
    this.options = new TileDBOptions(options);
  }

  public TileDBReaderFactory(Object subarray, StructField[] attributes, TileDBOptions options) {
    this.subarray = subarray;
    this.attributes = attributes;
    this.options = options;
  }

  public TileDBReaderFactory(
      Object subarray, StructType requiredSchema, DataSourceOptions options, boolean partitioning) {

    this(subarray, requiredSchema, options);
    this.partitioning = partitioning;
  }

  @Override
  public DataReader<ColumnarBatch> createDataReader() {
    return new TileDBReaderFactory(subarray, attributes, options);
  }

  @Override
  public boolean next() {
    if (query == null) {
      // initialize
      try {
        ctx = new Context();
        array = new Array(ctx, options.ARRAY_URI);
        arraySchema = array.getSchema();
        try (Domain domain = arraySchema.getDomain();
            NativeArray nativeSubarray = new NativeArray(ctx, subarray, domain.getType())) {
          // Compute maximum buffer elements for the query results per attribute
          HashMap<String, Pair<Long, Long>> max_sizes = array.maxBufferElements(nativeSubarray);

          // Create query
          query = new Query(array, QueryType.TILEDB_READ);
          // query.setLayout(tiledb_layout_t.TILEDB_GLOBAL_ORDER);
          query.setSubarray(nativeSubarray);

          int buffSize = (partitioning) ? options.PARTITION_SIZE : options.BATCH_SIZE;
          vectors = OnHeapColumnVector.allocateColumns(options.BATCH_SIZE, attributes);
          for (StructField field : attributes) {
            String name = field.name();
            if (!arraySchema.getAttributes().containsKey(name)) {
              // dimension column
              continue;
            }
            try (Attribute attr = arraySchema.getAttribute(name)) {
              long cellValNum = attr.getCellValNum();
              int valPerRow =
                  (int)
                      ((cellValNum == Constants.TILEDB_VAR_NUM)
                          ? max_sizes.get(name).getFirst()
                          : cellValNum);
              if (cellValNum == Constants.TILEDB_VAR_NUM) {
                query.setBuffer(
                    name,
                    new NativeArray(ctx, buffSize, Datatype.TILEDB_UINT64),
                    new NativeArray(
                        ctx,
                        buffSize * max_sizes.get(name).getSecond().intValue(),
                        attr.getType()));
              } else {
                query.setBuffer(name, new NativeArray(ctx, buffSize * valPerRow, attr.getType()));
              }
            }
          }
          query.setCoordinates(
              new NativeArray(ctx, (buffSize * (int) domain.getRank()), domain.getType()));
          batch = new ColumnarBatch(vectors);
          hasNext = true;
        }
      } catch (Exception tileDBError) {
        tileDBError.printStackTrace();
      }
    }
    if (!hasNext) {
      return false;
    }
    try {
      query.submit();
      boolean ret = hasNext;
      hasNext = query.getQueryStatus() == QueryStatus.TILEDB_INCOMPLETE;
      return ret;
    } catch (TileDBError tileDBError) {
      tileDBError.printStackTrace();
    }
    return false;
  }

  @Override
  public ColumnarBatch get() {
    try {
      int i = 0, currentSize = 0;
      if (attributes.length == 0) {
        // count
        try (Domain domain = arraySchema.getDomain();
            Dimension dim = domain.getDimension(0)) {
          currentSize = getDimensionColumn(dim.getName(), 0);
        }
      } else {
        for (StructField field : attributes) {
          currentSize = getColumnBatch(field, i);
          i++;
        }
      }
      batch.setNumRows(currentSize);
    } catch (TileDBError tileDBError) {
      tileDBError.printStackTrace();
    }
    return batch;
  }

  private int getColumnBatch(StructField field, int index) throws TileDBError {
    String name = field.name();
    if (!arraySchema.getAttributes().containsKey(name)) {
      // dimension column
      return getDimensionColumn(name, index);
    } else {
      // attribute column
      return getAttributeColumn(name, index);
    }
  }

  private int getAttributeColumn(String name, int index) throws TileDBError {
    try (Attribute attribute = arraySchema.getAttribute(name)) {
      if (attribute.getCellValNum() > 1) {
        // variable length values added as arrays
        return getVarLengthAttributeColumn(name, attribute, index);
      } else {
        // one value per cell
        return getSingleValueAttributeColumn(name, attribute, index);
      }
    }
  }

  private int getSingleValueAttributeColumn(String name, Attribute attribute, int index)
      throws TileDBError {
    int numValues = 0;
    int bufferLength = 0;
    switch (attribute.getType()) {
      case TILEDB_FLOAT32:
        {
          float[] buff = (float[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          vectors[index].reset();
          vectors[index].putFloats(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_FLOAT64:
        {
          double[] buff = (double[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          vectors[index].reset();
          vectors[index].putDoubles(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_INT8:
        {
          byte[] buff = (byte[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          vectors[index].reset();
          vectors[index].putBytes(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_INT16:
      case TILEDB_UINT8:
        {
          short[] buff = (short[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          vectors[index].reset();
          vectors[index].putShorts(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_INT32:
      case TILEDB_UINT16:
        {
          int[] buff = (int[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          vectors[index].reset();
          vectors[index].putInts(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_INT64:
      case TILEDB_UINT32:
      case TILEDB_UINT64:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          vectors[index].reset();
          vectors[index].putLongs(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_CHAR:
        {
          byte[] buff = (byte[]) query.getBuffer(name);
          bufferLength = buff.length;
          numValues = bufferLength;
          vectors[index].reset();
          vectors[index].putBytes(0, bufferLength, buff, 0);
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
    vectors[index].reset();
    vectors[index].getChild(0).reset();
    switch (attribute.getType()) {
      case TILEDB_FLOAT32:
        {
          float[] buff = (float[]) query.getBuffer(name);
          bufferLength = buff.length;
          vectors[index].getChild(0).reserve(bufferLength);
          vectors[index].getChild(0).putFloats(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_FLOAT64:
        {
          double[] buff = (double[]) query.getBuffer(name);
          bufferLength = buff.length;
          vectors[index].getChild(0).reserve(bufferLength);
          vectors[index].getChild(0).putDoubles(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_INT8:
        {
          byte[] buff = (byte[]) query.getBuffer(name);
          bufferLength = buff.length;
          vectors[index].getChild(0).reserve(bufferLength);
          vectors[index].getChild(0).putBytes(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_INT16:
      case TILEDB_UINT8:
        {
          short[] buff = (short[]) query.getBuffer(name);
          bufferLength = buff.length;
          vectors[index].getChild(0).reserve(bufferLength);
          vectors[index].getChild(0).putShorts(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_INT32:
      case TILEDB_UINT16:
        {
          int[] buff = (int[]) query.getBuffer(name);
          bufferLength = buff.length;
          vectors[index].getChild(0).reserve(bufferLength);
          vectors[index].getChild(0).putInts(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_INT64:
      case TILEDB_UINT32:
      case TILEDB_UINT64:
        {
          long[] buff = (long[]) query.getBuffer(name);
          bufferLength = buff.length;
          vectors[index].getChild(0).reserve(bufferLength);
          vectors[index].getChild(0).putLongs(0, bufferLength, buff, 0);
          break;
        }
      case TILEDB_CHAR:
        {
          byte[] buff = (byte[]) query.getBuffer(name);
          bufferLength = buff.length;
          vectors[index].getChild(0).reserve(bufferLength);
          vectors[index].getChild(0).putBytes(0, bufferLength, buff, 0);
          break;
        }
      default:
        {
          throw new TileDBError("Not supported getDomain getType " + attribute.getType());
        }
    }
    if (attribute.isVar()) {
      // add var length offsets
      long[] offsets = (long[]) query.getVarBuffer(name);
      int typeSize = attribute.getType().getNativeSize();
      for (int j = 0; j < offsets.length; j++) {
        int length =
            (j == offsets.length - 1)
                ? bufferLength * typeSize - (int) offsets[j]
                : (int) offsets[j + 1] - (int) offsets[j];
        vectors[index].putArray(j, ((int) offsets[j]) / typeSize, length / typeSize);
      }
      numValues = offsets.length;
    } else {
      int cellNum = (int) attribute.getCellValNum();
      numValues = bufferLength / cellNum;
      for (int j = 0; j < numValues; j++) {
        vectors[index].putArray(j, cellNum * j, cellNum);
      }
    }
    return numValues;
  }

  private int getDimensionColumn(String name, int index) throws TileDBError {
    int numValues = 0;
    int bufferLength = 0;
    try (Domain domain = arraySchema.getDomain()) {
      switch (domain.getType()) {
        case TILEDB_FLOAT32:
          {
            float[] coords = (float[]) query.getCoordinates();
            int dimIdx = 0;
            for (; dimIdx < domain.getRank(); dimIdx++) {
              try (Dimension dim = domain.getDimension(dimIdx)) {
                if (dim.getName().equals(name)) {
                  break;
                }
              }
            }
            bufferLength = coords.length;
            numValues = bufferLength / 2;
            vectors[index].reset();
            for (int i = dimIdx; i < bufferLength; i += 2) {
              vectors[index].putFloat(i / 2, coords[i]);
            }
            break;
          }
        case TILEDB_FLOAT64:
          {
            double[] coords = (double[]) query.getCoordinates();
            int dimIdx = 0;
            for (; dimIdx < domain.getRank(); dimIdx++) {
              try (Dimension dim = domain.getDimension(dimIdx)) {
                if (dim.getName().equals(name)) {
                  break;
                }
              }
            }
            bufferLength = coords.length;
            numValues = bufferLength / 2;
            vectors[index].reset();
            for (int i = dimIdx; i < bufferLength; i += 2) {
              vectors[index].putDouble(i / 2, coords[i]);
            }
            break;
          }
        case TILEDB_INT8:
          {
            byte[] coords = (byte[]) query.getCoordinates();
            int dimIdx = 0;
            for (; dimIdx < domain.getRank(); dimIdx++) {
              try (Dimension dim = domain.getDimension(dimIdx)) {
                if (dim.getName().equals(name)) {
                  break;
                }
              }
            }
            bufferLength = coords.length;
            numValues = bufferLength / 2;
            vectors[index].reset();
            for (int i = dimIdx; i < bufferLength; i += 2) {
              vectors[index].putByte(i / 2, coords[i]);
            }
            break;
          }
        case TILEDB_INT16:
        case TILEDB_UINT8:
          {
            short[] coords = (short[]) query.getCoordinates();
            int dimIdx = 0;
            for (; dimIdx < domain.getRank(); dimIdx++) {
              try (Dimension dim = domain.getDimension(dimIdx)) {
                if (dim.getName().equals(name)) {
                  break;
                }
              }
            }
            bufferLength = coords.length;
            numValues = bufferLength / 2;
            vectors[index].reset();
            for (int i = dimIdx; i < bufferLength; i += 2) {
              vectors[index].putShort(i / 2, coords[i]);
            }
            break;
          }
        case TILEDB_UINT16:
        case TILEDB_INT32:
          {
            int[] coords = (int[]) query.getCoordinates();
            int dimIdx = 0;
            for (; dimIdx < domain.getRank(); dimIdx++) {
              try (Dimension dim = domain.getDimension(dimIdx)) {
                if (dim.getName().equals(name)) {
                  break;
                }
              }
            }
            bufferLength = coords.length;
            numValues = bufferLength / 2;
            vectors[index].reset();
            for (int i = dimIdx; i < bufferLength; i += 2) {
              vectors[index].putInt(i / 2, coords[i]);
            }
            break;
          }
        case TILEDB_INT64:
        case TILEDB_UINT32:
        case TILEDB_UINT64:
          {
            long[] coords = (long[]) query.getCoordinates();
            int dimIdx = 0;
            for (; dimIdx < domain.getRank(); dimIdx++) {
              try (Dimension dim = domain.getDimension(dimIdx)) {
                if (dim.getName().equals(name)) {
                  break;
                }
              }
            }
            bufferLength = coords.length;
            numValues = bufferLength / 2;
            vectors[index].reset();
            for (int i = dimIdx; i < bufferLength; i += 2) {
              vectors[index].putLong(i / 2, coords[i]);
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

  @Override
  public void close() {
    try {
      if (batch != null) batch.close();
      if (query != null) query.close();
      if (array != null) array.close();
      if (ctx != null) ctx.close();
    } catch (TileDBError tileDBError) {
      tileDBError.printStackTrace();
    }
  }

  public List<Object> getPartitions() throws TileDBError {
    return new ArrayList<>(); // query.getPartitions();
  }
}
