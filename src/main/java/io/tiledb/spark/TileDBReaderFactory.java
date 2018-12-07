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

package io.tiledb.spark;

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
      // initialize Query
      try {
        ctx = new Context();
        array = new Array(ctx, options.ARRAY_URI);
        arraySchema = array.getSchema();
        try (Domain domain = arraySchema.getDomain();
            NativeArray nativeSubArray = new NativeArray(ctx, subarray, domain.getType())) {

          // Compute estimate for the max number of buffer elements for the subarray query
          HashMap<String, Pair<Long, Long>> maxBufferElements =
              array.maxBufferElements(nativeSubArray);

          // Compute an upper bound on the number of results in the subarray
          Long estNumRows =
              maxBufferElements.get(Constants.TILEDB_COORDS).getSecond() / domain.getNDim();

          // Create query
          query = new Query(array, QueryType.TILEDB_READ);
          query.setLayout(Layout.TILEDB_GLOBAL_ORDER);
          query.setSubarray(nativeSubArray);

          // Allocate result set batch based on the estimated (upper bound) number of rows
          vectors = OnHeapColumnVector.allocateColumns(estNumRows.intValue(), attributes);

          // loop over all attributes and set the query buffers based on the result size estimate
          for (StructField field : attributes) {
            // get the spark column name and match to array schema
            String name = field.name();
            if (domain.hasDimension(name)) {
              // dimension column (coordinate buffer allocation handled at the end)
              continue;
            }
            try (Attribute attr = arraySchema.getAttribute(name)) {
              // attribute is variable length, init the varlen result buffers
              if (attr.isVar()) {
                query.setBuffer(
                    name,
                    new NativeArray(
                        ctx,
                        maxBufferElements.get(name).getFirst().intValue(),
                        Datatype.TILEDB_UINT64),
                    new NativeArray(
                        ctx, maxBufferElements.get(name).getSecond().intValue(), attr.getType()));
              } else {
                // attribute is fixed length, use the result size estimate for allocation
                query.setBuffer(
                    name,
                    new NativeArray(
                        ctx, maxBufferElements.get(name).getSecond().intValue(), attr.getType()));
              }
            }
          }
          // set the coordinate buffer result buffer
          query.setCoordinates(
              new NativeArray(
                  ctx,
                  maxBufferElements.get(Constants.TILEDB_COORDS).getSecond().intValue(),
                  domain.getType()));
          batch = new ColumnarBatch(vectors);
          hasNext = true;
        }
      } catch (Exception err) {
        throw new RuntimeException(err.getMessage());
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
    } catch (TileDBError err) {
      throw new RuntimeException(err.getMessage());
    }
  }

  @Override
  public ColumnarBatch get() {
    try {
      int i = 0;
      int nRows = -1;
      if (attributes.length == 0) {
        // TODO: materialize the first dimension and count the result set size
        try (Domain domain = arraySchema.getDomain();
            Dimension dim = domain.getDimension(0)) {
          nRows = getDimensionColumn(dim.getName(), 0);
        }
      } else {
        // loop over all Spark attributes (Dataframe columns) and copy the query result set
        for (StructField field : attributes) {
          nRows = getColumnBatch(field, i);
          i++;
        }
      }
      batch.setNumRows(nRows);
    } catch (TileDBError err) {
      throw new RuntimeException(err.getMessage());
    }
    return batch;
  }

  private int getColumnBatch(StructField field, int index) throws TileDBError {
    String name = field.name();
    if (arraySchema.hasAttribute(name)) {
      return getAttributeColumn(name, index);
    } else {
      // dimension column
      return getDimensionColumn(name, index);
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
      long[] offsets = query.getVarBuffer(name);
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
      int ndim = Math.toIntExact(domain.getNDim());
      int dimIdx = 0;
      for (; dimIdx < ndim; dimIdx++) {
        try (Dimension dim = domain.getDimension(dimIdx)) {
          if (dim.getName().equals(name)) {
            break;
          }
        }
      }
      switch (domain.getType()) {
        case TILEDB_FLOAT32:
          {
            float[] coords = (float[]) query.getCoordinates();
            bufferLength = coords.length;
            numValues = bufferLength / ndim;
            if (vectors.length > 0) {
              vectors[index].reset();
              for (int i = dimIdx, row = 0; i < bufferLength; i += ndim, row++) {
                vectors[index].putFloat(row, coords[i]);
              }
            }
            break;
          }
        case TILEDB_FLOAT64:
          {
            double[] coords = (double[]) query.getCoordinates();
            bufferLength = coords.length;
            numValues = bufferLength / ndim;
            if (vectors.length > 0) {
              vectors[index].reset();
              for (int i = dimIdx, row = 0; i < bufferLength; i += ndim, row++) {
                vectors[index].putDouble(row, coords[i]);
              }
            }
            break;
          }
        case TILEDB_INT8:
          {
            byte[] coords = (byte[]) query.getCoordinates();
            bufferLength = coords.length;
            numValues = bufferLength / ndim;
            if (vectors.length > 0) {
              vectors[index].reset();
              for (int i = dimIdx, row = 0; i < bufferLength; i += ndim, row++) {
                vectors[index].putByte(row, coords[i]);
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
            if (vectors.length > 0) {
              vectors[index].reset();
              for (int i = dimIdx, row = 0; i < bufferLength; i += ndim, row++) {
                vectors[index].putShort(row, coords[i]);
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
            if (vectors.length > 0) {
              vectors[index].reset();
              for (int i = dimIdx, row = 0; i < bufferLength; i += ndim, row++) {
                vectors[index].putInt(row, coords[i]);
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
            if (vectors.length > 0) {
              vectors[index].reset();
              for (int i = dimIdx, row = 0; i < bufferLength; i += ndim, row++) {
                vectors[index].putLong(row, coords[i]);
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

  @Override
  public void close() {
    if (batch != null) {
      batch.close();
    }
    if (query != null) {
      query.close();
    }
    if (array != null) {
      array.close();
    }
    if (ctx != null) {
      ctx.close();
    }
  }

  public List<Object> getPartitions() throws TileDBError {
    return new ArrayList<>(); // query.getPartitions();
  }
}
