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
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

public class SubarrayBuilder {
  public Filter[] filters;
  public ArrayList<org.apache.spark.sql.sources.Filter> pushedFilters;
  public ArrayList<org.apache.spark.sql.sources.Filter> notPushedFilters;
  private TileDBOptions options;
  private Object subarray;
  private Context ctx;

  public SubarrayBuilder(Context ctx, TileDBOptions options) throws Exception {
    this.ctx = ctx;
    this.options = options;
    String arrayURI = options.ARRAY_URI;
    Array array = new Array(ctx, arrayURI);
    subarray = initSubarray(array.getSchema());
    pushedFilters = new ArrayList<>(1);
    notPushedFilters = new ArrayList<>(1);
  }

  public SubarrayBuilder(Context ctx, DataSourceOptions dataSourceOptions) throws Exception {
    this.ctx = ctx;
    this.options = new TileDBOptions(dataSourceOptions);
    String arrayURI = options.ARRAY_URI;
    Array array = new Array(ctx, arrayURI);
    subarray = initSubarray(array.getSchema());
    pushedFilters = new ArrayList<>(1);
    notPushedFilters = new ArrayList<>(1);
  }

  public void pushFilters(Filter[] filters) {
    this.filters = filters;
    for (Filter filter : filters) {
      addFilter(filter);
    }
  }

  private void addFilter(Filter filter) {
    //    System.out.println("Filter: " + filter);
    //    for(String ref : filter.references())
    //      System.out.println("!!! "+ref);
    notPushedFilters.add(filter);
    //    if (filter instanceof GreaterThan) {
    //      GreaterThanOrEqual gt = (GreaterThanOrEqual) filter;
    //      return gt.attribute().equals("i") && gt.value() instanceof Integer;
    //    }

    //      for(Filter filter : filters){
    //        if (filter instanceof GreaterThan) {
    //          GreaterThan gt = (GreaterThan) filter;
    //          return gt.attribute().equals("i") && gt.value() instanceof Integer;
    //        } else {
    //          return false;
    //        }
    //      }
    //      Filter[] supported = Arrays.stream(filters).filter(f -> {
    //        if (f instanceof GreaterThan) {
    //          GreaterThan gt = (GreaterThan) f;
    //          return gt.attribute().equals("i") && gt.value() instanceof Integer;
    //        } else {
    //          return false;
    //        }
    //      }).toArray(Filter[]::new);
    //
    //      Filter[] unsupported = Arrays.stream(filters).filter(f -> {
    //        if (f instanceof GreaterThan) {
    //          GreaterThan gt = (GreaterThan) f;
    //          return !gt.attribute().equals("i") || !(gt.value() instanceof Integer);
    //        } else {
    //          return true;
    //        }
    //      }).toArray(Filter[]::new);
    //
    //      this.filters = supported;
    //      return unsupported;
  }

  public Object nonEmptySubArray() throws Exception {
    Object subArray;
    try (Array array = new Array(ctx, options.ARRAY_URI);
        ArraySchema arraySchema = array.getSchema();
        Domain arrayDomain = arraySchema.getDomain()) {
      int ndim = (int) arrayDomain.getNDim();
      try (NativeArray nativeSubArray = new NativeArray(ctx, 2 * ndim, arrayDomain.getType())) {
        HashMap<String, Pair> nonEmptyDomain = array.nonEmptyDomain();
        for (int i = 0; i < (int) arrayDomain.getNDim(); i++) {
          try (Dimension dim = arrayDomain.getDimension(i)) {
            Pair domain = nonEmptyDomain.get(dim.getName());
            Object nonEmptyMin = domain.getFirst();
            Object nonEmptyMax = domain.getSecond();
            nativeSubArray.setItem((ndim * i) + 0, nonEmptyMin);
            nativeSubArray.setItem((ndim * i) + 1, nonEmptyMax);
          }
        }
        // we copy the native array here because the
        // reader / writers cannot serialize the NativeArray object
        subArray = nativeSubArray.toJavaArray();
      }
    }
    return subArray;
  }

  private Object initSubarray(ArraySchema arraySchema) throws Exception {
    try (Domain arrayDomain = arraySchema.getDomain()) {
      int ndim = (int) arrayDomain.getRank();
      switch (arrayDomain.getType()) {
        case TILEDB_FLOAT32:
          {
            float[] subarrayTmp = new float[ndim * 2];
            for (int i = 0; i < ndim; i++) {
              try (Dimension dimension = arrayDomain.getDimension(i)) {
                String dimName = dimension.getName();
                Pair dimDomain = dimension.getDomain();

                float min =
                    Math.max(
                        Float.parseFloat(
                            options
                                .get(TileDBOptions.SUBARRAY_MIN_KEY.replace("{}", dimName))
                                .orElse(Float.MIN_VALUE + "")),
                        (float) dimDomain.getFirst());
                float max =
                    Math.min(
                        Float.parseFloat(
                            options
                                .get(TileDBOptions.SUBARRAY_MAX_KEY.replace("{}", dimName))
                                .orElse(Float.MAX_VALUE + "")),
                        (float) dimDomain.getSecond());
                subarrayTmp[(2 * i) + 0] = min;
                subarrayTmp[(2 * i) + 1] = max;
              }
            }
            return subarrayTmp;
          }
        case TILEDB_FLOAT64:
          {
            double[] subarrayTmp = new double[ndim * 2];
            for (int i = 0; i < ndim; i++) {
              try (Dimension dimension = arrayDomain.getDimension(i)) {
                String dimName = dimension.getName();
                Pair domain = dimension.getDomain();
                double min =
                    Math.max(
                        Double.parseDouble(
                            options
                                .get(TileDBOptions.SUBARRAY_MIN_KEY.replace("{}", dimName))
                                .orElse(Double.MIN_VALUE + "")),
                        (double) domain.getFirst());
                double max =
                    Math.min(
                        Double.parseDouble(
                            options
                                .get(TileDBOptions.SUBARRAY_MAX_KEY.replace("{}", dimName))
                                .orElse(Double.MAX_VALUE + "")),
                        (double) domain.getSecond());
                subarrayTmp[(2 * i) + 0] = min;
                subarrayTmp[(2 * i) + 1] = max;
              }
            }
            return subarrayTmp;
          }
        case TILEDB_INT8:
          {
            byte[] subarrayTmp = new byte[ndim * 2];
            for (int i = 0; i < ndim; i++) {
              try (Dimension dimension = arrayDomain.getDimension(i)) {
                String dimName = dimension.getName();
                Pair domain = dimension.getDomain();
                int min =
                    Math.max(
                        Integer.parseInt(
                            options
                                .get(TileDBOptions.SUBARRAY_MIN_KEY.replace("{}", dimName))
                                .orElse(Byte.MIN_VALUE + "")),
                        ((Byte) domain.getFirst()).intValue());
                int max =
                    Math.min(
                        Integer.parseInt(
                            options
                                .get(TileDBOptions.SUBARRAY_MAX_KEY.replace("{}", dimName))
                                .orElse(Byte.MAX_VALUE + "")),
                        ((Byte) domain.getSecond()).intValue());
                subarrayTmp[(2 * i) + 0] = (byte) min;
                subarrayTmp[(2 * i) + 1] = (byte) max;
              }
            }
            return subarrayTmp;
          }
        case TILEDB_UINT8:
        case TILEDB_INT16:
          {
            short[] subarrayTmp = new short[ndim * 2];
            for (int i = 0; i < ndim; i++) {
              try (Dimension dimension = arrayDomain.getDimension(i)) {
                String dimName = dimension.getName();
                Pair domain = dimension.getDomain();
                int min =
                    Math.max(
                        Short.parseShort(
                            options
                                .get(TileDBOptions.SUBARRAY_MIN_KEY.replace("{}", dimName))
                                .orElse(Short.MIN_VALUE + "")),
                        (short) domain.getFirst());
                int max =
                    Math.min(
                        Short.parseShort(
                            options
                                .get(TileDBOptions.SUBARRAY_MAX_KEY.replace("{}", dimName))
                                .orElse(Short.MAX_VALUE + "")),
                        (short) domain.getSecond());
                subarrayTmp[(2 * i) + 0] = (short) min;
                subarrayTmp[(2 * i) + 1] = (short) max;
              }
            }
            return subarrayTmp;
          }
        case TILEDB_UINT16:
        case TILEDB_INT32:
          {
            int[] subarrayTmp = new int[ndim * 2];
            for (int i = 0; i < ndim; i++) {
              try (Dimension dimension = arrayDomain.getDimension(i)) {
                String dimName = dimension.getName();
                Pair domain = dimension.getDomain();
                int min =
                    Math.max(
                        Integer.parseInt(
                            options
                                .get(TileDBOptions.SUBARRAY_MIN_KEY.replace("{}", dimName))
                                .orElse(Integer.MIN_VALUE + "")),
                        (int) domain.getFirst());
                int max =
                    Math.min(
                        Integer.parseInt(
                            options
                                .get(TileDBOptions.SUBARRAY_MAX_KEY.replace("{}", dimName))
                                .orElse(Integer.MAX_VALUE + "")),
                        (int) domain.getSecond());
                subarrayTmp[(2 * i) + 0] = min;
                subarrayTmp[(2 * i) + 1] = max;
              }
            }
            return subarrayTmp;
          }
        case TILEDB_INT64:
        case TILEDB_UINT32:
          {
            long[] subarrayTmp = new long[ndim * 2];
            for (int i = 0; i < ndim; i++) {
              try (Dimension dimension = arrayDomain.getDimension(i)) {
                String dimName = dimension.getName();
                Pair domain = dimension.getDomain();
                long min =
                    Math.max(
                        Long.parseLong(
                            options
                                .get(TileDBOptions.SUBARRAY_MIN_KEY.replace("{}", dimName))
                                .orElse(Long.MIN_VALUE + "")),
                        (long) domain.getFirst());
                long max =
                    Math.min(
                        Long.parseLong(
                            options
                                .get(TileDBOptions.SUBARRAY_MAX_KEY.replace("{}", dimName))
                                .orElse(Long.MAX_VALUE + "")),
                        (long) domain.getSecond());
                subarrayTmp[(2 * i) + 0] = min;
                subarrayTmp[(2 * i) + 1] = max;
              }
            }
            return subarrayTmp;
          }
        default:
          {
            throw new TileDBError(
                "Unsupported subarray domain: " + arraySchema.getDomain().getType());
          }
      }
    }
  }

  public Filter[] getPushedFilters() {
    return pushedFilters.toArray(new Filter[pushedFilters.size()]);
  }

  public Filter[] getNotPushedFilters() {
    return notPushedFilters.toArray(new Filter[notPushedFilters.size()]);
  }

  public Object getSubArray() {
    return subarray;
  }
}
