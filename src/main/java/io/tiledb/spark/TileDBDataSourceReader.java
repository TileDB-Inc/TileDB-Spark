package io.tiledb.spark;

import io.tiledb.java.api.Context;
import io.tiledb.java.api.TileDBError;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

class TileDBDataSourceReader
    implements DataSourceReader,
        SupportsPushDownRequiredColumns,
        SupportsScanColumnarBatch,
        SupportsPushDownFilters {

  private Context ctx;
  private DataSourceOptions options;
  private StructType requiredSchema;
  private SubarrayBuilder subarrayBuilder;

  public TileDBDataSourceReader(URI uri, DataSourceOptions options) {
    try {
      this.options = options;
      ctx = new Context();
      subarrayBuilder = new SubarrayBuilder(ctx, options);
    } catch (TileDBError error) {
      throw new RuntimeException(error.getMessage());
    }
  }

  @Override
  public StructType readSchema() {
    try {
      TileDBSchemaConverter tileDBSchemaConverter = new TileDBSchemaConverter(ctx, options);
      tileDBSchemaConverter.setRequiredSchema(requiredSchema);
      return tileDBSchemaConverter.getSparkSchema();
    } catch (TileDBError err) {
      throw new RuntimeException(err.getMessage());
    }
  }

  @Override
  public List<DataReaderFactory<ColumnarBatch>> createBatchDataReaderFactories() {
    List<Object> partitions = new ArrayList<>();
    try {
      partitions =
          getSubarrayPartitions(subarrayBuilder.nonEmptySubArray(), requiredSchema, options);
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (partitions.isEmpty()) {
      try {
        return java.util.Arrays.asList(
            new TileDBReaderFactory(subarrayBuilder.nonEmptySubArray(), requiredSchema, options));
      } catch (Exception err) {
        err.printStackTrace();
        return new ArrayList<DataReaderFactory<ColumnarBatch>>();
      }
    } else {
      List<DataReaderFactory<ColumnarBatch>> ret = new ArrayList<>(partitions.size());
      for (Object partition : partitions) {
        ret.add(new TileDBReaderFactory(partition, requiredSchema, options));
      }
      return ret;
    }
  }

  private List<Object> getSubarrayPartitions(
      Object subarray, StructType requiredSchema, DataSourceOptions options) throws Exception {
    TileDBReaderFactory readerFactory =
        new TileDBReaderFactory(subarray, requiredSchema, options, true);
    List<Object> ret = readerFactory.getPartitions();
    readerFactory.close();
    return ret;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.requiredSchema = requiredSchema;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    subarrayBuilder.pushFilters(filters);
    return subarrayBuilder.getNotPushedFilters();
  }

  @Override
  public Filter[] pushedFilters() {
    return subarrayBuilder.getPushedFilters();
  }
}
