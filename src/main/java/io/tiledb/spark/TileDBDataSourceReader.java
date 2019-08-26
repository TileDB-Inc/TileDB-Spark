package io.tiledb.spark;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class TileDBDataSourceReader
    implements DataSourceReader,
        SupportsPushDownRequiredColumns,
        SupportsScanColumnarBatch,
        SupportsPushDownFilters {

  static Logger log = Logger.getLogger(TileDBDataSourceReader.class.getName());

  private URI uri;
  private TileDBReadSchema tileDBReadSchema;
  private TileDBDataSourceOptions tiledbOptions;
  private Filter[] pushedFilters;

  public TileDBDataSourceReader(URI uri, TileDBDataSourceOptions options) {
    this.uri = uri;
    this.tiledbOptions = options;
    this.tileDBReadSchema = new TileDBReadSchema(uri, options);
  }

  @Override
  public StructType readSchema() {
    log.trace("Reading schema for " + uri);
    StructType schema = tileDBReadSchema.getSparkSchema();
    log.trace("Read schema for " + uri + ": " + schema);
    return schema;
  }

  @Override
  public void pruneColumns(StructType pushDownSchema) {
    log.trace("Set pushdown columns for " + uri + ": " + pushDownSchema);
    tileDBReadSchema.setPushDownSchema(pushDownSchema);
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    log.info("Number of filters for pushdown " + filters.length);
    ArrayList<Filter> pushedFiltersList = new ArrayList<>();
    ArrayList<Filter> leftOverFilters = new ArrayList<>();

    // Loop through all filters and check if they are support type and on a domain. If so push them
    // down
    for (Filter filter : filters) {
      if (filter instanceof EqualNullSafe) {
        EqualNullSafe f = (EqualNullSafe) filter;
        if (tileDBReadSchema.isDimensionName(f.attribute())) {
          pushedFiltersList.add(filter);
        }
      } else if (filter instanceof EqualTo) {
        EqualTo f = (EqualTo) filter;
        if (tileDBReadSchema.isDimensionName(f.attribute())) {
          pushedFiltersList.add(filter);
        }
      } else if (filter instanceof GreaterThan) {
        GreaterThan f = (GreaterThan) filter;
        if (tileDBReadSchema.isDimensionName(f.attribute())) {
          pushedFiltersList.add(filter);
        }
      } else if (filter instanceof GreaterThanOrEqual) {
        GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
        if (this.tileDBReadSchema.isDimensionName(f.attribute())) {
          pushedFiltersList.add(filter);
        }
      } else if (filter instanceof In) {
        In f = (In) filter;
        if (this.tileDBReadSchema.isDimensionName(f.attribute())) {
          pushedFiltersList.add(filter);
        }
      } else if (filter instanceof LessThan) {
        LessThan f = (LessThan) filter;
        if (this.tileDBReadSchema.isDimensionName(f.attribute())) {
          pushedFiltersList.add(filter);
        }
      } else if (filter instanceof LessThanOrEqual) {
        LessThanOrEqual f = (LessThanOrEqual) filter;
        if (tileDBReadSchema.isDimensionName(f.attribute())) {
          pushedFiltersList.add(filter);
        }
      } else {
        leftOverFilters.add(filter);
      }
    }

    this.pushedFilters = new Filter[pushedFiltersList.size()];
    this.pushedFilters = pushedFiltersList.toArray(this.pushedFilters);

    Filter[] leftOvers = new Filter[leftOverFilters.size()];
    leftOvers = leftOverFilters.toArray(leftOvers);
    return leftOvers;
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public boolean enableBatchRead() {
    // always read in batch mode
    return true;
  }

  @Override
  public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {
    TileDBDomainPartitioner partitioner = new TileDBDomainPartitioner(tileDBReadSchema, tiledbOptions);
    partitioner.setPushdownFilters(pushedFilters);

    ArrayList<InputPartition<ColumnarBatch>> readerPartitions = new ArrayList<>();
    // TODO: multiple subarray partitioning
    readerPartitions.add(
        new TileDBDataReaderPartition(uri, tileDBReadSchema, tiledbOptions, pushedFilters));
    // foo
    return readerPartitions;
  }
}
