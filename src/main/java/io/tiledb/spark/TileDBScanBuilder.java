package io.tiledb.spark;

import static org.apache.spark.metrics.TileDBMetricsSource.dataSourcePruneColumnsTimerName;
import static org.apache.spark.metrics.TileDBMetricsSource.dataSourcePushFiltersTimerName;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.metrics.TileDBReadMetricsUpdater;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.types.StructType;

public class TileDBScanBuilder
    implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {
  private final TileDBDataSourceOptions options;
  private final TileDBReadMetricsUpdater metricsUpdater;
  private final TileDBReadSchema tileDBReadSchema;
  private Filter[] pushedFilters;
  private String uri;

  static Logger log = Logger.getLogger(TileDBScanBuilder.class.getName());

  public TileDBScanBuilder(
      Map<String, String> properties, TileDBDataSourceOptions tileDBDataSourceOptions)
      throws URISyntaxException {
    this.options = tileDBDataSourceOptions;
    this.metricsUpdater = new TileDBReadMetricsUpdater(TaskContext.get());
    this.uri = util.tryGetArrayURI(tileDBDataSourceOptions);
    this.tileDBReadSchema = new TileDBReadSchema(this.uri, options);
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    metricsUpdater.startTimer(dataSourcePushFiltersTimerName);
    log.trace("size of filters " + filters.length);
    ArrayList<Filter> pushedFiltersList = new ArrayList<>();
    ArrayList<Filter> leftOverFilters = new ArrayList<>();

    // Loop through all filters and check if they are support type and on a domain. If so push them
    // down
    for (Filter filter : filters) {
      if (filterCanBePushedDown(filter)) {
        pushedFiltersList.add(filter);
      } else {
        leftOverFilters.add(filter);
      }
    }

    this.pushedFilters = new Filter[pushedFiltersList.size()];
    this.pushedFilters = pushedFiltersList.toArray(this.pushedFilters);

    Filter[] leftOvers = new Filter[leftOverFilters.size()];
    leftOvers = leftOverFilters.toArray(leftOvers);
    metricsUpdater.finish(dataSourcePushFiltersTimerName);
    return leftOvers;
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public void pruneColumns(StructType pushDownSchema) {
    metricsUpdater.startTimer(dataSourcePruneColumnsTimerName);
    log.trace("Set pushdown columns for " + uri + ": " + pushDownSchema);
    tileDBReadSchema.setPushDownSchema(pushDownSchema);
    metricsUpdater.finish(dataSourcePruneColumnsTimerName);
  }

  @Override
  public Scan build() {
    return new TileDBScan(tileDBReadSchema, options, pushedFilters);
  }

  private boolean filterCanBePushedDown(Filter filter) {
    if (filter instanceof And) {
      And f = (And) filter;
      if (filterCanBePushedDown(f.left()) && filterCanBePushedDown(f.right())) {
        return true;
      }
    } else if (filter instanceof EqualNullSafe) {
      return true;
    } else if (filter instanceof EqualTo) {
      return true;
    } else if (filter instanceof GreaterThan) {
      return true;
    } else if (filter instanceof GreaterThanOrEqual) {
      return true;
    } else if (filter instanceof LessThan) {
      return true;
    } else if (filter instanceof LessThanOrEqual) {
      return true;
    }
    return false;
  }
}
