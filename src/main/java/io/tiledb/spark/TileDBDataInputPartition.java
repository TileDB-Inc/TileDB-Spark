package io.tiledb.spark;

import java.util.List;
import org.apache.spark.sql.connector.read.InputPartition;

public class TileDBDataInputPartition implements InputPartition {

  private final List<List<Range>> dimensionRanges;
  private final List<List<Range>> attributeRanges;
  private String uri;
  private TileDBReadSchema tileDBReadSchema;
  private TileDBDataSourceOptions tiledbOptions;

  public TileDBDataInputPartition(
      String uri,
      TileDBReadSchema schema,
      TileDBDataSourceOptions options,
      List<List<Range>> dimensionRanges,
      List<List<Range>> attributeRanges) {
    this.uri = uri;
    this.tileDBReadSchema = schema;
    this.tiledbOptions = options;
    this.dimensionRanges = dimensionRanges;
    this.attributeRanges = attributeRanges;
  }

  public List<List<Range>> getDimensionRanges() {
    return dimensionRanges;
  }

  public List<List<Range>> getAttributeRanges() {
    return attributeRanges;
  }

  public String getUri() {
    return uri;
  }

  public TileDBReadSchema getTileDBReadSchema() {
    return tileDBReadSchema;
  }

  public TileDBDataSourceOptions getTiledbOptions() {
    return tiledbOptions;
  }
}
