package io.tiledb.spark;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class TileDBPartitionReaderFactory implements PartitionReaderFactory {
  private boolean legacyReader;

  public TileDBPartitionReaderFactory(boolean legacyReader) {
    this.legacyReader = legacyReader;
  }

  // This method should be implemented only if a row-reader is available
  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    return null;
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    TileDBDataInputPartition tileDBDataInputPartition = (TileDBDataInputPartition) partition;
    if (legacyReader) {
      return new TileDBPartitionReaderLegacy(
          tileDBDataInputPartition.getUri(),
          tileDBDataInputPartition.getTileDBReadSchema(),
          tileDBDataInputPartition.getTiledbOptions(),
          tileDBDataInputPartition.getDimensionRanges(),
          tileDBDataInputPartition.getAttributeRanges());
    }
    return new TileDBPartitionReader(
        tileDBDataInputPartition.getUri(),
        tileDBDataInputPartition.getTileDBReadSchema(),
        tileDBDataInputPartition.getTiledbOptions(),
        tileDBDataInputPartition.getDimensionRanges(),
        tileDBDataInputPartition.getAttributeRanges());
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return true;
  }
}
