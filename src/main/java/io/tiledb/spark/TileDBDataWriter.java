package io.tiledb.spark;

import io.tiledb.java.api.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.Seq;

public class TileDBDataWriter implements DataWriter<Row> {

  private HashMap<String, Pair<NativeArray, NativeArray>> nativeArrays;
  private List<Row> batch;
  private HashMap<String, Pair<Integer, Integer>> varLengthIndex;
  private TileDBOptions options;
  private Array array;
  private ArraySchema arraySchema;
  private ArrayList<String> dimensionNames;
  private ArrayList<String> attributeNames;
  private ArrayList<Long> attributeCellValNum;
  private ArrayList<Datatype> attributeDatatype;
  private Context ctx;

  public TileDBDataWriter(StructType schema, TileDBOptions options) {
    try {
      ctx = new Context();
      array = new Array(ctx, options.ARRAY_URI, QueryType.TILEDB_WRITE);
      arraySchema = array.getSchema();
      attributeNames = new ArrayList<>();
      attributeCellValNum = new ArrayList<>();
      attributeDatatype = new ArrayList<>();
      for (int i = 0; i < arraySchema.getAttributeNum(); i++) {
        try (Attribute attr = arraySchema.getAttribute(i)) {
          attributeNames.add(attr.getName());
          attributeCellValNum.add(attr.getCellValNum());
          attributeDatatype.add(attr.getType());
        }
      }
      dimensionNames = new ArrayList<>();
      try (Domain domain = arraySchema.getDomain()) {
        for (int i = 0; i < domain.getRank(); i++) {
          try (Dimension dim = domain.getDimension(i)) {
            dimensionNames.add(dim.getName());
          }
        }
      }
      varLengthIndex = new HashMap<>();
      batch = new ArrayList<>(options.BATCH_SIZE);
      this.options = options;
    } catch (TileDBError error) {
      throw new RuntimeException(error.getMessage());
    }
  }

  @Override
  public void write(Row record) throws IOException {
    try {
      batch.add(record);
      for (int i = 0; i < attributeNames.size(); i++) {
        String name = attributeNames.get(i);
        long cellValNum = attributeCellValNum.get(i);
        if (cellValNum != 1) { // array
          try {
            Seq seq = record.getAs(name);
            increaseRowIndex(name, seq.length());
          } catch (ClassCastException e) {
            int length = ((String) record.getAs(name)).getBytes().length;
            increaseRowIndex(name, length);
          }
        } else {
          increaseRowIndex(name, 1);
        }
      }
      if (batch.size() >= options.BATCH_SIZE) {
        flush();
      }
    } catch (Exception err) {
      throw new IOException(err.getMessage());
    }
  }

  private void increaseValueIndex(String name, int length) {
    Pair<Integer, Integer> index = getIndex(name);
    Integer second = index.getSecond();
    second += length;
    index.setSecond(second);
  }

  private void increaseRowIndex(String name, int size) {
    Pair<Integer, Integer> index = getIndex(name);
    Integer first = index.getFirst();
    first += size;
    index.setFirst(first);
  }

  private Pair<Integer, Integer> getIndex(String name) {
    Pair<Integer, Integer> index = varLengthIndex.get(name);
    if (index == null) {
      index = new Pair<>(0, 0);
      varLengthIndex.put(name, index);
    }
    return index;
  }

  private void flush() throws Exception {
    if (!arraySchema.isSparse()) {
      throw new TileDBError("Dense array writes are unsupported");
    }
    flushSparse();
  }

  private void flushSparse() throws Exception {
    assert arraySchema.isSparse() == true;
    int nrecords = batch.size();
    // nothing to write for this partition
    if (nrecords == 0) {
      return;
    }
    // Create a query for every batched flush()
    try (Query query = new Query(array, QueryType.TILEDB_WRITE)) {

      // we rely on TileDB to sort to global order
      // no way to tell if the record batch
      // is sorted on the dimensions
      query.setLayout(Layout.TILEDB_UNORDERED);

      // Allocate and set Query write buffers
      nativeArrays = new HashMap<>();
      int ndim = dimensionNames.size();
      int nattr = attributeNames.size();
      for (int i = 0; i < nattr; i++) {
        String attrName = attributeNames.get(i);
        Datatype attrType = attributeDatatype.get(i);
        long cellValNum = attributeCellValNum.get(i);
        // var length cell
        if (cellValNum == Constants.TILEDB_VAR_NUM) {
          NativeArray first = new NativeArray(ctx, batch.size(), Datatype.TILEDB_UINT64);
          NativeArray second =
              new NativeArray(ctx, (int) varLengthIndex.get(attrName).getFirst(), attrType);
          Pair<NativeArray, NativeArray> pair = new Pair<>(first, second);
          nativeArrays.put(attrName, pair);
          query.setBuffer(attrName, first, second);
          // scalar cell
        } else {
          NativeArray second = new NativeArray(ctx, batch.size() * (int) cellValNum, attrType);
          Pair<NativeArray, NativeArray> pair = new Pair<>(null, second);
          nativeArrays.put(attrName, pair);
          query.setBuffer(attrName, second);
        }
      }

      // Allocate and set Query coordinate buffer
      NativeArray coords;
      try (Domain domain = arraySchema.getDomain()) {
        coords = new NativeArray(ctx, batch.size() * ndim, domain.getType());
      }
      nativeArrays.put(Constants.TILEDB_COORDS, new Pair<>(null, coords));
      query.setCoordinates(coords);

      int rowIndex = 0;
      varLengthIndex = new HashMap<>();
      for (Row record : batch) {
        for (int dimIdx = 0; dimIdx < ndim; dimIdx++) {
          String dimensionName = dimensionNames.get(dimIdx);
          coords.setItem(rowIndex * ndim + dimIdx, record.getAs(dimensionName));
        }
        for (int attrIdx = 0; attrIdx < nattr; attrIdx++) {
          String attrName = attributeNames.get(attrIdx);
          long cellValNum = attributeCellValNum.get(attrIdx);
          Pair<NativeArray, NativeArray> pair = nativeArrays.get(attrName);
          // var length cell
          if (cellValNum == Constants.TILEDB_VAR_NUM) {
            int datatypeBytes = attributeDatatype.get(attrIdx).getNativeSize();
            try {
              Seq array = record.getAs(attrName);
              for (int index = 0; index < array.size(); index++) {
                pair.getSecond().setItem(getIndex(attrName).getSecond(), array.apply(index));
                increaseValueIndex(attrName, 1);
              }
              pair.getFirst().setItem(rowIndex, (long) getIndex(attrName).getFirst());
              increaseRowIndex(attrName, array.size() * datatypeBytes);
            } catch (ClassCastException e) {
              String s = record.getAs(attrName);
              int nbytes = s.getBytes().length;
              pair.getSecond().setItem(getIndex(attrName).getSecond(), s);
              increaseValueIndex(attrName, nbytes);
              pair.getFirst().setItem(rowIndex, (long) getIndex(attrName).getFirst());
              increaseRowIndex(attrName, nbytes * datatypeBytes);
            }
            // scalar cell
          } else {
            if (cellValNum == 1) {
              pair.getSecond().setItem(rowIndex, record.getAs(attrName));
            } else {
              // fixed size scalar cell
              try {
                Seq array = record.getAs(attrName);
                for (int index = 0; index < cellValNum; index++) {
                  pair.getSecond().setItem(rowIndex * (int) cellValNum + index, array.apply(index));
                }
              } catch (ClassCastException e) {
                String s = record.getAs(attrName);
                pair.getSecond().setItem(rowIndex * (int) cellValNum, s);
              }
            }
          }
        }
        rowIndex++;
      }
      query.submit();
      query.finalizeQuery();
    }
    batch.clear();
    varLengthIndex = new HashMap<>();
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    try {
      flush();
      if (ctx != null) {
        array.close();
        ctx.close();
      }
    } catch (Exception tileDBError) {
      throw new IOException(tileDBError.getMessage());
    }
    return null;
  }

  @Override
  public void abort() throws IOException {}
}
