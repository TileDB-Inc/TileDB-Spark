package io.tiledb.spark;

import io.tiledb.java.api.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

public class TileDBWriterFactory implements DataWriterFactory, DataSourceWriter {
  private transient Context ctx;
  private String jobId;
  private StructType schema;
  private boolean createTable, deleteTable;
  private TileDBOptions tileDBOptions;

  public TileDBWriterFactory(
      String jobId,
      StructType schema,
      boolean createTable,
      boolean deleteTable,
      TileDBOptions tileDBOptions) {
    this.jobId = jobId;
    this.schema = schema;
    this.createTable = createTable;
    this.deleteTable = deleteTable;
    this.tileDBOptions = tileDBOptions;
  }

  public TileDBWriterFactory(
      String jobId,
      StructType schema,
      boolean createTable,
      boolean deleteTable,
      TileDBOptions tileDBOptions,
      boolean init)
      throws Exception {
    this.jobId = jobId;
    this.schema = schema;
    this.createTable = createTable;
    this.deleteTable = deleteTable;
    this.tileDBOptions = tileDBOptions;
    this.ctx = new Context();
    if (init) {
      if (deleteTable) deleteTable();

      if (createTable) createTable();
    }
  }

  public static Optional<DataSourceWriter> getWriter(
      String jobId, StructType schema, SaveMode mode, DataSourceOptions options) {
    boolean createTable = false;
    boolean deleteTable = false;
    TileDBOptions tileDBOptions = null;
    Context ctx;
    try {
      ctx = new Context();
      tileDBOptions = new TileDBOptions(options);
      Array array = new Array(ctx, tileDBOptions.ARRAY_URI, QueryType.TILEDB_WRITE);
      array.close();
    } catch (Exception tileDBError) {
      createTable = true;
    }
    try {
      switch (mode) {
        case Append:
          return Optional.of(
              new TileDBWriterFactory(jobId, schema, createTable, false, tileDBOptions, true));
        case Overwrite:
          if (!createTable)
            return Optional.of(
                new TileDBWriterFactory(jobId, schema, true, true, tileDBOptions, true));
          else
            return Optional.of(
                new TileDBWriterFactory(jobId, schema, true, false, tileDBOptions, true));
        case ErrorIfExists:
          if (!createTable) return Optional.empty();
          else
            return Optional.of(
                new TileDBWriterFactory(
                    jobId, schema, createTable, deleteTable, tileDBOptions, true));
        case Ignore:
          if (!createTable) return Optional.empty();
          else
            return Optional.of(
                new TileDBWriterFactory(
                    jobId, schema, createTable, deleteTable, tileDBOptions, true));
      }
    } catch (Exception tileDBError) {
      tileDBError.printStackTrace();
    }
    return Optional.empty();
  }

  @Override
  public DataWriter<Row> createDataWriter(int partitionId, int attemptNumber) {
    return new TileDBWriter(schema, tileDBOptions);
  }

  @Override
  public DataWriterFactory createWriterFactory() {
    return new TileDBWriterFactory(jobId, schema, createTable, deleteTable, tileDBOptions);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {}

  @Override
  public void abort(WriterCommitMessage[] messages) {}

  private void deleteTable() throws Exception {
    TileDBObject.remove(ctx, tileDBOptions.ARRAY_URI);
  }

  private void createTable() throws Exception {
    TileDBSchemaConverter tileDBSchemaConverter = new TileDBSchemaConverter(ctx, tileDBOptions);
    try (ArraySchema arraySchema = tileDBSchemaConverter.toTileDBSchema(schema)) {
      Array.create(tileDBOptions.ARRAY_URI, arraySchema);
    }
  }

  class TileDBWriter implements DataWriter<Row> {
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

    public TileDBWriter(StructType schema, TileDBOptions tileDBOptions) {
      this.options = tileDBOptions;
      try {
        init();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    private void init() throws Exception {
      ctx = new Context();
      array = new Array(ctx, tileDBOptions.ARRAY_URI, QueryType.TILEDB_WRITE);
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
              increaseRowIndex(name, seq.size());
            } catch (ClassCastException e) {
              byte[] seq = ((String) record.getAs(name)).getBytes();
              increaseRowIndex(name, seq.length);
            }
          } else {
            increaseRowIndex(name, 1);
          }
        }
        if (batch.size() >= options.BATCH_SIZE) {
          flush();
        }
      } catch (Exception err) {
        err.printStackTrace();
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
                    pair.getSecond()
                        .setItem(rowIndex * (int) cellValNum + index, array.apply(index));
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
}
