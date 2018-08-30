package io.tiledb.spark.datasourcev2;

import io.tiledb.java.api.*;
import io.tiledb.libtiledb.tiledb;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

import java.io.IOException;
import java.util.*;

import static io.tiledb.java.api.Layout.TILEDB_UNORDERED;
import static io.tiledb.java.api.QueryType.TILEDB_WRITE;

public class TileDBWriterFactory implements DataWriterFactory, DataSourceWriter {
  private transient Context ctx;
  private String jobId;
  private StructType schema;
  private boolean createTable, deleteTable;
  private TileDBOptions tileDBOptions;
  private static long start;

  public TileDBWriterFactory(String jobId, StructType schema, boolean createTable, boolean deleteTable, TileDBOptions tileDBOptions) {
    this.jobId = jobId;
    this.schema = schema;
    this.createTable = createTable;
    this.deleteTable = deleteTable;
    this.tileDBOptions = tileDBOptions;
  }

  public TileDBWriterFactory(String jobId, StructType schema, boolean createTable, boolean deleteTable, TileDBOptions tileDBOptions, boolean init) throws Exception {
    this.jobId = jobId;
    this.schema = schema;
    this.createTable = createTable;
    this.deleteTable = deleteTable;
    this.tileDBOptions = tileDBOptions;
    this.ctx = new Context();
    if(init) {
      if (deleteTable)
        deleteTable();

      if (createTable)
        createTable();
    }
  }

  public static Optional<DataSourceWriter> getWriter(String jobId, StructType schema, SaveMode mode, DataSourceOptions options) {
    start = System.currentTimeMillis();
    boolean createTable = false, deleteTable = false;
    TileDBOptions tileDBOptions = null;
    Context ctx = null;
    try {
      ctx = new Context();
      tileDBOptions = new TileDBOptions(options);
      Array array = new Array(ctx, tileDBOptions.ARRAY_URI, TILEDB_WRITE);
      array.close();
    } catch (Exception tileDBError) {
        createTable = true;
    }
    try {
      switch (mode){
        case Append:
          return Optional.of(new TileDBWriterFactory(jobId, schema, createTable, false, tileDBOptions, true));
        case Overwrite:
          if(!createTable)
            return Optional.of(new TileDBWriterFactory(jobId, schema, true, true, tileDBOptions, true));
          else
            return Optional.of(new TileDBWriterFactory(jobId, schema, true, false, tileDBOptions, true));
        case ErrorIfExists:
          if(!createTable)
            return Optional.empty();
          else
            return Optional.of(new TileDBWriterFactory(jobId, schema, createTable, deleteTable, tileDBOptions, true));
        case Ignore:
          if(!createTable)
            return Optional.empty();
          else
            return Optional.of(new TileDBWriterFactory(jobId, schema, createTable, deleteTable, tileDBOptions, true));
      }
    } catch (Exception tileDBError) {
      if(tileDBError.getMessage().contains("Schema file not found"))
        createTable = true;
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
  public void commit(WriterCommitMessage[] messages) {
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
  }

  private void deleteTable() throws Exception{
    TileDBObject.remove(ctx, tileDBOptions.ARRAY_URI);
  }

  private void createTable() throws Exception {
    TileDBSchemaConverter tileDBSchemaConverter = new TileDBSchemaConverter(ctx, tileDBOptions);
    ArraySchema arraySchema = tileDBSchemaConverter.toTileDBSchema(schema);
    Array.create(tileDBOptions.ARRAY_URI, arraySchema);
  }


  class TileDBWriter implements DataWriter<Row> {
    private HashMap<String,Pair<NativeArray,NativeArray>> nativeArrays;
    private List<Row> batch;
    private HashMap<String,Pair<Integer,Integer>> varLengthIndex;
    private int rowIndex;
    private TileDBOptions options;
    private StructType schema;
    private Array array;
    private ArraySchema arraySchema;
    private SubarrayBuilder subarrayBuilder;
    private Query query;
    private List<String> dimensionNames;
    private HashMap<String,Long> attributeCellValnums;
    private boolean test =false;

    public TileDBWriter(StructType schema, TileDBOptions tileDBOptions)  {
      this.schema = schema;
      this.options = tileDBOptions;
      try {
        init();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    private void init() throws Exception {
      ctx = new Context();
      subarrayBuilder = new SubarrayBuilder(ctx, options);
      array = new Array(ctx, tileDBOptions.ARRAY_URI, TILEDB_WRITE);
      arraySchema = array.getSchema();
      attributeCellValnums = new HashMap<>();
      for(Attribute attribute : arraySchema.getAttributes().values()) {
        String name = attribute.getName();
        attributeCellValnums.put(name, arraySchema.getAttribute(name).getCellValNum());
      }
      dimensionNames = new ArrayList<>();
      for(Dimension dimension : arraySchema.getDomain().getDimensions()){
        dimensionNames.add(dimension.getName());
      }
      varLengthIndex = new HashMap<>();
      batch = new ArrayList<>(options.BATCH_SIZE);
    }

    @Override
    public void write(Row record) throws IOException {
//      if(!test){
//        test=true;
//        System.out.println("Init time(ms): "+(System.currentTimeMillis()-start));
//        start = System.currentTimeMillis();
//      }
      try {
        batch.add(record);
        for(Map.Entry<String, Long> attr : attributeCellValnums.entrySet()){
          if(attr.getValue() != 1){ //array
            try {
              Seq seq = (Seq) record.getAs(attr.getKey());
              increaseRowIndex(attr.getKey(),seq.size());
            } catch (ClassCastException e){
              byte[] seq = ((String) record.getAs(attr.getKey())).getBytes();
              increaseRowIndex(attr.getKey(),seq.length);
            }
          }
          else{
            increaseRowIndex(attr.getKey(),1);
          }
        }

        if(batch.size()>=options.BATCH_SIZE){
            flush();
        }

      } catch (Exception tileDBError) {
        tileDBError.printStackTrace();
        throw new IOException(tileDBError.getMessage());
      }
    }

    private void increaseValueIndex(String name, int length) {
      Pair<Integer,Integer> index = getIndex(name);
      Integer second = index.getSecond();
      second+=length;
      index.setSecond(second);
    }

    private void increaseValueIndex(String name) {
      Pair<Integer,Integer> index = getIndex(name);
      Integer second = index.getSecond();
      second++;
      index.setSecond(second);
    }

    private void increaseRowIndex(String name, int size) {
      Pair<Integer,Integer> index = getIndex(name);
      Integer first = index.getFirst();
      first+=size;
      index.setFirst(first);
    }

    private Pair<Integer,Integer> getIndex(String name) {
      Pair<Integer,Integer> index = varLengthIndex.get(name);
      if(index==null) {
        index = new Pair<>(0,0);
        varLengthIndex.put(name, index);
      }
      return index;
    }

    private void flush() throws Exception {
      if(batch.size()==0)
        return;

//      System.out.println("Iterate time(ms): "+(System.currentTimeMillis()-start));
//      start = System.currentTimeMillis();

      // Create query
      query = new Query(array, TILEDB_WRITE);
      query.setLayout(TILEDB_UNORDERED);
//      NativeArray nsubarray = new NativeArray(ctx, subarrayBuilder.getSubArray(), arraySchema.getDomain().getType());
//      query.setSubarray(nsubarray);
      nativeArrays = new HashMap<>();
      for(Attribute attribute : arraySchema.getAttributes().values()) {
        String name = attribute.getName();
        long cellValNum = arraySchema.getAttribute(name).getCellValNum();
        if (cellValNum == tiledb.tiledb_var_num()) {
          NativeArray first = new NativeArray(ctx, (int) batch.size(), Long.class);
          NativeArray second = new NativeArray(ctx, (int)  varLengthIndex.get(name).getFirst(), arraySchema.getAttribute(name).getType());
          Pair<NativeArray, NativeArray> pair = new Pair<NativeArray, NativeArray>(first, second);
          nativeArrays.put(name, pair);
          query.setBuffer(name, first, second);
        } else {
          NativeArray second = new NativeArray(ctx, batch.size() * (int) cellValNum, arraySchema.getAttribute(name).getType());
          Pair<NativeArray, NativeArray> pair = new Pair<NativeArray, NativeArray>(null, second);
          nativeArrays.put(name, pair);
          query.setBuffer(name, second);
        }
      }
      NativeArray coords = new NativeArray(ctx,
          batch.size() * arraySchema.getDomain().getDimensions().size(), arraySchema.getDomain().getType());
      nativeArrays.put(tiledb.tiledb_coords(),new Pair<NativeArray, NativeArray>(null, coords));
      query.setCoordinates(coords);
      rowIndex=0;
      varLengthIndex = new HashMap<>();
      for (Row record : batch) {
        int dimIndex = 0;
        for(String dimension : dimensionNames){
          coords.setItem(rowIndex*dimensionNames.size()+dimIndex, record.getAs(dimension));
          dimIndex++;
        }
        long varNum = tiledb.tiledb_var_num();
        for(Map.Entry<String, Long> attr : attributeCellValnums.entrySet()){
          String name = attr.getKey();
          long cellValNum = attr.getValue();
          Pair<NativeArray, NativeArray> pair = nativeArrays.get(name);
          if(cellValNum == varNum){
            int typeSize = arraySchema.getAttribute(name).getType().getNativeSize();
            try {
              Seq array = (Seq) record.getAs(name);
              for (int index = 0; index < array.size(); index++) {
                pair.getSecond().setItem(getIndex(name).getSecond(), array.apply(index));
                increaseValueIndex(name);
              }
              pair.getFirst().setItem(rowIndex, getIndex(name).getFirst());
              increaseRowIndex(name,array.size()*typeSize);
            } catch (ClassCastException e){
              String s = (String) record.getAs(name);
              pair.getSecond().setItem(getIndex(name).getSecond(), s);
              increaseValueIndex(name, s.getBytes().length);
              pair.getFirst().setItem(rowIndex, getIndex(name).getFirst());
              increaseRowIndex(name,s.getBytes().length * typeSize);
            }
          } else {
            if(cellValNum == 1 ) {
              pair.getSecond().setItem(rowIndex, record.getAs(name));
            }
            else {
              try {
                Seq array = (Seq) record.getAs(name);
                for (int index = 0; index < cellValNum; index++) {
                  pair.getSecond().setItem(rowIndex * (int)cellValNum + index, array.apply(index));
                }
              } catch (ClassCastException e){
                String s = (String) record.getAs(name);
                pair.getSecond().setItem(rowIndex * (int)cellValNum, s);
              }
            }
          }
        }
        rowIndex++;
      }
      query.submit();
      query.close();
      batch.clear();
      varLengthIndex = new HashMap<>();
//      System.out.println("Flush time(ms): "+(System.currentTimeMillis()-start));
//      start = System.currentTimeMillis();
    }


    @Override
    public WriterCommitMessage commit() throws IOException {
      try {
        flush();
        if(ctx!=null) {
          array.close();
          ctx.close();
        }
      } catch (Exception tileDBError) {
        throw new IOException(tileDBError.getMessage());
      }
      return null;
    }

    @Override
    public void abort() throws IOException {
    }
  }


}
