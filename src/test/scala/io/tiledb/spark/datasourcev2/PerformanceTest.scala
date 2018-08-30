package io.tiledb.spark.datasourcev2

import java.util.Random
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest._
import sys.process._

class PerformanceTest extends FlatSpec {
  val sparkSession = SparkSession
    .builder
    .master("local")
    .appName("PerformanceTest")
    .getOrCreate()

  val sc = sparkSession.sparkContext
  sc.setLogLevel("ERROR")
  //warmup
  var i = 0
  def nextInt(): Int = {i += 1; return i}
  def randomInt = scala.util.Random.nextInt(100)
  val df = sparkSession.createDataFrame(sc.parallelize(
    Seq.fill(1000) {
      (nextInt, randomInt)
    }
  ))
  df.cache()
  df.write.option("compression", "snappy").mode(SaveMode.Overwrite).parquet("test.parquet")
  sparkSession.read.parquet("test.parquet").filter("_1>1000").count()

  val million =1000000;
  val sizes = List( 100000, million)
  var randomness = List(100, 10000, million)
  val compressionCodecs = List("none", "gzip", "snappy")

  System.out.println("Random ints as values")
  System.out.println(
    String.format("%7s", "Rows") +
      String.format("%9s", "MaxValue") +
      String.format("%13s", "Compression") +
      String.format("%20s", "ParquetWrite(ms)") +
      String.format("%20s", "ParquetRead(ms)") +
      String.format("%20s", "ParquetDiskSize") +
      String.format("%20s", "TileDBWrite(ms)") +
      String.format("%20s", "TileDBRead(ms)") +
      String.format("%20s", "TileDBDiskSize"))
  for( size <- sizes) {
    for (max <- randomness) {
      //generate data
      i = 0
      var j = 0
      def randomInt= scala.util.Random.nextInt(max)
      val df = sparkSession.createDataFrame(sc.parallelize(
        Seq.fill(size) {
          (nextInt, randomInt)
        }
      ))
      df.cache()
      for (compression <- compressionCodecs) {
        val start = System.currentTimeMillis()
        df.write.option("compression", compression).mode(SaveMode.Overwrite).parquet("test.parquet")
        val write1 = System.currentTimeMillis()
        sparkSession.read.parquet("test.parquet").filter("_1>1000").count()
        val read1 = System.currentTimeMillis()
        val parquetDiskSize = ("du -hs test.parquet " !!).split("\t"){0}
        df.write
          .format("io.tiledb.spark.datasourcev2")
          .option("arrayURI", "my_array")
          .option("dimensions", "_1")
          .option("subarray._1.min", 1)
          .option("subarray._1.max", size)
          .option("subarray._1.extent", size/10)
          .option("compression", compression)
          .mode(SaveMode.Overwrite)
          .save()

        val write2 = System.currentTimeMillis()
        val tiledbDiskSize = ("du -hs my_array " !!).split("\t"){0}
        sparkSession.read
          .format("io.tiledb.spark.datasourcev2")
          .option("arrayURI", "my_array")
          .option("batchSize", million+"")
          .load().filter("_1>1000").count()
        val read2 = System.currentTimeMillis()

        System.out.println(
          String.format("%7s", if(size==100000) "100K" else size/million+"M") +
            String.format("%9s", if(max== 100) "100" else if(max==10000) "10K" else max/million+"M") +
            String.format("%13s", compression) +
            String.format("%20s", (write1-start)+"") +
            String.format("%20s", (read1-write1)+"") +
            String.format("%20s", parquetDiskSize) +
            String.format("%20s", (write2-read1)+"") +
            String.format("%20s", (read2-write2)+"")+
            String.format("%20s", tiledbDiskSize+""))
      }
    }
  }

  System.out.println("Consecutive ints as values")
  System.out.println(
    String.format("%7s", "Rows") +
      String.format("%13s", "Compression") +
      String.format("%20s", "ParquetWrite(ms)") +
      String.format("%20s", "ParquetRead(ms)") +
      String.format("%20s", "ParquetDiskSize") +
      String.format("%20s", "TileDBWrite(ms)") +
      String.format("%20s", "TileDBRead(ms)") +
      String.format("%20s", "TileDBDiskSize"))
  for( size <- sizes) {
    //generate data
    i = 0
    var j = 0
    def randomInt: Int = {j += 1; return j}
    val df = sparkSession.createDataFrame(sc.parallelize(
      Seq.fill(size) {
        (nextInt, randomInt)
      }
    ))
    df.cache()

    for (compression <- compressionCodecs) {
      val start = System.currentTimeMillis()
      df.write.option("compression", compression).mode(SaveMode.Overwrite).parquet("test.parquet")
      val write1 = System.currentTimeMillis()
      sparkSession.read.parquet("test.parquet").filter("_1>1000").count()
      val read1 = System.currentTimeMillis()
      val parquetDiskSize = ("du -hs test.parquet " !!).split("\t"){0}

      df.write
        .format("io.tiledb.spark.datasourcev2")
        .option("arrayURI", "my_array")
        .option("dimensions", "_1")
        .option("subarray._1.min", 1)
        .option("subarray._1.max", size)
        .option("subarray._1.extent", size/10)
        .option("compression", compression)
        .mode(SaveMode.Overwrite)
        .save()

      val write2 = System.currentTimeMillis()
      val tiledbDiskSize = ("du -hs my_array " !!).split("\t"){0}
      sparkSession.read
        .format("io.tiledb.spark.datasourcev2")
        .option("arrayURI", "my_array")
        .option("batchSize", million+"")
        .load().filter("_1>1000").count()
      val read2 = System.currentTimeMillis()

      System.out.println(
        String.format("%7s", if(size==100000) "100K" else size/million+"M") +
          String.format("%13s", compression) +
          String.format("%20s", (write1-start)+"") +
          String.format("%20s", (read1-write1)+"") +
          String.format("%20s", parquetDiskSize) +
          String.format("%20s", (write2-read1)+"") +
          String.format("%20s", (read2-write2)+"")+
          String.format("%20s", tiledbDiskSize+""))
    }
  }
}
