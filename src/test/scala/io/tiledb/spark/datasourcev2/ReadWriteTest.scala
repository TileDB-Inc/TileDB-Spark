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

package io.tiledb.spark.datasourcev2

import org.apache.spark.sql.{SaveMode, SparkSession}
//import org.scalatest.junit.JUnitRunner
//
//import org.junit.runner.RunWith

//@RunWith(classOf[JUnitRunner])
//object Test {
//  def main(args: Array[String]): Unit = {

import org.scalatest._

class ReadWriteTest extends FlatSpec {
    case class Record(d1: Int, d2: Int, a1: Int, a2: String, a3: Array[Float])
    val sparkSession = SparkSession
      .builder
      .master("local")
      .appName("Test")
      .getOrCreate()

    val df = sparkSession.createDataFrame(
        Seq(Record(1,1,0,"a",Array(0.1f,0.2f)),
            Record(1,2,1,"bb",Array(1.1f,1.2f)),
            Record(1,3,2,"ccc",Array(2.1f,2.2f)),
            Record(1,4,3,"dddd",Array(3.1f,3.2f)),
            Record(2,1,4,"e",Array(4.1f,4.2f)),
            Record(2,2,5,"ff",Array(5.1f,5.2f)),
            Record(2,3,6,"ggg",Array(6.1f,6.2f)),
            Record(2,4,7,"hhhh",Array(7.1f,7.2f)),
            Record(3,1,8,"i",Array(8.1f,8.2f)),
            Record(3,2,9,"jj",Array(9.1f,9.2f)),
            Record(3,3,10,"kkk",Array(10.1f,10.2f)),
            Record(3,4,11,"llll",Array(12.1f,11.2f)),
            Record(4,1,12,"m",Array(13.1f,13.2f)),
            Record(4,2,13,"nn",Array(14.1f,14.2f)),
            Record(4,3,14,"ooo",Array(15.1f,15.2f)),
            Record(4,4,15,"pppp",Array(16.1f,16.2f))
                    ))


    //print df schema
    df.schema.printTreeString()

    //add sql filters
    //    df = df.filter("(d1>=1 and d1<=2) and (d2>=1 and d2<=4)")
    //    df = df.filter(df("d2").geq(1))
    //      .filter(df("d2").leq(2))

    System.out.println("Count: " + df.collect().length)
    //print df
    df.show()


    df.write
      .format("io.tiledb.spark.datasourcev2")
      .option("arrayURI", "my_dense_array")
      .option("dimensions", "d1,d2")
      .option("subarray.d1.min", 1)
      .option("subarray.d1.max", 4)
      .option("subarray.d1.extent", 2)
      .option("subarray.d2.min", 1)
      .option("subarray.d2.max", 4)
      .option("subarray.d2.extent", 2)
      .mode(SaveMode.Overwrite)
      .save()

    sparkSession.read
      .format("io.tiledb.spark.datasourcev2")
      .option("arrayURI", "my_dense_array")
//        .option("batchSize", "10000")
//        .option("partitionSize", "10000")
      //add subarray filter
//      .option("subarray.d1.min", 1)
//      .option("subarray.d1.max", 2)
//      .option("subarray.d2.min", 1)
//      .option("subarray.d2.max", 4)
      // select columns
//      .select("d1","d2","a1","a3")
      .load()
      .show()

}
