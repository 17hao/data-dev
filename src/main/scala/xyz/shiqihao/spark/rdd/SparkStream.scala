package xyz.shiqihao.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * nc -lk 9999
 */
object SparkStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("NetWorkCount")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.sparkContext.setLogLevel("WARN")
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordsCounts = words.map((_, 1)).reduceByKey((a, b) => a + b)
    wordsCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
