package xyz.shiqihao.spark.rdd

import org.apache.spark.sql.SparkSession

import java.util.Random

/**
 * Usage GroupByTest [numMappers] [numKVPairs] [valSize] [numReducers]
 */
object GroupByTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GroupBy Test")
      //      .master("local[*]")
      .config("spark.eventLog.enabled", value = true)
      .config("spark.eventLog.dir", "hdfs://localhost:9000/spark-log")
      .getOrCreate()
    val sc = spark.sparkContext

    val numMappers = if (args.length > 0) args(0).toInt else 2
    val numKVPairs = if (args.length > 1) args(1).toInt else 1000
    val valSize = if (args.length > 2) args(2).toInt else 1000
    val numReducers = if (args.length > 3) args(3).toInt else numMappers

    val pairs = sc.parallelize(0 until numMappers, numMappers).flatMap { _ =>
      val ranGen = new Random()
      val arr = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr
    }

    println(pairs.toDebugString)

    println("\n#####\n")

    val groupByRDD = pairs.groupByKey(numReducers)

    println(groupByRDD.toDebugString)

    println(pairs.count())
    println(groupByRDD.count())

    sc.stop()
  }
}
