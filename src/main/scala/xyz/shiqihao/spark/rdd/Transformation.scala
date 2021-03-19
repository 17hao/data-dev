package xyz.shiqihao.spark.rdd

import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * transformation in RDD
 */
object Transformation extends App {
  val spark = SparkSession
    .builder()
    .appName("transformation")
    .master("local[*]")
    .config("spark.eventLog.enabled", true)
    .config("spark.eventLog.dir", "hdfs://localhost:9000/spark-log")
    .getOrCreate()

  val input0 = spark.sparkContext.parallelize(0 to 16, 3)

  val res0 = input0.map(i => (Random.nextInt(3), i.toString)).reduceByKey((x, y) => x + "_" + y)
  //  val res0 = input0.map(_.toString).treeReduce((x, y) => x + "_" + y)
  println(res0.toDebugString)
}
