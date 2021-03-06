package xyz.shiqihao.spark.rdd

import org.apache.spark.sql.SparkSession

/**
 * quick start
 */
object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/opt/spark-2.4.7-bin-hadoop2.7/README.md" // Should be some file on your system
    val spark = SparkSession.builder
      .appName("Simple Application")
      .config("spark.master", "local") // dev mode
      .getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
