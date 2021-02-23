package xyz.shiqihao.spark

import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  private val SPARK_HOME = "/opt/spark-2.4.7-bin-hadoop2.7"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test1")
      .setSparkHome(SPARK_HOME)
    val spark = new SparkContext(conf)
    val text = spark.textFile(conf.get("spark.home") + "/README.md")
    println(text.toDebugString)
    val totalLen = text.map(s => s.length).reduce((a, b) => a + b)
    println(s"total len: $totalLen")
  }
}
