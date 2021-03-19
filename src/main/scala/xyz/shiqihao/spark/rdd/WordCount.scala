package xyz.shiqihao.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("word count")
    val spark = new SparkContext(sparkConf)

    val res = spark.textFile("src/main/resources/wordcount-input")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    println(res.collect().mkString("Array(", ", ", ")"))
  }
}
