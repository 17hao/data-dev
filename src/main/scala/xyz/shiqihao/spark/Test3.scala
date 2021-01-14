package xyz.shiqihao.spark

import org.apache.spark.{SparkConf, SparkContext}

object Test3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test3")
    val sc = new SparkContext(conf)
    val text = sc.textFile(System.getProperty("user.dir") + "/src/main/resources/data.txt")
    val pairs= text.map { l =>
      val tmp = l.split(",")
      (tmp(0).trim, tmp(1).trim.toInt)
    }.reduceByKey((x, y) => x + y)
    println(pairs.collect().mkString("Array(", ", ", ")"))
  }
}
