package xyz.shiqihao.app.spark

import org.apache.spark.{SparkConf, SparkContext}

object Test2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("test2")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val projectDir = System.getProperty("user.dir")
    val text = sc.textFile(projectDir + "/src/main/resources/data.txt")
    val pairs = text.map(s => (s.split(",")(0), 1))
    val counts = pairs.reduceByKey((a, b) => a + b)
    println(counts.sortByKey().collect().mkString("Array(", ", ", ")"))
    while (true) {}
  }
}
