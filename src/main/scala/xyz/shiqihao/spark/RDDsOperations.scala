package xyz.shiqihao.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 1. Sorting word count in descending order.
 * 2. Retrieving top 10 words
 */
object RDDsOperations {
  private val filePath = "src/main/resources/wordcount-input"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("sort word count")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val text = sc.textFile(filePath)
    val reducedByKey = text
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)

    val res1 = reducedByKey.sortBy(_._2, ascending = false)
    println(res1.toDebugString)
    println(res1.collect().mkString("Array(", ", ", ")"))

    val res2 = reducedByKey.aggregateByKey(0)(_ + _, _ + _).sortBy(_._2, ascending = false)
    println(res2.collect().mkString("Array(", ", ", ")"))

    val res3 = sc.parallelize(Array(("a", 1), ("a", 4), ("a", 7), ("b", 2)))
      .aggregateByKey(new mutable.HashSet[Int]())(_ + _, _ ++ _)
    println(res3.toDebugString)
    println(res3.collect().mkString("Array(", ", ", ")"))

    val res4 = reducedByKey.takeOrdered(2)(Ordering[Int].reverse.on(_._2))
    println(res4.mkString("Array(", ", ", ")"))

//    val  res5 = reducedByKey.combineByKey()
  }
}
