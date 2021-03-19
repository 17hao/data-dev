package xyz.shiqihao.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Pi Estimation
 */
object Pi {
  private val STEP_NUM = 100000

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Pi")
//      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val count = sc.range(1, STEP_NUM).filter { _ =>
      val x = Math.random()
      val y = Math.random()
      x * x + y * y < 1
    }.count()

    println(s"Pi is roughly ${4.0 * count / STEP_NUM}")
  }
}
