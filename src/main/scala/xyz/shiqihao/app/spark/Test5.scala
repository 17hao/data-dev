package xyz.shiqihao.app.spark

import org.apache.spark.sql.SparkSession

object Test5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("test5")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()
  }
}

final case class Person(name: String, age: Int)