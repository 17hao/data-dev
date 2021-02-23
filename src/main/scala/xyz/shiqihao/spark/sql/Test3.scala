package xyz.shiqihao.spark.sql

import org.apache.spark.sql.SparkSession

object Test3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Create Dataset")
      .config("spark.master", "local[*]")
      .getOrCreate()

    import spark.implicits._

    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.show()
  }
}

final case class Person(name: String, age: Int)
