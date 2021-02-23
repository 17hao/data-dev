package xyz.shiqihao.spark.sql

import org.apache.spark.sql.SparkSession

object Test1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("test-1")
      .config("spark.master", "local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.json(System.getProperty("user.dir") + "/src/main/resources/people.json")

    df.show()

    df.printSchema()

    df.select("name").show()

    df.groupBy("age").count().show()

    df.filter($"age" > 21).show()
  }
}
