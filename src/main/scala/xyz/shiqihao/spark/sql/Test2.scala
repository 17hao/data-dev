package xyz.shiqihao.spark.sql

import org.apache.spark.sql.SparkSession

object Test2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("test5")
//      .master("local[*]") // comment this line if submit to yarn
      .getOrCreate()

    spark.read
      .json("hdfs:///user/people.json")
      .createOrReplaceTempView("people")

    val df = spark.sql("SELECT * FROM people")
    df.show()
  }
}