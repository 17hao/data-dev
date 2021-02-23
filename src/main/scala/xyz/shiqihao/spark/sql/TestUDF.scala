package xyz.shiqihao.spark.sql

import org.apache.spark.sql.SparkSession

object TestUDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Test UDF")
      .config("spark.master", "local[*]")
      .getOrCreate()

    spark.udf.register("myAverage", MyAverage)

    val df = spark.read.json("hdfs:///user/employees.json")
    df.createOrReplaceTempView("employees")
    df.show()

    val result = spark.sql("SELECT myAverage(salary) average_salary FROM employees")
    result.show()
  }
}
