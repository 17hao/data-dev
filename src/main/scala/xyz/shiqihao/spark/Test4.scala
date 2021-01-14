package xyz.shiqihao.spark

import org.apache.spark.sql.SparkSession

object Test4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("test4")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.json(System.getProperty("user.dir") + "/src/main/resources/people.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    df.filter($"age" > 21).show()
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
  }
}
