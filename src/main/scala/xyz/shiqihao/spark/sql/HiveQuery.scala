package xyz.shiqihao.spark.sql

import org.apache.spark.sql.{Row, SparkSession}

case class Record(key: Int, value: String)

object HiveQuery {
  private val warehouseLocation = "hdfs:///user/hive/warehouse"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Hive example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("SELECT * FROM spark_table").show()

    sql("SELECT count(*) FROM spark_table").show()

    val sqlDF = sql("SELECT key, value FROM spark_table WHERE key < 10 ORDER BY key")
    val stringDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value :$value"
    }
    stringDS.show()
  }
}
