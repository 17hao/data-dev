package xyz.shiqihao.spark.rdd

import org.apache.spark.sql.SparkSession

import java.time._

/**
 * 数据端笔试题
 * <p>
 * 现有如下数据文件需要处理
 * 格式：CSV
 * 位置：hdfs://myhdfs/input.csv
 * 大小：100GB
 * 字段：用户ID，位置ID，开始时间，停留时长(分钟）
 * <p>
 * 4行样例：
 * <p>
 * UserA,LocationA,2018-01-01 08:00:00,60
 * UserA,LocationA,2018-01-01 09:00:00,60
 * UserA,LocationB,2018-01-01 10:00:00,60
 * UserA,LocationA,2018-01-01 11:00:00,60
 * <p>
 * 解读：
 * <p>
 * 样例数据中的数据含义是：
 * 用户UserA，在LocationA位置，从8点开始，停留了60分钟
 * 用户UserA，在LocationA位置，从9点开始，停留了60分钟
 * 用户UserA，在LocationB位置，从10点开始，停留了60分钟
 * 用户UserA，在LocationA位置，从11点开始，停留了60分钟
 * <p>
 * 该样例期待输出：
 * UserA,LocationA,2018-01-01 08:00:00,120
 * UserA,LocationB,2018-01-01 10:00:00,60
 * UserA,LocationA,2018-01-01 11:00:00,60
 * <p>
 * 处理逻辑：
 * 1 对同一个用户，在同一个位置，连续的多条记录进行合并
 * 2 合并原则：开始时间取最早时间，停留时长加和
 * <p>
 * 要求：请使用Spark、MapReduce或其他分布式计算引擎处理
 */
object InterviewApp {
  private val filePath = "src/main/resources/events.csv"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("interview app")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val accumulator = spark.sparkContext.longAccumulator("partition number")

    val originalDF = spark.read.csv(filePath)
      .map { e =>
        val startAt = getTimestamp(e.getString(2))
        Event(e.getString(0), e.getString(1), startAt, startAt + 60 * Integer.parseInt(e.getString(3)))
      }
      .repartition($"user", $"location")

    originalDF.foreachPartition(p => if (p.nonEmpty) accumulator.add(1))
    val df = originalDF.repartitionByRange(accumulator.value.toInt, $"user", $"location").sortWithinPartitions($"startAt")

    val res = df.mapPartitions { events =>
      println(events.isEmpty)
//      println(events)
      var res = Seq[Event]()
      var startEvent = events.next()
      var currentEvent = startEvent
      while (events.hasNext) {
        val nextEvent = events.next()
        if (currentEvent.endAt >= nextEvent.startAt) {
          currentEvent = nextEvent
        } else {
          res = res :+ startEvent.copy(endAt = currentEvent.endAt)
          startEvent = nextEvent
          currentEvent = nextEvent
        }
      }
      res = res :+ startEvent.copy(endAt = currentEvent.endAt)
      res.iterator
    }
      .map(e => Array(e.user, e.location, toLocalDateTime(e.startAt), (e.endAt - e.startAt) / 60).mkString(","))
      .collect()

    println(res.mkString("\n"))
  }

  private def getTimestamp(datetime: String) = {
    val dt = datetime.split("\\s+")
    LocalDate.parse(dt(0)).atTime(LocalTime.parse(dt(1))).toEpochSecond(ZoneOffset.ofHours(8))
  }

  private def toLocalDateTime(timestamp: Long) = {
    LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneOffset.ofHours(8))
  }
}

case class Event(user: String, location: String, startAt: Long, endAt: Long)
