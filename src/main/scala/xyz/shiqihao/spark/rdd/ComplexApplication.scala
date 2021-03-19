package xyz.shiqihao.spark.rdd

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

/*
  (3) MapPartitionsRDD[8] at join at ComplexApplication.scala:25 []
   |  MapPartitionsRDD[7] at join at ComplexApplication.scala:25 []
   |  CoGroupedRDD[6] at join at ComplexApplication.scala:25 []
   |  ShuffledRDD[4] at partitionBy at ComplexApplication.scala:25 []
   +-(3) ParallelCollectionRDD[0] at parallelize at ComplexApplication.scala:17 []
   +-(4) UnionRDD[5] at union at ComplexApplication.scala:25 []
      |  MapPartitionsRDD[2] at map at ComplexApplication.scala:20 []
      |  ParallelCollectionRDD[1] at parallelize at ComplexApplication.scala:20 []
      |  ParallelCollectionRDD[3] at parallelize at ComplexApplication.scala:23 []
 */
object ComplexApplication extends App {
  val spark = SparkSession
    .builder()
    .appName("complex application")
    .config("spark.eventLog.enabled", value = true)
    .config("spark.eventLog.dir", "hdfs://localhost:9000/spark-log")
    .getOrCreate()

  val sc = spark.sparkContext

  val data1 = Seq((1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'))
  val rdd1 = sc.parallelize(data1, 3)

  val data2 = Seq((1, "A"), (2, "B"), (3, "C"), (4, "D"))
  val rdd2 = sc.parallelize(data2, 2).map(x => (x._1, x._2 + x._2))

  val data3 = Seq((3, "X"), (5, "Z"), (3, "W"), (4, "Y"))
  val rdd3 = sc.parallelize(data3, 2)

  val res = rdd1.partitionBy(new HashPartitioner(3)).join(rdd2.union(rdd3))
  println(res.toDebugString)
  res.foreach(println)
}
