package xyz.shiqihao.flink

import org.apache.flink.streaming.api.scala._

object FraudDetectionJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val transactions: DataStream[Transaction] = env
      .addSource(new TransactionSource)
      .name("transaction source")

    val alerts: DataStream[Alert] = transactions
      .keyBy(_.getAccountId)
      .process(new FraudDetector)
      .name("fraud detector")

    alerts
      .addSink(new AlertSink)
      .name("alert sink")

    env.execute("Fraud Detection Job")
  }
}
