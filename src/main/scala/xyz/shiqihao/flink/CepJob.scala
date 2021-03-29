package xyz.shiqihao.flink

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object CepJob {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val pattern = Pattern.begin[Transaction]("start")
      .next("middle").where(_.getAmount < 10)
      .followedBy("end").where(_.getAmount > 1)
      .within(Time.seconds(10))

    val input: DataStream[Transaction] = env
      .addSource(new TransactionSource)
      .name("transaction source")

    val patternStream = CEP.pattern(input, pattern)

    val res = patternStream.select(out => createAlert(out.head._2.head))
    res.addSink(new AlertSink).name("alert sink")

    env.execute()
  }

  def createAlert(tnx: Transaction): Alert = {
    val alert = new Alert()
    alert.setId(tnx.getAccountId)
    alert
  }
}