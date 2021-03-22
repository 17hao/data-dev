package xyz.shiqihao.flink

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

object FraudDetector {
  private val SMALL_AMOUNT = 2.00

  private val LARGE_AMOUNT = 5.00
}

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {
  @transient private var flagState: ValueState[java.lang.Boolean] = _

  override def open(parameters: Configuration): Unit = {
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescriptor)
  }

  override def processElement(value: Transaction, ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context, out: Collector[Alert]): Unit = {
    val lastTransactionStateWasSmall = flagState.value()

    if (lastTransactionStateWasSmall != null) {
      if (value.getAmount > FraudDetector.LARGE_AMOUNT) {
        val alert = new Alert()
        alert.setId(value.getAccountId)
        alert.setTnxDetail("amount: " + value.getAmount)
        out.collect(alert)
        flagState.clear()
      }
    }

    if (value.getAmount < FraudDetector.SMALL_AMOUNT) {
      flagState.update(true)
    }
  }
}
