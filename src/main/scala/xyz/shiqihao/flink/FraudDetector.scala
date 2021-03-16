package xyz.shiqihao.flink

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

object FraudDetector {
  private val SMALL_AMOUNT = 2.00

  private val LARGE_AMOUNT = 5.00

  private val ONE_MINUTE = 60 * 1000
}

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {
  @transient private var flagState: ValueState[java.lang.Boolean] = _
  @transient private var timerState: ValueState[java.lang.Long] = _

  override def open(parameters: Configuration): Unit = {
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescriptor)

    val timerDescriptor = new ValueStateDescriptor("timer-flag", Types.LONG)
    timerState = getRuntimeContext.getState(timerDescriptor)
  }

  override def processElement(value: Transaction, ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context, out: Collector[Alert]): Unit = {
    val lastTransactionStateWasSmall = flagState.value()

    if (lastTransactionStateWasSmall != null) {
      if (value.getAmount > FraudDetector.LARGE_AMOUNT) {
        val alert = new Alert()
        alert.setId(value.getAccountId)
        alert.setTnxDetail("amount: " + value.getAmount)
        out.collect(alert)
        cleanUp(ctx)
      }
    }

    if (value.getAmount < FraudDetector.SMALL_AMOUNT) {
      flagState.update(true)

      val timer = ctx.timerService().currentProcessingTime() + FraudDetector.ONE_MINUTE
      ctx.timerService().registerProcessingTimeTimer(timer)
      timerState.update(timer)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext, out: Collector[Alert]): Unit = {
    timerState.clear()
    flagState.clear()
  }

  @throws[Exception]
  private def cleanUp(ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context): Unit = {
    val timer = timerState.value()
    ctx.timerService().deleteProcessingTimeTimer(timer)

    timerState.clear()
    flagState.clear()
  }
}
