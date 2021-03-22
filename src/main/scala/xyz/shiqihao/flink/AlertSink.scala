package xyz.shiqihao.flink

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.slf4j.LoggerFactory
import xyz.shiqihao.flink.AlertSink.LOG

import java.time.LocalDateTime

class AlertSink extends SinkFunction[Alert] {
  override def invoke(value: Alert, context: SinkFunction.Context[_]): Unit = {
    val content = LocalDateTime.now + " " + value
    println(content)
    LOG.info(content)
  }
}

object AlertSink {
  private val LOG = LoggerFactory.getLogger(AlertSink.getClass)
}
