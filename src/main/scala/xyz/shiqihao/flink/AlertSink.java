package xyz.shiqihao.flink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

class AlertSink implements SinkFunction<Alert> {
    @Override
    public void invoke(Alert value, Context context) throws Exception {
        System.out.println(value.getId());
    }
}
