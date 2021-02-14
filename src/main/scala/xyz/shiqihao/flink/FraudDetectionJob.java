package xyz.shiqihao.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Fraud detection job
 */
public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Transaction> transactions = env
                .addSource(new TransactionSource())
                .name("transactions");

        DataStream<Alert> alerts = transactions
                .keyBy(Transaction::getAccountId)
                .flatMap(new Deduplicator())
                .process(new FraudDetector())
                .name("fraud-detector");

        alerts.addSink(new AlertSink()).name("send-alerts");

        env.execute("Fraud Detection");
    }
}

class FraudDetector extends ProcessFunction<Transaction, Alert> {
    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;

    private static final double LARGE_AMOUNT = 500.00;

    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {
        Alert alert = new Alert();
        alert.setId(transaction.getAccountId());

        collector.collect(alert);
    }
}

