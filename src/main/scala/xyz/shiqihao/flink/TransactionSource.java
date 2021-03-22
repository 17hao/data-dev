package xyz.shiqihao.flink;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class TransactionSource extends RichParallelSourceFunction<Transaction> {
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {
        Random random = new Random();
        while (isRunning) {
            Transaction transaction = new Transaction(
                    random.nextInt(10000),
                    System.currentTimeMillis(),
                    random.nextInt(8)
            );
            ctx.collect(transaction);
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {

    }
}
