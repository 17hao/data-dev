package xyz.shiqihao.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

class TransactionSource implements SourceFunction<Transaction> {
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {
        final Transaction transaction = new Transaction();
        transaction.setAccountId(123456L);

        while (isRunning) {
            ctx.collect(transaction);
        }
    }

    @Override
    public void cancel() {

    }
}
