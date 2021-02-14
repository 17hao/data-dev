package xyz.shiqihao.flink;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class Deduplicator extends RichFlatMapFunction<Transaction, Transaction> {
    ValueState<Boolean> keyHasBeenSeen;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("", Types.BOOLEAN);
        keyHasBeenSeen = getRuntimeContext().getState(desc);
    }

    @Override
    public void flatMap(Transaction value, Collector<Transaction> out) throws Exception {
        if (keyHasBeenSeen.value() == null) {
            out.collect(value);
            keyHasBeenSeen.update(true);
        }
    }
}
