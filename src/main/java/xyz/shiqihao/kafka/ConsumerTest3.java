package xyz.shiqihao.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;

/**
 * kafka消费者多分区顺序消费,只能对局部消费到的消息做到顺序消费,无法对全局的消息顺序消费.
 *
 * MAX.PARTITION.FETCH.BYTES = 1MB
 */
public class ConsumerTest3 {
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = ConsumerTest2.class.getClassLoader().getResourceAsStream("kafka.properties");
        properties.load(inputStream);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        TopicPartition tp0 = new TopicPartition("test-topic", 0);
        TopicPartition tp1 = new TopicPartition("test-topic", 1);
        List<TopicPartition> tps = new ArrayList<>();
        tps.add(tp0);
        tps.add(tp1);
        consumer.assign(tps);
        consumer.seek(tp0, 0);
        consumer.seek(tp1, 0);
        PriorityQueue<Record> pq = new PriorityQueue<>((o1, o2) -> (int)(o1.timestamp - o2.timestamp));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> r : records) {
            pq.add(new Record(r.timestamp(), r.value()));
        }
        System.out.println(pq.size());
        while (!pq.isEmpty()) {
            System.out.println(pq.poll());
        }
    }


    static class Record {
        private final long timestamp;
        private final String value;

        public Record(long timestamp, String value) {
            this.timestamp = timestamp;
            this.value = value;
        }

        @Override
        public String toString() {
            return "Record{" +
                    "timestamp=" + timestamp +
                    ", value='" + value + '\'' +
                    '}';
        }
    }
}
