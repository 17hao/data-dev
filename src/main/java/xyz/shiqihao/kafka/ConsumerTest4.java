package xyz.shiqihao.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * 手动提交位移
 */
public class ConsumerTest4 {
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = ConsumerTest2.class.getClassLoader().getResourceAsStream("kafka.properties");
        properties.load(inputStream);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        TopicPartition tp0 = new TopicPartition("test-topic", 0);
        TopicPartition tp1 = new TopicPartition("test-topic", 1);
        TopicPartition tp2 = new TopicPartition("test-topic", 2);
        List<TopicPartition> tps = new ArrayList<>();
        tps.add(tp0);
        tps.add(tp1);
        tps.add(tp2);
        consumer.assign(tps);
        consumer.seek(tp0, 0);
        consumer.seek(tp1, 0);
        consumer.seek(tp2, 0);
        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofSeconds(1))) {
            int offset0 = new Random().nextInt(5);
            int offset1 = new Random().nextInt(5);
            int offset2 = new Random().nextInt(5);

            if (r.offset() == offset0 && r.partition() == 0) {
                System.out.println(r.partition() + " : " + r.value());
                consumer.commitAsync(Collections.singletonMap(tp0, new OffsetAndMetadata(offset0 + 1, Instant.now().toString())), null);
                break;
            }

            if (r.offset() == offset1 && r.partition() == 1) {
                System.out.println(r.partition() + " : " + r.value());
                consumer.commitAsync(Collections.singletonMap(tp1, new OffsetAndMetadata(offset1 + 1, Instant.now().toString())), null);
                break;
            }

            if (r.offset() == offset2 && r.partition() == 2) {
                System.out.println(r.partition() + " : " + r.value());
                consumer.commitAsync(Collections.singletonMap(tp2, new OffsetAndMetadata(offset2 + 1, Instant.now().toString())), null);
                break;
            }
        }

        System.out.println("### next record offset ###");
        System.out.println(consumer.position(tp0));
        System.out.println(consumer.position(tp1));
        System.out.println(consumer.position(tp2));

        System.out.println("### last committed offset ###");
        System.out.println(consumer.committed(Collections.singleton(tp0)));
        System.out.println(consumer.committed(Collections.singleton(tp1)));
        System.out.println(consumer.committed(Collections.singleton(tp2)));
    }
}
