package xyz.shiqihao.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 指定位移消费
 */
public class ConsumerTest2 {
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = ConsumerTest2.class.getClassLoader().getResourceAsStream("kafka.properties");
        properties.load(inputStream);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        TopicPartition tp = new TopicPartition("test-topic", 0);
        consumer.assign(Collections.singleton(tp));
        consumer.seek(tp, 0);
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> record : records) {
            System.out.println(record.offset() + ": " + record.value());
        }
    }
}
