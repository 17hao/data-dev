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
 * 消费者提交的offset是拉取消息的最大offset + 1
 */
public class ConsumerTest1 {
    public static void main(String[] args) throws IOException {
        InputStream inputStream = ConsumerTest1.class.getClassLoader().getResourceAsStream("kafka.properties");
        Properties properties = new Properties();
        properties.load(inputStream);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //consumer.subscribe(Collections.singleton("test-topic"));
        TopicPartition tp = new TopicPartition("test-topic", 0);
        consumer.assign(Collections.singleton(tp));
        System.out.println("### BEFORE CONSUMING ###");
        System.out.println("position: " + consumer.position(tp));
        System.out.println("committed offset: " + consumer.committed(Collections.singleton(tp)));
        System.out.println();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                }
                //consumer.commitSync();
                System.out.println("position: " + consumer.position(tp));
                System.out.println("committed offset: " + consumer.committed(Collections.singleton(tp)));
            }
        }
    }
}
