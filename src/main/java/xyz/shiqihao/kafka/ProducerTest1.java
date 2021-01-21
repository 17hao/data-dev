package xyz.shiqihao.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * messages flow:
 * ProducerInterceptors => Serializer => Partitioner =>
 * RecordAccumulator(ProducerBatch) => Sender
 */
public class ProducerTest1 {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Properties properties = new Properties();
        InputStream inputStream = ProducerTest1.class.getClassLoader().getResourceAsStream("kafka.properties");
        properties.load(inputStream);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", Instant.now().toString(), UUID.randomUUID().toString());
            RecordMetadata metadata = producer.send(record).get();
            System.out.println(metadata.topic() + " -> " + metadata.partition() + " -> " + metadata.offset());
        }
    }
}
