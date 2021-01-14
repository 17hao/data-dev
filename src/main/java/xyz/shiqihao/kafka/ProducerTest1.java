package xyz.shiqihao.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
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
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", args[0]);
        RecordMetadata metadata = producer.send(record).get();
        System.out.println(metadata.topic() + " -> " + metadata.partition() + " -> " + metadata.offset());
    }
}
