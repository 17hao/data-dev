package xyz.shiqihao.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaBrokersAdmin {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        InputStream inputStream = KafkaBrokersAdmin.class.getClassLoader().getResourceAsStream("kafka.properties");
        Properties properties = new Properties();
        properties.load(inputStream);
        AdminClient adminClient = AdminClient.create(properties);
        Collection<TopicListing> topics = adminClient.listTopics().listings().get();
        TopicListing testTopic = null;
        for (TopicListing t : topics) {
            if (t.name().equals("test-topic")) {
                testTopic = t;
            }
            System.out.println(t);
        }
        if (!topics.contains(testTopic)) {
            CreateTopicsResult res = adminClient.createTopics(Collections.singleton(new NewTopic("test-topic", 1, (short) 1)));
            System.out.println(res.all().get());
        }
    }
}
