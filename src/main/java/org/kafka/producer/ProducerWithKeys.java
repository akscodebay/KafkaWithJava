package org.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {
    private static final Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);
    public static void main(String[] args) {
        //Kafka properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        //producer
        try (org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 20; i++) {
                String key = String.valueOf(i%3);
                ProducerRecord<String, String> record =
                        new ProducerRecord<>("my-topic", key, "Producing to topic" + i);
                int finalI = i;
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Error sending record", exception);
                    } else {
                        logger.info("Iteration:{} :Key: {} Partition: {}", finalI, key, metadata.partition());
                    }
                });
            }
        }
    }
}