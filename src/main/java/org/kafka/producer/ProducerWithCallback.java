package org.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {
    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);
    public static void main(String[] args) {
        //Kafka properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("batch.size", "100");

        //Sticky Partitioner
        //producer
        try (org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int j = 0; j < 10; j++) {
                for (int i = 0; i < 30; i++) {
                    logger.info("Starting producer");
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>("my-topic", "Producing to topic" + i);
                    //producer with callback
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Error sending record", exception);
                        } else {
                            logger.info("Sent record to topic \n topic:{} \n partition:{} \n offset:{} \n timestamp:{} \n",
                                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                        }
                    });
                    logger.info("Sent record to topic");
                    logger.info("Ending producer");
                    Thread.sleep(500);
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}