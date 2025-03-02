package org.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWithShutdownHook {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWithShutdownHook.class);
    public static void main(String[] args) {
        String groupId = "my-topic-group";
        String topic = "my-topic";
        //Kafka properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", groupId);
        props.put("auto.offset.reset", "earliest");//none,earliest,latest


        org.apache.kafka.clients.consumer.Consumer<String, String> consumer = new KafkaConsumer<>(props);
        //getting the current thread
        Thread mainThread = Thread.currentThread();
        //adding a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down...");
            consumer.wakeup();
            try {
                mainThread.join();}
            catch (InterruptedException e) {logger.error("error occurred {}", e.getMessage());}
        }));

        //producer
        try {
            consumer.subscribe(List.of(topic));
            while (true) {
                //Polling, waiting for a second to retrieve data
                logger.info("Polling");
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("topic: {} partition: {} offset: {}",
                            record.topic(), record.partition(), record.offset());
                    logger.info("key: {} value: {}", record.key(), record.value());
                }
            }

        } catch (WakeupException e) {
            logger.info("consumer is shutting down...");
        } catch (Exception e) {
            logger.error("error occurred: {}", e.getMessage());
        } finally {
            logger.info("Shutting down consumer gracefully...");
            consumer.close();
        }
    }
}