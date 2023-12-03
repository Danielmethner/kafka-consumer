package com.danielmethner.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.logging.Logger;

@Component
public class ConsumerRunner implements CommandLineRunner {

    Logger logger = Logger.getLogger(ConsumerRunner.class.getName());

    private KafkaConsumer<String, String> kafkaConsumer;

    @Override
    public void run(String... args) throws Exception {
        logger.info("Initialize Runner!");
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-first-consumer-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(java.util.Arrays.asList("market-data"));

        while (true) {
            kafkaConsumer.poll(java.time.Duration.ofMillis(100))
                .forEach(record -> {
                    logger.info("Received record: " + record.value());
                });
        }
    }
}
