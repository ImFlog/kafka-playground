package com.ippon.kafka.basic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by @ImFlog on 04/03/2017.
 */
@Component
public class BasicConsumer implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(BasicConsumer.class);
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    public void run(String... args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "basic");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("effectifs"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                try {
                    Effectif effectif = jsonMapper.readValue(record.value(), Effectif.class);
                    logger.info("Read message => year : {}, location : {}",
                            effectif.getYear(),
                            effectif.getGeographicUnit());
                } catch (IOException e) {
                    logger.error("Could not deserialize message");
                }
            }
        }
    }
}
