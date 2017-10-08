package com.ippon.kafka.basic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ippon.kafka.playground.model.Effectif;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;


/**
 * Simple consumer example.
 * Read the effectifs topic, deserialize the result and count the read items.
 */
public class BasicConsumerApp {

    private static final Logger logger = LoggerFactory.getLogger(BasicConsumerApp.class);
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private static Long count = 0L;

    public static void main(String[] args) throws InterruptedException {
        Properties props = buildKafkaProps();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("effectifs"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                try {
                    Effectif effectif = jsonMapper.readValue(record.value(), Effectif.class);
                    count++;
                    if (count % 10000 == 0) {
                        logger.info("Read {} messages, latest element: {year={}, nbStudents={}, location={}}",
                                count,
                                effectif.getYear(),
                                effectif.getStudentCount(),
                                effectif.getGeographicUnit());
                    }
                } catch (IOException e) {
                    logger.error("Could not deserialize message");
                }
            }
        }
    }

    private static Properties buildKafkaProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // Consumer group ID
        props.put("group.id", "basic");

        // Enabling transaction support if necessary.
        props.put("isolation.level", "read_committed");
        return props;
    }
}
