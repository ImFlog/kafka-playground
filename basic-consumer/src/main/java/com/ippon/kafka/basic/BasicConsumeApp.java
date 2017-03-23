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
 * Created by @ImFlog on 15/02/2017.
 */
public class BasicConsumeApp {

    private static final Logger logger = LoggerFactory.getLogger(BasicConsumeApp.class);
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
        props.put("group.id", "basic");
        // props.put("enable.auto.commit", "true");
        // props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
