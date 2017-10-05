package com.ippon.kafka.basic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ippon.kafka.playground.model.Effectif;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by @ImFlog on 15/02/2017.
 */
public class BasicProduceApp {

    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(BasicProduceApp.class);

    private static Long count = 0L;

    public static void main(String[] args) throws InterruptedException {
        Properties props = buildKafkaProps();

        // Init producer from props
        try (KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props)) {
            // Read file
            BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(BasicProduceApp.class.getResourceAsStream("/" + args[0])));
            bufferedReader.lines()
                .skip(1L) // Skip headers
                .map(Effectif::buildEffectif)
                .filter(Objects::nonNull)
                .forEach(effectif -> sendToKafka(kafkaProducer, effectif));
            logger.info("Finished reading {} elements", count);
        }

        // TODO : Add transaction send example.
    }

    private static void sendToKafka(KafkaProducer<Integer, String> kafkaProducer, Effectif effectif) {
        try {
            kafkaProducer.send(
                new ProducerRecord<>(
                    "effectifs",
                    effectif.getYear(), // Shard by year
                    jsonMapper.writeValueAsString(effectif)));
            count++;
            if (count % 10000 == 0) {
                logger.info("Sent {} messages, last => year : {}, location : {}",
                    count, effectif.getYear(), effectif.getGeographicUnit());
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static Properties buildKafkaProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Exactly once
        props.put("enable.idempotence", true);
        /* Equivalent to =>
        props.put("retries", Integer.MAX_VALUE);
        props.put("acks", "all");
        props.put("max.inflight.requests.per.connection", 1);
        */
        return props;
    }


}
