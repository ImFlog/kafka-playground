package com.ippon.kafka.basic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ippon.kafka.playground.model.Effectif;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Simple producer example.
 * Read the full_data.csv file, and the content to the "effectifs" topic.
 * <p>
 * Second part shows an example with a transaction.
 */
public class BasicProducerApp {

    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(BasicProducerApp.class);
    private static final String EFFECTIFS_TOPIC = "effectifs";

    private static Long count = 0L;

    public static void main(String[] args) throws InterruptedException {

        // Parse args
        String inputFile = "partial_data.csv";
        boolean withTransaction = false;

        if (args.length >= 1) {
            inputFile = args[0];
        }
        if (args.length == 2) {
            withTransaction = Boolean.parseBoolean(args[2]);
        }
        // Required for lambda parameter
        boolean finalWithTransaction = withTransaction;


        // Read file
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(BasicProducerApp.class.getResourceAsStream("/" + inputFile)));
        List<Effectif> effectifs = bufferedReader.lines()
                .skip(1L) // Skip headers
                .map(Effectif::buildEffectif)
                .collect(Collectors.toList());

        Properties props = buildKafkaProps(withTransaction);
        // Send file content to effectifs.
        try (KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props)) {
            // Globally init transaction support
            if (finalWithTransaction) {
                kafkaProducer.initTransactions();
            }
            for (Effectif effectif : effectifs) {
                if (effectif != null) {
                    sendToKafka(kafkaProducer, effectif, finalWithTransaction);
                }
            }
            logger.info("Finished reading {} elements", count);
        }
    }

    private static void sendToKafka(KafkaProducer<Integer, String> kafkaProducer, Effectif effectif, boolean withTransaction) {
        if (withTransaction) {
            sendWithTransaction(kafkaProducer, effectif);
        } else {
            sendWithoutTransaction(kafkaProducer, effectif);
        }
    }

    private static Properties buildKafkaProps(boolean withTransaction) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Transaction support
        if (withTransaction) {
            props.put("transactional.id", "myAppId");
        }
        // Exactly once
        props.put("enable.idempotence", true);
        /* Equivalent to =>
        props.put("retries", Integer.MAX_VALUE);
        props.put("acks", "all");
        props.put("max.inflight.requests.per.connection", 1);
        */
        return props;
    }

    /**
     * Send to Kafka using transactions.
     * This guarantees e2e Exactly Once as the consumer will not be able to read uncommitted message.
     * Transaction can also be used to send multiple messages.
     */
    private static void sendWithTransaction(KafkaProducer<Integer, String> kafkaProducer, Effectif effectif) {
        try {
            kafkaProducer.beginTransaction();
            kafkaProducer.send(
                    new ProducerRecord<>(
                            EFFECTIFS_TOPIC,
                            effectif.getYear(), // Shard by year
                            jsonMapper.writeValueAsString(effectif)));
            kafkaProducer.commitTransaction();
            count++;
            if (count % 10000 == 0) {
                logger.info("Sent {} messages, last => year : {}, location : {}",
                        count, effectif.getYear(), effectif.getGeographicUnit());
            }
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
        } catch (ProducerFencedException e) {
            // Zombie fencing, we close the producer
            kafkaProducer.close();
        } catch (KafkaException e) {
            // In every other case, just abort transaction
            kafkaProducer.abortTransaction();
        }
    }

    /**
     * Send to kafka without using transactions.
     * Note that the send is still idempotent but the consumer might be able to read uncommitted messages
     * (not yet cleaned).
     */
    private static void sendWithoutTransaction(KafkaProducer<Integer, String> kafkaProducer, Effectif effectif) {
        try {
            kafkaProducer.send(
                    new ProducerRecord<>(
                            EFFECTIFS_TOPIC,
                            effectif.getYear(), // Shard by year
                            jsonMapper.writeValueAsString(effectif)));
            count++;
            if (count % 10000 == 0) {
                logger.info("Sent {} messages, last => year : {}, location : {}",
                        count, effectif.getYear(), effectif.getGeographicUnit());
            }
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
        }
    }
}