package com.ippon.kafka.basic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ippon.kafka.playground.model.Effectif;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Properties;

/**
 * Created by @ImFlog on 04/03/2017.
 */
public class BasicProducer implements Runnable {
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(BasicProducer.class);

    public static String INPUT_FILE = "partial_data.csv";

    private Long count = 0L;

    @Override
    public void run() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        // props.put("acks", "1");
        // props.put("retries", 1);
        // props.put("compression.type", "none"); // Could be gzip, snappy or lz4
        //props.put("batch.size", 16384);
        // props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Init producer from props
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);

        // Read file
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream("/" + INPUT_FILE)));
        bufferedReader.lines()
                .skip(1L) // Skip headers
                .map(this::readCsv)
                .filter(Objects::nonNull)
                .forEach(effectif -> sendToKafka(kafkaProducer, effectif));
        logger.info("Finished reading {} elements", count);
    }

    private void sendToKafka(KafkaProducer<Integer, String> kafkaProducer, Effectif effectif) {
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

    private Effectif readCsv(String line) {
        String[] columns = line.split(";");
        return new Effectif(
                Integer.parseInt(columns[0]),
                columns[1],
                columns[2],
                columns[3],
                columns[4],
                columns[5],
                columns[6],
                columns[7],
                columns[8],
                columns[9],
                columns[10].isEmpty() ? null : Double.parseDouble(columns[10]),
                columns[11],
                columns[12].isEmpty() ? null : Double.parseDouble(columns[12]),
                columns[13],
                columns[14].isEmpty() ? null : Double.parseDouble(columns[14]),
                columns[15],
                columns[16],
                columns[17],
                columns[18],
                columns[19],
                columns[20]);
    }
}
