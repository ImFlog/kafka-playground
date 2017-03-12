package com.ippon.kafka.basic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Properties;

/**
 * Created by @ImFlog on 04/03/2017.
 */
@Order(value = Ordered.HIGHEST_PRECEDENCE)
@Component
public class BasicProducer implements CommandLineRunner {
    private static final CsvMapper csvMapper = new CsvMapper();
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(BasicProducer.class);

    @Override
    public void run(String... args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("retries", 1);
        // props.put("compression.type", "none"); // Could be gzip, snappy or lz4
        //props.put("batch.size", 16384);
        // props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Init producer from props
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);

        // Read file
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream("/" + args[0])));
        bufferedReader.lines()
                .skip(1L)
                .map(this::readCsv)
                .filter(Objects::nonNull)
                .forEach(effectif -> {
                    try {
                        // Send to kafka
                        // TODO : It's a bit dumb to rewrite in json in kafka
                        kafkaProducer.send(
                                new ProducerRecord<>(
                                        "effectifs",
                                        effectif.getYear(), // Shard by year
                                        jsonMapper.writeValueAsString(effectif)));
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                });
        logger.info("I read everything : {}", bufferedReader.lines().count());
    }

    private Effectif readCsv(String line) {
        // DOES NOT WORK
        /*try {
            return csvMapper.readerFor(Effectif.class)
                    .with(csvMapper.schemaFor(Effectif.class))
                    .readValue(line);
        } catch (IOException e) {
            logger.error("Could not read csv line", e);
        }
        return null;*/
        // TODO : rewrite this, it's ugly
        String[] columns = line.split(";");
        Effectif effectif = new Effectif(
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
        return effectif;
    }
}
