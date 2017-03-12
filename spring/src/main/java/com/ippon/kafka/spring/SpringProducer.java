package com.ippon.kafka.spring;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Objects;

/**
 * Created by @ImFlog on 04/03/2017.
 */
@Component
@EnableBinding(Source.class)
public class SpringProducer implements CommandLineRunner {

    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static Logger logger = LoggerFactory.getLogger(SpringConsumer.class);
    @Autowired
    private Source source;

    @Override
    public void run(String... args) {
        // Read file
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream("/" + args[0])));
        bufferedReader.lines()
                .skip(1L)
                .map(this::readCsv)
                .filter(Objects::nonNull)
                .forEach(effectif -> {
                    // Send to kafka
                    // TODO : KEY SERIALIZATION ERROR !!!!!!
                    try {
                        source.output()
                                .send(MessageBuilder.withPayload(jsonMapper.writeValueAsString(effectif))
                                        .setHeader(KafkaHeaders.MESSAGE_KEY, effectif.getYear())
                                        .build());
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
