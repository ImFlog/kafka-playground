package com.ippon.kafka.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ippon.kafka.playground.model.Effectif;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Objects;

/**
 * Created by @ImFlog on 04/03/2017.
 */
@Component
public class SpringProducer implements CommandLineRunner {

    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static Logger logger = LoggerFactory.getLogger(SpringConsumer.class);

    private static final String TOPIC = "effectifs-spring";
    private Long count = 0L;

    @Autowired
    private KafkaTemplate<Integer, String> template;

    @Override
    public void run(String... args) {
        // Read file
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream("/" + args[0])));
        bufferedReader.lines()
                .skip(1L)
                .map(this::readCsv)
                .filter(Objects::nonNull)
                .forEach(this::sendToKafka);
        logger.info("Finished reading {} elements", count);
    }

    private void sendToKafka(Effectif effectif) {
        try {
            template.send(TOPIC, effectif.getYear(), jsonMapper.writeValueAsString(effectif));
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
