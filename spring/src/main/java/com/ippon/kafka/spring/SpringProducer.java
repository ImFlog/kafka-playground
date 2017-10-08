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
 * Same job as BasicProducerApp except that it relies on Spring Kafka.
 * We just inject a kafkaTemplate for producing instead of building one.
 */
@Component
public class SpringProducer implements CommandLineRunner {

    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static Logger logger = LoggerFactory.getLogger(SpringConsumer.class);

    private static final String TOPIC = "effectifs-spring";
    private Long count = 0L;

    private KafkaTemplate<Integer, String> template;

    @Autowired
    public SpringProducer(KafkaTemplate<Integer, String> template) {
        this.template = template;
    }

    @Override
    public void run(String... args) {
        // Read file
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream("/" + args[0])));
        bufferedReader.lines()
                .skip(1L)
                .map(Effectif::buildEffectif)
                .filter(Objects::nonNull)
                .forEach(this::sendToKafka);
        logger.info("Finished reading {} elements", count);

        // Transaction example
        template.executeInTransaction(t -> {
            t.send("transaction", "Record 1");
            t.send("transaction", "Record 2");
            return true;
        });
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

}
