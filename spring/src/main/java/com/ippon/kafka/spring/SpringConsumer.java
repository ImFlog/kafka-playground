package com.ippon.kafka.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ippon.kafka.playground.model.Effectif;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Same job as com.ippon.kafka.basic.BasicConsumerApp except that it relies on Spring Kafka.
 */
@Component
public class SpringConsumer {

    private static final Logger logger = LoggerFactory.getLogger(SpringConsumer.class);
    private static final String TOPIC = "effectifs-spring";

    private final ObjectMapper mapper = new ObjectMapper();

    private Long count = 0L;

    /**
     * Use Kafka listener annotation to read message from the effectifs-spring topic.
     */
    @KafkaListener(topics = TOPIC)
    public void processData(ConsumerRecord<Integer, String> consumerRecord) {
        try {
            Effectif effectif = mapper.readValue(consumerRecord.value(), Effectif.class);
            count++;
            if (count % 10000 == 0) {
                logger.info("Read {} messages, last => year : {}, location : {}",
                        count,
                        effectif.getYear(),
                        effectif.getGeographicUnit());
            }
        } catch (IOException e) {
            logger.error("Could not deserialize message");
        }
    }
}
