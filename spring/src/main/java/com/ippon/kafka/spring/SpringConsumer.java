package com.ippon.kafka.spring;

import com.ippon.kafka.spring.model.Effectif;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.stereotype.Component;

/**
 * Created by @ImFlog on 04/03/2017.
 */
@Component
@EnableBinding(Sink.class)
public class SpringConsumer {

    private static Logger logger = LoggerFactory.getLogger(SpringConsumer.class);

    @StreamListener(Sink.INPUT)
    public void processData(Effectif effectif) {
        logger.info("Read message => year : {}, location : {}",
                effectif.getYear(),
                effectif.getGeographicUnit());
    }
}
