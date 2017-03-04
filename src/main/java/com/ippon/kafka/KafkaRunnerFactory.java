package com.ippon.kafka;

import com.ippon.kafka.consumer.BasicConsumer;
import com.ippon.kafka.producer.BasicProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by @ImFlog on 04/03/2017.
 */
@Component
public class KafkaRunnerFactory implements CommandLineRunner {

    private static final int POOL_SIZE = 2;
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(POOL_SIZE);
    private Logger logger = LoggerFactory.getLogger(KafkaRunnerFactory.class);

    private ApplicationContext applicationContext;

    public KafkaRunnerFactory(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public void run(String... args) throws Exception {
        String runningMode = args[0];
        String inputFile = args[1];

        Runnable consummerRunnable = null;
        Runnable producerRunnable = null;

        if ("basic".equals(runningMode)) {
            consummerRunnable = new BasicConsumer();
            producerRunnable = new BasicProducer(inputFile);
        } else if ("spring".equals(runningMode)) {
            // TODO
        } else {
            logger.error("Could not find a supported running mode. The application will now exit.");
            SpringApplication.exit(applicationContext, () -> 0);
        }
        if (producerRunnable != null && consummerRunnable != null) {
            EXECUTOR_SERVICE.submit(consummerRunnable);
            EXECUTOR_SERVICE.submit(producerRunnable);
        }
    }
}
