package com.ippon.kafka.basic;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Sample of a test for a producer.
 * We use kafka-junit to simplify the creation of an in-memory broker.
 */
public class BasicProducerTest {

    private static final String MY_TEST_TOPIC = "my-test-topic";

    @Rule
    public KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create());

    @Test
    public void testProducer() throws Exception {
        KafkaProducer<String, String> stringProducer = kafkaRule.helper().createStringProducer();

        stringProducer.send(new ProducerRecord<>(MY_TEST_TOPIC, "key", "a"));
        stringProducer.send(new ProducerRecord<>(MY_TEST_TOPIC, "key", "b"));
        stringProducer.send(new ProducerRecord<>(MY_TEST_TOPIC, "key", "c"));

        ListenableFuture<List<ConsumerRecord<String, String>>> consume = kafkaRule.helper().consume(MY_TEST_TOPIC, kafkaRule.helper().createStringConsumer(), 3);
        assertThat(consume.get().size()).isEqualTo(3);
    }
}
