package com.ippon.kafka.basic;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/**
 * Sample of a test for a consumer.
 * We use kafka-junit to simplify the creation of an in-memory broker.
 */
public class BasicConsumerTest {

    private static final String MY_TEST_TOPIC = "my-test-topic";
    @Rule
    public KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create());

    @Test
    public void testConsumer() throws Exception {
        kafkaRule.helper().produceStrings(MY_TEST_TOPIC, "a", "b", "c", "d", "e");
        KafkaConsumer<String, String> consumer = kafkaRule.helper().createStringConsumer();
        consumer.subscribe(Collections.singletonList(MY_TEST_TOPIC));

        ConsumerRecords<String, String> poll;
        do {
            poll = consumer.poll(10000);
        } while (poll == null || poll.isEmpty());

        assertThat(poll.count()).isEqualTo(5);
    }

}