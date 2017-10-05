package com.ippon.kafka.basic;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.utils.MockTime;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;

// TODO : use kafka-junit
public abstract class KafkaAbstractTest {

    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private static final String KAFKA_PORT = "9092";

    private EmbeddedZookeeper zkServer;
    private KafkaServer kafkaServer;
    private ZkClient zkClient;
    private ZkUtils zkUtils;

    public void initKafkaServer(String... topics) throws IOException {
        String zkAddress = setupZookeeper();
        setupKafka(zkAddress);
        createTopics(topics);
    }

    public Properties getDefaultProducerProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BROKERHOST + ":" + KAFKA_PORT);
        properties.put("acks", "all"); // To simplify tests on sent content
        properties.put("retries", 1);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

    public Properties getDefaultConsumerProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BROKERHOST + ":" + KAFKA_PORT);
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "earliest");
        return properties;
    }

    public void closeKafkaServer() {
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }

    private String setupZookeeper() {
        zkServer = new EmbeddedZookeeper();
        String zkAddress = ZKHOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkAddress, 30000, 30000, ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient, false);
        return zkAddress;
    }

    private void setupKafka(String zkAddress) throws IOException {
        Properties brokerProps = new Properties();
        brokerProps.setProperty("auto.create.topics.enable", "true");
        brokerProps.setProperty("zookeeper.connect", zkAddress);
        brokerProps.setProperty("num.partitions", "1");
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST + ":" + KAFKA_PORT);
        KafkaConfig config = new KafkaConfig(brokerProps);

        kafkaServer = TestUtils.createServer(config, new MockTime());
        kafkaServer.startup();
    }

    private void createTopics(String[] topics) {
        // Create topic paths
        if (topics.length > 0) {
            Arrays.stream(topics)
                    .forEach(topic -> AdminUtils.createTopic(
                            zkUtils, topic, 1, 1, new Properties(), RackAwareMode.Enforced$.MODULE$));
        }
    }
}