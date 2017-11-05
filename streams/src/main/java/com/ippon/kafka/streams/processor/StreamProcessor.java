package com.ippon.kafka.streams.processor;

import com.ippon.kafka.streams.serdes.SerdeFactory;
import com.ippon.kafka.streams.serdes.pojos.HashtagEntity;
import com.ippon.kafka.streams.serdes.pojos.TwitterStatus;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class StreamProcessor implements CommandLineRunner {

    private static final String TWITTER_TOPIC = "twitter_json";
    private static final String SCHEDULE_TOPIC = "schedule";

    // Store names
    private static final String TWEET_PER_USER = "tweetPerUser";
    private static final String HASHTAG_PER_USER = "hashtagPerUser";
    private static final String TWEET_PER_TALK = "tweetPerTalk";

    private KafkaStreams streams;

    public void run(String... args) throws Exception {
        // Define custom serdes
        Map<String, Object> serdeProps = new HashMap<>();
        Serde<TwitterStatus> twitterStatusSerde = SerdeFactory.createSerde(TwitterStatus.class, serdeProps);
        Serde<HashtagEntity> hashtagEntitySerde = SerdeFactory.createSerde(HashtagEntity.class, serdeProps);
        Serde<HashSet> hashSetSerde = SerdeFactory.createSerde(HashSet.class, serdeProps);

        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(getProperties());
        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        // 1. Simply read the stream
        KStream<String, TwitterStatus> twitterStream = kStreamBuilder
                .stream(
                        TopologyBuilder.AutoOffsetReset.EARLIEST,
                        Serdes.String(),
                        twitterStatusSerde,
                        TWITTER_TOPIC);
        twitterStream.print();

        // 2. Count the user who tweeted the most about bdx.io
        twitterStream
                .filter((key, value) -> value.getText().toLowerCase().contains("bdxio"))
                .groupBy((key, value) -> value.getUser().getName(), Serdes.String(), twitterStatusSerde)
                .count(TWEET_PER_USER);

        // 3. Used hashtag per user
        twitterStream
                .selectKey((key, value) -> value.getUser().getName())
                .flatMapValues(TwitterStatus::getHashtagEntities)
                .groupByKey(Serdes.String(), hashtagEntitySerde)
                .aggregate(
                        () -> new HashSet<String>(),
                        (key, value, set) -> {
                            set.add(value.getText());
                            return set;
                        },
                        hashSetSerde,
                        HASHTAG_PER_USER
                );

        // Faire une jointure entre la KTable des talk et l'heure des tweets du stream
        KTable<String, String> talksSchedules = kStreamBuilder.table(Serdes.String(), Serdes.String(), SCHEDULE_TOPIC);
        twitterStream
                .selectKey((key, value) -> truncateDate(value))
                .groupByKey(Serdes.String(), twitterStatusSerde)
                .count("test_count")
                .join(
                        talksSchedules,
                        (count, talk) -> talk + ", nombre de tweets = " + count,
                        Serdes.String(),
                        TWEET_PER_TALK
                );

        streams = new KafkaStreams(kStreamBuilder, config);
        // Clean local store between runs
        streams.cleanUp();
        streams.start();
    }

    private String truncateDate(TwitterStatus value) {
        return String.valueOf(LocalDateTime.ofEpochSecond(value.getCreatedAt() / 1000, 0, ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.HOURS)
                .toEpochSecond(ZoneOffset.UTC)) + "000";
    }

    public ReadOnlyKeyValueStore<String, Long> getTweetCountPerUser() {
        return streams.store(TWEET_PER_USER, QueryableStoreTypes.keyValueStore());
    }

    public ReadOnlyKeyValueStore<String, Set<String>> getHashtagPerUser() {
        return streams.store(HASHTAG_PER_USER, QueryableStoreTypes.keyValueStore());
    }

    public ReadOnlyKeyValueStore<String, String> getTweetPerTalk() {
        return streams.store(TWEET_PER_TALK, QueryableStoreTypes.keyValueStore());
    }

    @PreDestroy
    public void destroy() {
        streams.close();
    }

    /**
     * Init stream properties.
     *
     * @return the created stream settings.
     */
    private static Properties getProperties() {
        Properties settings = new Properties();
        // Application ID, used for consumer groups
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "TwitterStreamingTest");
        // Kafka bootstrap server (broker to talk to)
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // default serdes for serializing and deserializing key and value from and to streams
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Enable exactly once
        settings.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        // We can also set Consumer properties
        // settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return settings;
    }
}
