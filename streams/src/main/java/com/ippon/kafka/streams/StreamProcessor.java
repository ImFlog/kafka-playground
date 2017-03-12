package com.ippon.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by @ImFlog on 12/03/2017.
 */
@Component
public class StreamProcessor implements CommandLineRunner {


    @Override
    public void run(String... strings) throws Exception {
        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(getProperties());

        Map<String, Object> serdeProps = new HashMap<>();
        Serializer<Effectif> effectifSerializer = new JsonPOJOSerializer<>();
        Deserializer<Effectif> effectifDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", Effectif.class);
        serdeProps.put("JsonPOJOClass", Effectif.class);
        effectifDeserializer.configure(serdeProps, false);
        Serde<Effectif> effectifSerde = Serdes.serdeFrom(effectifSerializer, effectifDeserializer);

        // building Kafka Streams Model
        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        KStream<Integer, Effectif> effectifsStream = kStreamBuilder.stream(Serdes.Integer(), effectifSerde, "effectifs");


        // THIS IS THE CORE OF THE STREAMING ANALYTICS:
        KTable<String, EffectifTop3> top3Communes = effectifsStream
                .filter((k, effectif) -> "Commune".equals(effectif.getGeographicLevel()))
                .selectKey((k, effectif) -> effectif.getGeographicUnit())
                .groupByKey()
                .aggregate(
                        EffectifTop3::new,
                        (continent, countryMsg, top3) -> {
                            top3.nrs[3] = new EffectifMessage(countryMsg);
                            Arrays.sort(
                                    top3.nrs, (a, b) -> {
                                        // in the initial cycles, not all nrs element contain a EffectifMessage object
                                        if (a == null) return 1;
                                        if (b == null) return -1;
                                        // with two proper EffectifMessage objects, do the normal comparison
                                        return Double.compare(b.effectif, a.effectif);
                                    }
                            );
                            top3.nrs[3] = null;
                            return (top3);
                        },
                        Serdes.serdeFrom(new JsonPOJOSerializer<>(), new JsonPOJODeserializer<>()),
                        "top3PerCommune"
                );

        // SEND TO STREAM TOPIC
        top3Communes.to(
                Serdes.String(),
                Serdes.serdeFrom(new JsonPOJOSerializer<>(), new JsonPOJODeserializer<>())
                , "stream");

        // PRINT RESULT
        top3Communes.mapValues((top3) -> " 1. " + top3.nrs[0].commune + " - " + top3.nrs[0].effectif
                + ((top3.nrs[1] != null) ? ", 2. " + top3.nrs[1].commune + " - " + top3.nrs[1].effectif : "")
                + ((top3.nrs[2] != null) ? ", 3. " + top3.nrs[2].commune + " - " + top3.nrs[2].effectif : "")
        ).print();

        System.out.println("Starting Kafka Streams");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.start();
        System.out.println("Now started CountriesStreams Example");
    }

    private static Properties getProperties() {
        Properties settings = new Properties();
        // Set a few key parameters
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "Stream-application");
        // Kafka bootstrap server (broker to talk to)
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Apache ZooKeeper instance keeping watch over the Kafka cluster; ubuntu is the host name for my VM running Kafka, port 2181 is where the ZooKeeper listens
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // default serdes for serialzing and deserializing key and value from and to streams in case no specific Serde is specified
        settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return settings;
    }

    private static class EffectifMessage {
        /* the JSON messages produced to the stream Topic have this structure:
        this class needs to have at least the corresponding fields to deserialize the JSON messages into
        */

        public EffectifMessage(Effectif effectif) {
            this.commune = effectif.getGeographicUnit();
            this.effectif = effectif.getStudentCount();
        }

        public String commune;
        public Double effectif;
    }

    private static class EffectifTop3 {

        public EffectifMessage[] nrs = new EffectifMessage[4];

        public EffectifTop3() {
        }
    }

    private static class JsonPOJODeserializer<T> implements Deserializer<T> {
        private ObjectMapper objectMapper = new ObjectMapper();

        private Class<T> tClass;

        /**
         * Default constructor needed by Kafka
         */
        public JsonPOJODeserializer() {
        }

        @SuppressWarnings("unchecked")
        @Override
        public void configure(Map<String, ?> props, boolean isKey) {
            tClass = (Class<T>) props.get("JsonPOJOClass");
        }

        @Override
        public T deserialize(String topic, byte[] bytes) {
            if (bytes == null)
                return null;

            T data;
            try {
                data = objectMapper.readValue(bytes, tClass);
            } catch (Exception e) {
                throw new SerializationException(e);
            }

            return data;
        }

        @Override
        public void close() {

        }
    }

    public class JsonPOJOSerializer<T> implements Serializer<T> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        private Class<T> tClass;

        /**
         * Default constructor needed by Kafka
         */
        public JsonPOJOSerializer() {

        }

        @SuppressWarnings("unchecked")
        @Override
        public void configure(Map<String, ?> props, boolean isKey) {
            tClass = (Class<T>) props.get("JsonPOJOClass");
        }

        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null)
                return null;

            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }

        @Override
        public void close() {
        }

    }

}
