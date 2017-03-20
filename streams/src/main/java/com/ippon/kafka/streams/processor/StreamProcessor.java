package com.ippon.kafka.streams.processor;

import com.ippon.kafka.streams.model.Effectif;
import com.ippon.kafka.streams.serdes.SerdeFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by @ImFlog on 12/03/2017.
 */
@Component
public class StreamProcessor implements CommandLineRunner {

    public static final String STREAM_APPLICATION_NAME = "StreamingTest";
    private static final String inputTopic = "effectifs";

    @Override
    public void run(String... strings) throws Exception {
        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(getProperties());


        // ------------------------------------------------------
        //       Define Serialization / Deserialization
        // ------------------------------------------------------
        Map<String, Object> serdeProps = new HashMap<>();
        Serde<Effectif> effectifSerde = SerdeFactory.createSerde(Effectif.class, serdeProps);

        // Building Kafka Streams Model
        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        // Init common topology
        KStream<Integer, Effectif> effectifsStream = kStreamBuilder
                .stream(Serdes.Integer(), effectifSerde, inputTopic)
                .filter((year, effectif) -> !"TOTAL".equals(effectif.getGroup()))
                .filter((year, effectif) -> "Commune".equals(effectif.getGeographicLevel()));

        // ------------------------------------------------------
        //              Define streams topologies               |
        // ------------------------------------------------------
        //          1. Global student count per year            |
        // ------------------------------------------------------
        KTable<Integer, Double> studentsPerYear = effectifsStream
                .map((year, effectif) -> new KeyValue<>(year, effectif.getStudentCount()))
                .groupByKey(Serdes.Integer(), Serdes.Double())
                .aggregate(
                        () -> 0D,
                        (aggKey, value, aggregate) -> aggregate + value,
                        Serdes.Double(),
                        "studentsPerYear");

        // We can easily send to another topic
        studentsPerYear.to(Serdes.Integer(), Serdes.Double(), "studentsPerYear");
        // Or print the result
        studentsPerYear.print();

        // ------------------------------------------------------
        //              2. Formation communes count             |
        // ------------------------------------------------------
        effectifsStream
                .groupBy((year, effectif) -> effectif.getGroup(), Serdes.String(), effectifSerde)
                .count("countPerFormation")
                .print();


        // ------------------------------------------------------
        //              3. Student count variation
        // ------------------------------------------------------
        // Students per commune 2014
        KTable<String, Double> studentsPerCommune2014 = effectifsStream
                .filter((year, effectif) -> year == 2014)
                .groupBy((year, effectif) -> effectif.getGeographicUnit(), Serdes.String(), effectifSerde)
                .aggregate(
                        () -> 0D,
                        ((aggKey, value, aggregate) ->
                                aggregate + (value.getStudentCount() != null ? value.getStudentCount() : 0)),
                        Serdes.Double(),
                        "studentsPerCommune2014");

        // Students per commune 2015
        KTable<String, Double> studentsPerCommune2015 = effectifsStream
                .filter((year, effectif) -> year == 2015)
                .groupBy((year, effectif) -> effectif.getGeographicUnit(), Serdes.String(), effectifSerde)
                .aggregate(
                        () -> 0D,
                        ((aggKey, value, aggregate) ->
                                aggregate + (value.getStudentCount() != null ? value.getStudentCount() : 0)),
                        Serdes.Double(),
                        "studentsPerCommune2015");

        // Student count variation between 2014 and 2015
        studentsPerCommune2014
                .leftJoin(studentsPerCommune2015, this::calculateEvolution)
                .print("Evolution 2014 2015");

        // ------------------------------------------------------
        //                  Start processing
        // ------------------------------------------------------
        System.out.println("Starting Kafka Streams");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.start();
        System.out.println("Kafka Streams started");
    }

    private Double calculateEvolution(Double previousValue, Double newValue) {
        if (previousValue == null && newValue == null) {
            return 0D;
        }
        if (previousValue == null) {
            return 100D;
        }
        if (newValue == null) {
            return -100D;
        }

        return  (newValue / previousValue) * 100;
    }

    /**
     * Init stream properties.
     * @return the created stream settings.
     */
    private static Properties getProperties() {
        Properties settings = new Properties();
        // Application ID, used for consumer groups
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, STREAM_APPLICATION_NAME);
        // Kafka bootstrap server (broker to talk to)
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // default serdes for serializing and deserializing key and value from and to streams
        settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // We can also set Consumer properties
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return settings;
    }
}
