# kafka-playground

Steps to run the application :
1. Start a local kafka (on localhost:9092)
2. Create a topic:
 `${KAFKA_PATH}/bin/kafka-topics.sh --zookeeper localhost:2181 --create --partitions 1 --topic effectifs --replication-factor 1`
3. Start the basic application with the first arg specifying the input file for the producer (default to partial_data.csv).
4. Start the stream application for Kafka Stream.