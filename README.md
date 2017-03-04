# kafka-playground

Steps to run the application :
1. Start a local kafka (on localhost:9092)
2. Create a topic:
 `${KAFKA_PATH}/bin/kafka-topics.sh --zookeeper localhost:2181 --create --partitions 1 --topic effectifs --replication-factor 1`
3. Start the application with args.

- The first arg specify the type of consumer you want to launch.
- The second one specify the input file for the producer.

TODO : IMPROVE DOCUMENTATION + USE KAFKA IN DOCKER (BUG ON MY MACHINE)