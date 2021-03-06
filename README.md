# kafka-playground
For all the examples, a local Kafka broker should be started.

### Basic consumers / producers
1. Create the topic: `${KAFKA_PATH}/bin/kafka-topics.sh --zookeeper localhost:2181 --create --partitions 1 --topic effectifs --replication-factor 1`
2. Start the basic producer application with the first arg specifying the partial_data.csv or full_data.csv.
You can also specify if you want to send each message in a transaction (slow and a bit silly for the use case). 
3. Start the basic consumer application.

Same configuration applies for the Spring example.

### Kafka Stream
For the Kafka Stream example, you will have to load a twitter feed into your Kafka broker.
To do so:
1. Create the twitter topic `${KAFKA_PATH}/bin/kafka-topics.sh --zookeeper localhost:2181 --create --partitions 1 --topic twitter_json --replication-factor 1`
2. Clone the following [repo](https://github.com/jcustenborder/kafka-connect-twitter)
3. Build the connector `mvn clean package`
4. Make it visible for Kafka connect `export CLASSPATH="$(find target/kafka-connect-target/usr/share/java -type f -name '*.jar' | tr '\n' ':')`
5. Create a .properties file for the Kafka connector (with your twitter access token)
```
name=twitter_source_json
connector.class=com.github.jcustenborder.kafka.connect.twitter.TwitterSourceConnector
twitter.oauth.accessToken=xxxx
twitter.oauth.consumerSecret=xxxxx
twitter.oauth.consumerKey=xxxx
twitter.oauth.accessTokenSecret=xxxxx
process.deletes=false
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
key.converter.schemas.enable=false
kafka.status.topic=twitter_json
kafka.delete.topic=twitter_delete_json
filter.keywords=kafka,BDXIO17,Im_flog
```
6. Start kafka connect `${KAFKA_PATH}/bin/connect-standalone /etc/kafka/connect-standalone.properties twitter.properties`
7. Create the schedule topic `${KAFKA_PATH}/bin/kafka-topics.sh --zookeeper localhost:2181 --create --partitions 1 --topic schedule --replication-factor 1`
8. Ingest bdxio schedule data `${KAFKA_PATH}/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic schedule --property "parse.key=true" --property "key.separator=:" < resources/schedule`
9. Ingest fake tweet if internet issues `${KAFKA_PATH}/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic twitter_json --property "parse.key=true" --property "key.separator=&" < resources/twitter_backup`

To reset streaming :
- `kafka-streams-application-reset --application-id TwitterStreamingTest --input-topics twitter_json --bootstrap-servers localhost:9092 --zookeeper localhost:2181`
- `kafka-streams-application-reset --application-id TwitterStreamingTest --input-topics schedule --bootstrap-servers localhost:9092 --zookeeper localhost:2181`