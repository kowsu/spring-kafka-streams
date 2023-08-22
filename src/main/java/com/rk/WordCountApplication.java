package com.rk;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

/*
    @created 08/21/2023 - 9:17 PM
    @project kafka-streams-example
    @author k.ramanjineyulu
*/
public class WordCountApplication {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputStream = streamsBuilder.stream("test-topic");

        KStream<String, Long> topology = inputStream.flatMapValues(value -> Arrays.asList(value.split(" ")))
                .groupBy((key, value) -> value)
                .count(Materialized.as("word-count-container"))
                .toStream();
        topology
                .to("test1-topic", Produced.with(Serdes.String(), Serdes.Long()));


        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);

        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));




    }


}
