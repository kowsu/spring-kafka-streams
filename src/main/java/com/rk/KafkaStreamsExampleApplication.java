package com.rk;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

import static com.rk.util.Util.streamsProps;


@SpringBootApplication
public class KafkaStreamsExampleApplication {


    public static void main(String[] args) {
        // Configure Kafka Streams properties


        StreamsBuilder builder = new StreamsBuilder();

        // Create a KStream from the input topic
        builder.stream("test-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .to("test1-topic", Produced.with(Serdes.String(), Serdes.String()));

        // Create a Kafka Streams application using the properties and the builder
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps());

        // Start the application
        streams.start();

        // Add shutdown hook to gracefully close the Kafka Streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


}
