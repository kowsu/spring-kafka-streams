package com.rk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rk.event.model.Person;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import static com.rk.util.Util.streamsProps;

/*
    @created August/21/2023 - 11:20 PM
    @project kafka-streams-example
    @author k.ramanjineyulu
*/
public class PersonStreamApplication {

    public static void main(String[] args) {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> stream = streamsBuilder.stream("test-topic");


        stream
                .filter((key, value) -> serialize(value).getDept().equals("it"))
                .mapValues(value -> serialize(value))
                .groupBy((key, value) -> value.getDept())
                .count(Materialized.as("dept-total-salaries"))
                .toStream()
                .to("test1-topic", Produced.with(Serdes.String(), Serdes.Long()));



        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamsProps());


        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }

    static Person serialize(String json) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(json, Person.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
