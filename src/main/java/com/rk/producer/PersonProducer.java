package com.rk.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rk.event.model.Person;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static com.rk.util.Util.producerProps;

/*
    @created August/21/2023 - 11:16 PM
    @project kafka-streams-example
    @author k.ramanjineyulu
*/
public class PersonProducer {

    public static void main(String[] args) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();

        Producer<String, String> producer = new KafkaProducer<>(producerProps());

        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "IT",
                objectMapper.writeValueAsString(new Person("John Doe", "john@example.com", 50, "IT")));

        ProducerRecord<String, String> record1 = new ProducerRecord<>("test-topic", "IT",
                objectMapper.writeValueAsString(new Person("John Doe", "john@example.com", 50, "IT")));

        ProducerRecord<String, String> record2 = new ProducerRecord<>("test-topic", "IT",
                objectMapper.writeValueAsString(new Person("John Doe", "john@example.com", 50, "IT")));


        ProducerRecord<String, String> record3 = new ProducerRecord<>("test-topic", "IT",
                objectMapper.writeValueAsString(new Person("John Doe", "john@example.com", 50, "IT")));

        ProducerRecord<String, String> record4 = new ProducerRecord<>("test-topic", "FINANCE",
                objectMapper.writeValueAsString(new Person("John Doe", "john@example.com", 50, "FINANCE")));

        ProducerRecord<String, String> record5 = new ProducerRecord<>("test-topic", "IT",
                objectMapper.writeValueAsString(new Person("John Doe", "john@example.com", 50, "IT")));

        List<ProducerRecord<String, String>> records = new LinkedList<>();

        records.add(record);
        records.add(record1);
        records.add(record2);
        records.add(record3);
        records.add(record4);
        records.add(record5);

        records.stream().forEach(producer::send);

    }
}
