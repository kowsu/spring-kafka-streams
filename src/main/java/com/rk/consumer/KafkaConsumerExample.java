package com.rk.consumer;

/*
    @created August/21/2023 - 11:03 PM
    @project kafka-streams-example
    @author k.ramanjineyulu
*/

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;

import static com.rk.util.Util.consumerProps;

public class KafkaConsumerExample {
    public static void main(String[] args) {


        Consumer<String, Long> consumer = new KafkaConsumer<>(consumerProps());
        consumer.subscribe(Collections.singleton("test1-topic"));

        while (true) {
            ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                System.out.println("Received message: " +
                        "Key = " + record.key() +
                        ", Value = " + record.value());
            });
        }
    }
}

