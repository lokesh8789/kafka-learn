package com.reactive.kafka.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

@Slf4j
public class MultiTopicConsumer {
    public static void main(String[] args) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, "demo",
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "2"
        );

        ReceiverOptions<Object, Object> receiverOptions = ReceiverOptions.create(config)
                .subscription(List.of("hello-world", "inventory"));

        KafkaReceiver.create(receiverOptions)
                .receive()
                .doOnNext(r -> log.info("Topic: {}, Key: {}, value: {}", r.topic(), r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();
    }
}
