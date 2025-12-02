package com.reactive.kafka.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

@Slf4j
public class ConsumerGroup {
    public static void start(String id) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, "demo",
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, id
        );

        ReceiverOptions<Object, Object> receiverOptions = ReceiverOptions.create(config)
                .subscription(List.of("hello-world"));

        KafkaReceiver.create(receiverOptions)
                .receive()
                .doOnNext(r -> log.info("Topic: {}, Key: {}, value: {}", r.topic(), r.key(), r.value()))
                .doOnNext(r -> r.headers().forEach(header -> log.info("Header Key: {}, Value: {}", header.key(), new String(header.value()))))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();
    }


    private static class Consumer1 {
        public static void main(String[] args) {
            start("1");
        }
    }

    private static class Consumer2 {
        public static void main(String[] args) {
            start("2");
        }
    }

    private static class Consumer3 {
        public static void main(String[] args) {
            start("3");
        }
    }
}
