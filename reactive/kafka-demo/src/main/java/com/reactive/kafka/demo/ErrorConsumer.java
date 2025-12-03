package com.reactive.kafka.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
public class ErrorConsumer {
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
                .subscription(List.of("hello-world"));

        KafkaReceiver.create(receiverOptions)
                .receive()
                .doOnNext(r -> log.info("Topic: {}, Key: {}, value: {}", r.topic(), r.key(), r.value().toString().toCharArray()[15]))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(1))
                        .onRetryExhaustedThrow((spec, signal) -> signal.failure()))
                .blockLast();
    }
}
