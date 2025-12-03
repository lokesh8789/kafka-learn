package com.reactive.kafka.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
public class ParallelBatchConsumer {
    public static void main(String[] args) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, "demo",
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3
        );

        ReceiverOptions<Object, Object> receiverOptions = ReceiverOptions.create(config)
                .commitInterval(Duration.ofMillis(200))
                .subscription(List.of("hello-world"));

        KafkaReceiver.create(receiverOptions)
                .receiveAutoAck()
                .flatMap(ParallelBatchConsumer::batchProcess, 4)
                .subscribe();
    }

    private static Mono<Void> batchProcess(Flux<ConsumerRecord<Object, Object>> flux) {
        return flux
                .doFirst(() -> log.info("------------------------------"))
                .doOnNext(r -> log.info("Topic: {}, Key: {}, value: {}", r.topic(), r.key(), r.value()))
                .then(Mono.delay(Duration.ofSeconds(1)))
                .then();
    }
}
