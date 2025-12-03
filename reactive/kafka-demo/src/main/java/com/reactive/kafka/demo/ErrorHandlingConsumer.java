package com.reactive.kafka.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class ErrorHandlingConsumer {
    public static void main(String[] args) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, "demo",
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );

        ReceiverOptions<Object, Object> receiverOptions = ReceiverOptions.create(config)
                .subscription(List.of("hello-world"));

        KafkaReceiver.create(receiverOptions)
                .receive()
                .concatMap(ErrorHandlingConsumer::process)
                .subscribe();
    }

    private static Mono<Void> process(ReceiverRecord<Object, Object> receiverRecord) {
        return Mono.just(receiverRecord)
                .doOnNext(r -> {
                    int i = ThreadLocalRandom.current().nextInt(1, 100);
                    log.info("Topic: {}, Key: {}, value: {}", r.topic(), r.key(), r.value().toString().toCharArray()[i]);
                })
                .retryWhen(Retry.fixedDelay(3, Duration.ofMillis(100)).onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> retrySignal.failure()))
                .doOnError(e -> log.error(e.getMessage()))
                .doFinally(signalType -> receiverRecord.receiverOffset().acknowledge())
                .onErrorComplete()
                .then();
    }
}
