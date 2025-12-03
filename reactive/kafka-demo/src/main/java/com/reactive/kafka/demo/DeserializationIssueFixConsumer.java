package com.reactive.kafka.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

@Slf4j
public class DeserializationIssueFixConsumer {
    public static void main(String[] args) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, "demo",
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
//                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "2"
        );

        ReceiverOptions<String, Integer> receiverOptions = ReceiverOptions.<String, Integer>create(config)
                .withValueDeserializer(errorHandlingDeserializer())
                .subscription(List.of("hello-world"));

        KafkaReceiver.create(receiverOptions)
                .receive()
//                .filter(re -> re.value() != -10_000)
                .doOnNext(r -> log.info("Topic: {}, Key: {}, value: {}", r.topic(), r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();
    }

    private static ErrorHandlingDeserializer<Integer> errorHandlingDeserializer() {
        ErrorHandlingDeserializer<Integer> deserializer = new ErrorHandlingDeserializer<>(new IntegerDeserializer());
        deserializer.setFailedDeserializationFunction(info -> {
            log.info("Failed Record: {}", new String(info.getData()));
            return -10_000;
        });
        return deserializer;
    }
}
