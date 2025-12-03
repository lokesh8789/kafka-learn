package com.reactive.kafka.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
public class DLTConsumer {

    public static void main(String[] args) {
        ReactiveDeadLetterTopicProducer<String, String> dltProducer = dltProducer();
        KafkaReceiver<String, String> kafkaReceiver = kafkaReceiver();

        kafkaReceiver.receive()
                .concatMap(rec -> process(rec, dltProducer))
                .subscribe();
    }

    private static Mono<Void> process(ReceiverRecord<String, String> receiverRecord, ReactiveDeadLetterTopicProducer<String, String> dltProducer) {
        return Mono.just(receiverRecord)
                .doOnNext(r -> {
                    if (r.key().endsWith("5")) {
                        throw new RuntimeException("Processing Exception as key ends with 5");
                    }
                    log.info("Key: {}, Value: {}", r.key(), r.value());
                    r.receiverOffset().acknowledge();
                })
                .onErrorMap(ex -> new RecordProcessingException(receiverRecord, ex))
                .transform(dltProducer.recordProcessingErrorHandler());
    }

    private static ReactiveDeadLetterTopicProducer<String, String> dltProducer() {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        SenderOptions<String, String> senderOptions = SenderOptions.create(config);
        KafkaSender<String, String> kafkaSender = KafkaSender.create(senderOptions);

        Retry retry = Retry.fixedDelay(2, Duration.ofMillis(100));
        return new ReactiveDeadLetterTopicProducer<>(kafkaSender, retry);
    }

    private static KafkaReceiver<String, String> kafkaReceiver() {
        Map<String, Object> config = Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, "demo",
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );

        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(config)
                .subscription(List.of("hello-world"));

        return KafkaReceiver.create(receiverOptions);
    }
}
