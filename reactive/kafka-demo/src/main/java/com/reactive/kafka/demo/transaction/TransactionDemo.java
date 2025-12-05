package com.reactive.kafka.demo.transaction;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.List;
import java.util.Map;

@Slf4j
public class TransactionDemo {
    public static void main(String[] args) {
        TransferEventProcessor transferEventProcessor = new TransferEventProcessor(kafkaSender());
        TransferEventConsumer transferEventConsumer = new TransferEventConsumer(kafkaReceiver());

        transferEventConsumer.receive()
                .transform(transferEventProcessor::process)
                .doOnNext(r -> log.info("Transfer Success: {}", r.correlationMetadata()))
                .doOnError(ex -> log.info(ex.getMessage()))
                .subscribe();
    }

    private static KafkaSender<String, String> kafkaSender() {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "money"
        );
        SenderOptions<String, String> senderOptions = SenderOptions.create(config);
        return KafkaSender.create(senderOptions);
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
                .subscription(List.of("transfer-requests"));

        return KafkaReceiver.create(receiverOptions);
    }
}
