package com.reactive.kafka.demo.transaction;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

@Slf4j
public class TransferEventConsumer {
    private final KafkaReceiver<String, String> receiver;

    public TransferEventConsumer(KafkaReceiver<String, String> receiver) {
        this.receiver = receiver;
    }

    public Flux<TransferEvent> receive() {
        return receiver.receive()
                .doOnNext(r -> log.info("Key: {}, Value: {}", r.key(), r.value()))
                .map(this::toTransferEvent);
    }

    private TransferEvent toTransferEvent(ReceiverRecord<String, String> record) {
        String[] arr = record.value().split(",");
        Runnable runnable = record.key().equals("6") ? fail() : ack(record);
        return new TransferEvent(
                record.key(),
                arr[0],
                arr[1],
                arr[2],
                runnable
        );
    }

    private Runnable ack(ReceiverRecord<String, String> record) {
        return () -> record.receiverOffset().acknowledge();
    }

    private Runnable fail() {
        return () -> { throw new RuntimeException("Error While Ack"); };
    }
}
