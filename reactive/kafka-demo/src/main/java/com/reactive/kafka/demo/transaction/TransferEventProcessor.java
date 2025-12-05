package com.reactive.kafka.demo.transaction;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.kafka.sender.TransactionManager;

import java.time.Duration;
import java.util.function.Predicate;

@Slf4j
public class TransferEventProcessor {
    private final KafkaSender<String, String> sender;

    public TransferEventProcessor(KafkaSender<String, String> sender) {
        this.sender = sender;
    }

    public Flux<SenderResult<String>> process(Flux<TransferEvent> flux) {
        return flux.concatMap(this::validate)
                .concatMap(this::sendTransaction);
    }

    private Mono<SenderResult<String>> sendTransaction(TransferEvent event) {
        Flux<SenderRecord<String, String, String>> senderRecords = toSenderRecords(event);
        TransactionManager transactionManager = sender.transactionManager();
        return transactionManager.begin()
                .then(sender.send(senderRecords)
                        .concatWith(Mono.delay(Duration.ofSeconds(1)).then(Mono.fromRunnable(event.acknowledge())))
                        .concatWith(transactionManager.commit())
                        .last())
                .doOnError(ex -> log.info(ex.getMessage()))
                .onErrorResume(ex -> transactionManager.abort());
    }

    private Mono<TransferEvent> validate(TransferEvent event) {
        return Mono.just(event)
                .filter(Predicate.not(e -> e.key().equals("5")))
                .switchIfEmpty(Mono.<TransferEvent>fromRunnable(event.acknowledge())
                        .doFirst(() -> log.info("Fails Validation: {}", event.key())));
    }

    private Flux<SenderRecord<String, String, String>> toSenderRecords(TransferEvent event) {
        ProducerRecord<String, String> pr1 = new ProducerRecord<>("transaction-events", event.key(), "%s+%s".formatted(event.to(), event.amount()));
        ProducerRecord<String, String> pr2 = new ProducerRecord<>("transaction-events", event.key(), "%s-%s".formatted(event.from(), event.amount()));
        SenderRecord<String, String, String> sr1 = SenderRecord.create(pr1, pr1.key());
        SenderRecord<String, String, String> sr2 = SenderRecord.create(pr2, pr2.key());
        return Flux.just(sr1, sr2);
    }
}
