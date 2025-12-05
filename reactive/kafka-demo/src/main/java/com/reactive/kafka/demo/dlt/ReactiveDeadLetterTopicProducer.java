package com.reactive.kafka.demo.dlt;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.retry.Retry;

import java.util.function.Function;

@Slf4j
public class ReactiveDeadLetterTopicProducer<K, V> {
    private final KafkaSender<K, V> sender;
    private final Retry retry;

    public ReactiveDeadLetterTopicProducer(KafkaSender<K, V> sender, Retry retry) {
        this.sender = sender;
        this.retry = retry;
    }

    public Mono<SenderResult<K>> produce(ReceiverRecord<K, V> receiverRecord) {
        SenderRecord<K, V, K> senderRecord = toSenderRecord(receiverRecord);
        return sender.send(Mono.just(senderRecord)).next();
    }

    public SenderRecord<K, V, K> toSenderRecord(ReceiverRecord<K, V> receiverRecord) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(
                receiverRecord.topic() + "-dlt",
                receiverRecord.key(),
                receiverRecord.value()
        );
        return SenderRecord.create(producerRecord, receiverRecord.key());
    }

    public Function<Mono<ReceiverRecord<K, V>>, Mono<Void>> recordProcessingErrorHandler() {
        return mono -> mono
                .retryWhen(retry)
                .onErrorMap(ex -> ex.getCause() instanceof RecordProcessingException, Throwable::getCause)
                .doOnError(ex -> log.error(ex.getMessage()))
                .onErrorResume(RecordProcessingException.class, ex -> produce(ex.getReceiverRecord())
                        .then(Mono.fromRunnable(() -> ex.getReceiverRecord().receiverOffset().acknowledge())))
                .then();
    }
}
