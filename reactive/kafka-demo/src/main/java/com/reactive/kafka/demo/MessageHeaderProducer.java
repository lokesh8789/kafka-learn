package com.reactive.kafka.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class MessageHeaderProducer {
    public static void main(String[] args) {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        SenderOptions<String, String> senderOptions = SenderOptions.create(config);

        Flux<SenderRecord<String, String, String>> flux = Flux.range(1, 10)
                .map(MessageHeaderProducer::createSenderRecord);

        KafkaSender<String, String> kafkaSender = KafkaSender.create(senderOptions);
        kafkaSender
                .send(flux)
                .doOnNext(r -> log.info("Correlation Id: {}", r.correlationMetadata()))
                .doOnComplete(kafkaSender::close)
                .subscribe();
    }

    private static SenderRecord<String, String, String> createSenderRecord(Integer i) {
        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("client-id", "CH123".getBytes(StandardCharsets.UTF_8));
        recordHeaders.add("tracing-id", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));

        ProducerRecord<String, String> pr = new ProducerRecord<>("hello-world", null, i.toString(), "h-" + i, recordHeaders);
        return SenderRecord.create(pr, pr.key());
    }
}
