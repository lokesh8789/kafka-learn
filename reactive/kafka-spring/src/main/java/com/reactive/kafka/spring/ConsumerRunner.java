package com.reactive.kafka.spring;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class ConsumerRunner implements CommandLineRunner {

    private final ReactiveKafkaConsumerTemplate<String, String> template;

    @Override
    public void run(String... args) throws Exception {
        template.receive()
                .doOnNext(r -> log.info("Key: {}, Value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();
    }
}
