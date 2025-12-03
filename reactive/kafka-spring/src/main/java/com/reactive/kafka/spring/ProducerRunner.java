package com.reactive.kafka.spring;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

@Component
@Slf4j
@RequiredArgsConstructor
public class ProducerRunner implements CommandLineRunner {

    private final ReactiveKafkaProducerTemplate<String, OrderEvent> template;

    @Override
    public void run(String... args) throws Exception {
        orderFlux().flatMap(oe -> template.send("hello-world", oe.orderId().toString(), oe))
                .doOnNext(r -> log.info("result: {}", r.recordMetadata()))
                .subscribe();
    }

    private Flux<OrderEvent> orderFlux() {
        return Flux.interval(Duration.ofMillis(50))
                .take(100)
                .map(i -> new OrderEvent(
                        UUID.randomUUID(),
                        i,
                        LocalDateTime.now()
                ));
    }
}
