package com.reactive.kafka.spring;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.util.List;

@Configuration
public class ProducerConfig {
    @Bean
    public SenderOptions<String, OrderEvent> senderOptions(KafkaProperties kafkaProperties) {
        return SenderOptions.<String, OrderEvent>create(kafkaProperties.buildProducerProperties());
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, OrderEvent> producerTemplate(SenderOptions<String, OrderEvent> receiverOptions) {
        return new ReactiveKafkaProducerTemplate<>(receiverOptions);
    }
}
