package com.reactive.kafka.spring;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderEvent(
        UUID orderId,
        Long customerId,
        LocalDateTime orderDate
) {
}
