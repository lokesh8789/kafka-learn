package com.reactive.kafka.demo.transaction;

public record TransferEvent(
   String key,
   String from,
   String to,
   String amount,
   Runnable acknowledge
) {}
