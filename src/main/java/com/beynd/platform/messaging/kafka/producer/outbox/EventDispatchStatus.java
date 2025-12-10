package com.beynd.platform.messaging.kafka.producer.outbox;

public enum EventDispatchStatus {
    PENDING,
    SENT,
    FAILED
}
